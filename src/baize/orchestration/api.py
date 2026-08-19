"""
编排 API — 后台执行 + 运行管理。

端点设计：
    POST   /api/v1/runs                        创建并启动一次执行 (立即返回 run_id)
    GET    /api/v1/runs                        列出运行记录
    GET    /api/v1/runs/{run_id}               查询单次执行状态 + 事件列表
    GET    /api/v1/runs/{run_id}/stream         SSE 实时事件流（支持重连）
    POST   /api/v1/runs/{run_id}/confirm        人工确认恢复
    GET    /api/v1/pipelines/templates          模板列表
    POST   /api/v1/pipelines/{id}/parse         解析 YAML 为 PipelineDefinition
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any

from fastapi import FastAPI, Request, Query, Depends, HTTPException

# Conditional import — avoid breaking if langgraph not installed
try:
    from sse_starlette.sse import EventSourceResponse
except ImportError:
    EventSourceResponse = None  # type: ignore

from baize.orchestration.node_types import PipelineDefinition, PipelineNode, BranchRule, ParallelBranch
from baize.orchestration.templates import get_builtin_templates
from baize.orchestration.run_store import get_run_store
from baize.orchestration.runner import get_runner
from baize.orchestration.run_store import get_activation_store

logger = logging.getLogger(__name__)

# Pydantic models for request/response validation
try:
    from pydantic import BaseModel, Field
except ImportError:
    BaseModel = object  # type: ignore
    Field = None  # type: ignore


class PipelineRunRequest(BaseModel if BaseModel is not object else object):
    context: dict[str, Any] = {}
    webhook: str = ""
    dedup_key: str = ""  # 幂等去重键：同一事件重复提交时返回既有 run_id


class ConfirmRequest(BaseModel if BaseModel is not object else object):
    action: str  # "approve" | "reject" 或自定义选项值
    feedback: str = ""


def _build_pipeline_from_yaml(yaml_data: dict[str, Any]) -> PipelineDefinition:
    """从 YAML 字典构建 PipelineDefinition。

    兼容旧格式 steps 列表和新格式 nodes 图结构。
    """
    nodes_raw = yaml_data.get("nodes", yaml_data.get("steps", []))

    nodes: list[PipelineNode] = []
    for raw in nodes_raw:
        node_type = raw.get("type", "agent")

        branches: list[BranchRule] = []
        if node_type in ("decision", "condition", "ai_decision"):
            if node_type == "condition":
                node_type = "decision"
            for br in raw.get("branches", []):
                branches.append(BranchRule(
                    condition=br.get("when", br.get("condition", "")),
                    target=br.get("goto", br.get("target", "")),
                    label=br.get("label", ""),
                    is_default=br.get("default", False),
                ))

        parallel_branches: list[ParallelBranch] = []
        if node_type == "parallel":
            for pb in raw.get("branches", raw.get("parallel_branches", [])):
                if isinstance(pb, str):
                    parallel_branches.append(ParallelBranch(node_id=pb))
                elif isinstance(pb, dict):
                    sub_node = _node_from_dict(pb)
                    parallel_branches.append(ParallelBranch(
                        node_id=pb.get("id", ""),
                        node=sub_node,
                    ))

        confirm_options = raw.get("choices", raw.get("confirm_options", []))
        confirm_branches = raw.get("branches", raw.get("confirm_branches", {}))

        # 兼容旧 human 类型
        if node_type == "human":
            node_type = "confirm"

        node = PipelineNode(
            id=raw.get("id", ""),
            type=node_type,
            display_name=raw.get("display_name", raw.get("name", raw.get("id", ""))),
            description=raw.get("description", ""),
            agent=raw.get("agent", ""),
            prompt_template=raw.get("prompt", raw.get("prompt_template", "")),
            branches=branches,
            parallel_branches=parallel_branches,
            merge_strategy=raw.get("merge_strategy", raw.get("merge", "all")),
            confirm_prompt=raw.get("confirm_prompt", raw.get("prompt", "")),
            confirm_options=confirm_options,
            confirm_branches=confirm_branches,
            # decision / ai_decision 专用
            decision_expression=raw.get("decision_expression", ""),
            decision_prompt=raw.get("decision_prompt", raw.get("ai_prompt", "")),
            decision_model=raw.get("decision_model", ""),
            target=raw.get("target", ""),
        )
        nodes.append(node)

    pipe_type = yaml_data.get("type", "auto")
    # 如果存在 confirm/human 节点，自动检测为 manual
    has_confirm = any(n.type == "confirm" for n in nodes)
    if pipe_type == "auto" and has_confirm:
        pipe_type = "manual"

    return PipelineDefinition(
        id=yaml_data.get("id", ""),
        name=yaml_data.get("name", ""),
        type=pipe_type,
        description=yaml_data.get("description", ""),
        category=yaml_data.get("category", ""),
        tags=yaml_data.get("tags", []),
        nodes=nodes,
        triggers=yaml_data.get("triggers", []),
        context_schema=yaml_data.get("context_schema", {}),
        version=yaml_data.get("version", "1.0"),
        timeout_seconds=yaml_data.get("timeout_seconds", 3600),
    )


def _node_from_dict(raw: dict[str, Any]) -> PipelineNode:
    """从单节点字典构建 PipelineNode（用于 parallel 子节点）。"""
    return PipelineNode(
        id=raw.get("id", ""),
        type=raw.get("type", "agent"),
        display_name=raw.get("display_name", raw.get("id", "")),
        agent=raw.get("agent", ""),
        prompt_template=raw.get("prompt", raw.get("prompt_template", "")),
    )


# ====================================================================
# Module Register
# ====================================================================

def _require_api_key(request: Request) -> None:
    """编排端点认证：与 baize-core 保持一致的 Token 校验。

    - 未开启认证（require_auth=False）时放行；
    - 认证管理器未初始化时放行；
    - 开启认证后，所有编排端点（含激活高危流水线）均需携带
      X-Baize-API-Key 或 Authorization: Bearer <token>。
    """
    if not getattr(request.app.state, "require_auth", False):
        return
    auth_manager = getattr(request.app.state, "auth_manager", None)
    if auth_manager is None:
        return
    key = request.headers.get("X-Baize-API-Key") or request.headers.get(
        "Authorization", ""
    ).replace("Bearer ", "")
    if not auth_manager.validate_token(key):
        raise HTTPException(status_code=401, detail="无效的 API 密钥")


def register(app: FastAPI) -> None:
    """注册编排 API 路由到 FastAPI 应用。"""
    runner = get_runner()
    store = get_run_store()

    # ---- 模板列表 ----
    @app.get("/api/v1/pipelines/templates", dependencies=[Depends(_require_api_key)])
    def pipeline_templates() -> dict:
        templates = get_builtin_templates()
        # 为模板添加 source 标记
        enriched = []
        for t in templates:
            t_copy = dict(t)
            t_copy["source"] = "builtin"
            enriched.append(t_copy)
        return {"templates": enriched}

    # ---- 删除内置模板 ----
    @app.delete("/api/v1/pipelines/templates/{template_id}", dependencies=[Depends(_require_api_key)])
    def delete_template(template_id: str) -> dict:
        from baize.api.custom_agents import get_deleted_store
        deleted = get_deleted_store()
        # 确认模板存在
        from baize.orchestration.templates import get_template_by_id as _find_tpl
        tpl = _find_tpl(template_id, skip_deleted=False)
        if tpl is None:
            return {"error": f"模板 '{template_id}' 未找到", "ok": False}
        deleted.delete_template(template_id)
        return {"ok": True, "template_id": template_id, "message": "模板已删除"}

    # ---- 恢复所有已删除模板 ----
    @app.post("/api/v1/pipelines/templates/reset", dependencies=[Depends(_require_api_key)])
    def reset_templates() -> dict:
        from baize.api.custom_agents import get_deleted_store
        deleted = get_deleted_store()
        count = deleted.reset_templates()
        return {"ok": True, "restored": count, "message": f"已恢复 {count} 个模板"}

    # ---- YAML 解析/验证 ----
    @app.post("/api/v1/pipelines/{pipeline_id}/parse", dependencies=[Depends(_require_api_key)])
    async def pipeline_parse(pipeline_id: str, request: Request) -> dict:
        body = await request.json()
        try:
            pipeline = _build_pipeline_from_yaml(body)
            errors = pipeline.validate()
            return {
                "ok": len(errors) == 0,
                "pipeline": {
                    "id": pipeline.id,
                    "name": pipeline.name,
                    "type": pipeline.type,
                    "nodes": [
                        {
                            "id": n.id,
                            "type": n.type,
                            "display_name": n.display_name,
                        }
                        for n in pipeline.nodes
                    ],
                },
                "errors": errors,
            }
        except Exception as e:
            return {"ok": False, "errors": [str(e)]}

    # ---- 创建并启动执行 ----
    @app.post("/api/v1/runs", dependencies=[Depends(_require_api_key)])
    async def create_run(request: Request) -> dict:
        body = await request.json()
        pipeline_id = body.get("pipeline_id", "")

        # 查找管道定义：先查模板，再查自定义
        pipeline_def = _find_pipeline(pipeline_id)
        if pipeline_def is None:
            return {"error": f"流水线 '{pipeline_id}' 未找到", "ok": False}

        context = body.get("context", {})
        webhook = body.get("webhook", "")
        dedup_key = body.get("dedup_key", "")

        # 幂等去重：提交前先查是否已有同 key 的 run（避免把首次提交误判为 duplicate）
        existing = get_run_store().find_by_dedup_key(dedup_key) if dedup_key else None

        run_id = await runner.submit(pipeline_def, context, webhook, dedup_key)
        runner.cache_pipeline(pipeline_def)

        record = runner.get_run(run_id)
        return {
            "ok": True,
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "status": record.status if record else "running",
            "duplicate": bool(existing),
        }

    # ---- 列出运行记录 ----
    @app.get("/api/v1/runs", dependencies=[Depends(_require_api_key)])
    def list_runs(
        pipeline_id: str = Query(default=""),
        status: str = Query(default=""),
        limit: int = Query(default=50),
    ) -> dict:
        runs = runner.list_runs(
            pipeline_id=pipeline_id or None,
            status=status or None,
            limit=limit,
        )
        return {"runs": runs, "total": len(runs)}

    # ---- 查询单次执行 ----
    @app.get("/api/v1/runs/{run_id}", dependencies=[Depends(_require_api_key)])
    def get_run(run_id: str) -> dict:
        record = runner.get_run(run_id)
        if record is None:
            return {"error": "运行记录未找到", "ok": False}
        return {
            "ok": True,
            "run": record.to_dict(),
        }

    # ---- SSE 事件流 ----
    @app.get("/api/v1/runs/{run_id}/stream", dependencies=[Depends(_require_api_key)])
    async def stream_events(
        run_id: str,
        last_event_id: str = Query(default=""),
    ):
        if EventSourceResponse is None:
            from fastapi.responses import StreamingResponse

            async def _fallback():
                async for event in runner.subscribe_events(run_id, last_event_id):
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

            return StreamingResponse(_fallback(), media_type="text/event-stream")

        async def _event_generator():
            async for event in runner.subscribe_events(run_id, last_event_id):
                if event.get("event_id") == "done":
                    yield {"event": "done", "data": "[DONE]"}
                    return
                event_type = event.get("type", "log")
                yield {
                    "event": event_type,
                    "data": json.dumps(event, ensure_ascii=False),
                }

        return EventSourceResponse(_event_generator())

    # ---- 人工确认恢复 ----
    @app.post("/api/v1/runs/{run_id}/confirm", dependencies=[Depends(_require_api_key)])
    async def confirm_run(run_id: str, request: Request) -> dict:
        body = await request.json()
        action = body.get("action", "")

        if not action:
            return {"error": "缺少 action 字段", "ok": False}

        record = await runner.resume_after_confirm(run_id, action)
        if record is None:
            return {"error": "运行记录未找到或状态不是 paused", "ok": False}

        return {
            "ok": True,
            "run_id": run_id,
            "status": record.status,
            "message": f"已执行操作: {action}",
        }

    # ---- 自动化流水线激活控制 ----
    @app.post("/api/v1/pipelines/{pipeline_id}/activate", dependencies=[Depends(_require_api_key)])
    def activate_pipeline(pipeline_id: str) -> dict:
        """开启自动化流水线，使其开始接收数据。"""
        activation = get_activation_store()
        pipeline_def = _find_pipeline(pipeline_id)
        if pipeline_def is None:
            return {"error": f"流水线 '{pipeline_id}' 未找到", "ok": False}
        if pipeline_def.type != "auto":
            return {"error": "仅自动化流水线可切换激活状态", "ok": False}
        activation.activate(pipeline_id)
        return {"ok": True, "pipeline_id": pipeline_id, "active": True}

    @app.post("/api/v1/pipelines/{pipeline_id}/deactivate", dependencies=[Depends(_require_api_key)])
    def deactivate_pipeline(pipeline_id: str) -> dict:
        """关闭自动化流水线。"""
        activation = get_activation_store()
        activation.deactivate(pipeline_id)
        return {"ok": True, "pipeline_id": pipeline_id, "active": False}

    @app.get("/api/v1/pipelines/{pipeline_id}/status", dependencies=[Depends(_require_api_key)])
    def pipeline_status(pipeline_id: str) -> dict:
        """获取自动化流水线的激活状态。"""
        activation = get_activation_store()
        return activation.get_status(pipeline_id)

    # ---- 人工介入流水线列表（供对话选择） ----
    @app.get("/api/v1/pipelines/manual", dependencies=[Depends(_require_api_key)])
    def list_manual_pipelines() -> dict:
        """列出所有人工介入类型的流水线，供对话时选择。"""
        templates = get_builtin_templates()
        manual = []

        def _add_pipeline(pipe: dict):
            definition = _build_pipeline_from_yaml(pipe)
            if definition.type == "manual":
                manual.append({
                    "id": definition.id,
                    "name": definition.name,
                    "description": definition.description,
                    "category": definition.category,
                    "tags": definition.tags,
                    "nodes_count": len(definition.nodes),
                    "node_types": [n.type for n in definition.nodes],
                })

        for tpl in templates:
            _add_pipeline(tpl)

        # 也查自定义管道
        try:
            from baize.api.custom_agents import CustomPipelineStore
            store = CustomPipelineStore()
            for p in store.list():
                _add_pipeline(p)
        except Exception:
            pass

        return {"pipelines": manual, "total": len(manual)}

    # ---- 服务启动：预填充管道缓存 + 恢复中断的 run ----
    @app.on_event("startup")
    async def _recover_interrupted_runs() -> None:
        try:
            # 1. 预填充 pipeline 定义缓存（内置模板 + 自定义管道）
            for tpl in get_builtin_templates():
                try:
                    runner.cache_pipeline(_build_pipeline_from_yaml(tpl))
                except Exception:
                    continue
            try:
                from baize.api.custom_agents import CustomPipelineStore
                for p in CustomPipelineStore().list():
                    try:
                        runner.cache_pipeline(_build_pipeline_from_yaml(p))
                    except Exception:
                        continue
            except Exception:
                pass

            # 2. 恢复服务重启前中断的 run
            recovered = await runner.recover_interrupted()
            if recovered:
                logger.info(f"已恢复 {len(recovered)} 个中断的 run: {recovered}")
        except Exception as e:  # 恢复失败不应阻止服务启动
            logger.warning(f"启动恢复中断 run 失败: {e}")

    logger.info("Orchestration API registered (background runner + SSE stream)")


# ====================================================================
# Pipeline 查找辅助
# ====================================================================

def _find_pipeline(pipeline_id: str) -> PipelineDefinition | None:
    """查找流水线定义：内置模板 + 自定义管道。"""
    # 先查内置模板
    for tpl in get_builtin_templates():
        if tpl.get("id") == pipeline_id:
            return _build_pipeline_from_yaml(tpl)

    # 查自定义管道存储
    try:
        from baize.api.custom_agents import CustomPipelineStore
        store = CustomPipelineStore()
        for p in store.list():
            if p.get("id") == pipeline_id:
                return _build_pipeline_from_yaml(p)
    except Exception:
        pass

    return None

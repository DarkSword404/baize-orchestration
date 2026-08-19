"""
流水线编译器 — 将 PipelineDefinition 编译为 LangGraph StateGraph。

核心设计（借鉴 LangGraph / Dify 模式）：
1. 每种节点类型注册为一个图节点函数
2. decision 节点用 add_conditional_edges 实现非顺序路由
3. confirm 节点用 LangGraph interrupt() 实现人工确认暂停
4. parallel 节点内部用 asyncio.gather 实现并发
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Callable

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.base import BaseCheckpointSaver

from baize.orchestration.state import PipelineState, build_initial_state
from baize.orchestration.node_types import PipelineDefinition, PipelineNode
from baize.orchestration.nodes.base import BaseNodeExecutor
from baize.orchestration.nodes.parallel import get_executor

logger = logging.getLogger(__name__)

# === LangGraph 节点函数名前缀，避免冲突 ===
NODE_PREFIX = "_pnode_"

# 模块级默认 checkpointer 单例。
# 关键：runner 每次执行会新建 PipelineGraphCompiler，若各自持有独立 MemorySaver，
# confirm 中断的 checkpoint 会在 resume 时丢失（P0-2）。共享单例保证跨实例可恢复。
_default_checkpointer = MemorySaver()


class PipelineGraphCompiler:
    """将 PipelineDefinition 编译为可执行的 LangGraph StateGraph。"""

    def __init__(
        self,
        pipeline: PipelineDefinition,
        checkpointer: BaseCheckpointSaver | None = None,
    ):
        self.pipeline = pipeline
        self.checkpointer = checkpointer or _default_checkpointer
        self._node_names: dict[str, str] = {}  # node_id → LangGraph node name
        self._decision_nodes: set[str] = set()
        self._confirm_nodes: set[str] = set()
        self._parallel_nodes: set[str] = set()

        # 执行器缓存
        self._executors: dict[str, BaseNodeExecutor] = {}

        # 已编译图缓存：同一编译器实例重复 compile() 时复用，
        # 避免重建图导致 checkpoint / 内存状态丢失
        self._compiled: Any = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compile(self) -> StateGraph:
        """编译管道为 StateGraph，返回已编译的图对象（结果缓存复用）。"""
        if self._compiled is not None:
            return self._compiled

        graph = StateGraph(PipelineState)

        self._register_nodes(graph)
        self._wire_edges(graph)

        self._compiled = graph.compile(checkpointer=self.checkpointer)
        return self._compiled

    async def execute(
        self,
        context: dict[str, Any],
        webhook: str = "",
        config: dict[str, Any] | None = None,
    ) -> PipelineState:
        """直接执行（同步等待完成）。"""
        compiled = self.compile()
        cfg = dict(config or {})
        run_id = cfg.get("configurable", {}).get("thread_id") or str(uuid.uuid4())
        cfg.setdefault("configurable", {})["thread_id"] = run_id
        start = self.pipeline.get_start_node()
        initial = build_initial_state(
            pipeline_id=self.pipeline.id,
            run_id=run_id,
            pipe_type=self.pipeline.type,
            context=context,
            webhook=webhook,
            start_node_id=start.id if start else "",
        )
        result = await compiled.ainvoke(initial, cfg)
        return result

    async def execute_stream(
        self,
        context: dict[str, Any],
        webhook: str = "",
        config: dict[str, Any] | None = None,
        on_event: Callable[[str, dict[str, Any]], Any] | None = None,
    ):
        """流式执行（通过回调推送每个节点的事件）。

        run_id 统一取 config["configurable"]["thread_id"]（由 API 层传入），
        保证事件流 / checkpointer / RunStore 使用同一个 id（P0-3）。
        confirm 中断（GraphInterrupt）时推送 pipeline_paused 事件，
        且不终止生成器 —— 调用方通过 get_state 读取暂停状态。
        """
        from langgraph.errors import GraphInterrupt

        compiled = self.compile()
        cfg = dict(config or {})
        run_id = cfg.get("configurable", {}).get("thread_id") or str(uuid.uuid4())
        cfg.setdefault("configurable", {})["thread_id"] = run_id

        start = self.pipeline.get_start_node()
        initial = build_initial_state(
            pipeline_id=self.pipeline.id,
            run_id=run_id,
            pipe_type=self.pipeline.type,
            context=context,
            webhook=webhook,
            start_node_id=start.id if start else "",
        )

        paused_node_id = ""
        try:
            async for event in compiled.astream_events(initial, cfg, version="v2"):
                kind = event.get("event", "")
                name = event.get("name", "")
                metadata = event.get("metadata", {})

                if kind == "on_chain_start" and name.startswith(NODE_PREFIX):
                    node_id = name[len(NODE_PREFIX):]
                    data = event.get("data", {})
                    inp = data.get("input", {}) if isinstance(data, dict) else {}
                    if on_event:
                        on_event("node_started", {
                            "run_id": run_id,
                            "node_id": node_id,
                            "node_type": inp.get("current_node_type", ""),
                        })

                elif kind == "on_chain_end" and name.startswith(NODE_PREFIX):
                    node_id = name[len(NODE_PREFIX):]
                    data = event.get("data", {})
                    output = data.get("output", {}) if isinstance(data, dict) else {}
                    if on_event:
                        on_event("node_completed", {
                            "run_id": run_id,
                            "node_id": node_id,
                            "data": output,
                        })
        except GraphInterrupt as exc:
            # 兼容旧版 langgraph：interrupt 直接抛给调用方
            paused_node_id = self._extract_interrupt_node_id(exc)

        # 读取最终状态（完成 / 失败 / 暂停均包含）。
        # 关键：snapshot.next 非空表示执行被中断暂停（等 resume）
        final = compiled.get_state(cfg)
        final_values: dict[str, Any] | None = None
        if final:
            if final.next:
                if not paused_node_id:
                    paused_node_id = self._extract_paused_node_id(final)
                if on_event:
                    on_event("pipeline_paused", {
                        "run_id": run_id,
                        "node_id": paused_node_id,
                        "data": {"confirm_required": True},
                    })
                yield run_id, final.values if final.values else initial
                return
            final_values = final.values
            if final_values and on_event:
                # 初始 status="pending"，正常跑完未被节点改写 → 视为 completed
                status = final_values.get("status", "pending")
                if status == "failed":
                    on_event("pipeline_failed", {"run_id": run_id, "data": final_values})
                else:
                    on_event("pipeline_completed", {"run_id": run_id, "data": final_values})

        yield run_id, final_values if final_values is not None else initial

    @staticmethod
    def _extract_interrupt_node_id(exc: Exception) -> str:
        """从 GraphInterrupt 异常中提取被中断的 confirm 节点 id。"""
        interrupts = getattr(exc, "__interrupts__", None) or []
        for it in interrupts:
            payload = getattr(it, "value", None)
            if isinstance(payload, dict):
                node_id = payload.get("confirm_node_id", "")
                if node_id:
                    return node_id
        return ""

    @staticmethod
    def _extract_paused_node_id(snapshot: Any) -> str:
        """从暂停时的 StateSnapshot 中提取被中断的 confirm 节点 id。"""
        try:
            tasks = getattr(snapshot, "tasks", ()) or ()
            for task in tasks:
                interrupts = getattr(task, "interrupts", ()) or ()
                for it in interrupts:
                    payload = getattr(it, "value", None)
                    if isinstance(payload, dict):
                        node_id = payload.get("confirm_node_id", "")
                        if node_id:
                            return node_id
                name = getattr(task, "name", "") or ""
                if name.startswith(NODE_PREFIX):
                    return name[len(NODE_PREFIX):]
        except Exception:
            pass
        return ""

    def get_confirm_node_state(self, run_id: str) -> dict[str, Any] | None:
        """获取等待人工确认的节点状态。"""
        compiled = self.compile()
        state = compiled.get_state({"configurable": {"thread_id": run_id}})
        if state and state.values:
            return state.values
        return None

    async def resume_after_confirm(self, run_id: str, choice: str) -> PipelineState:
        """人工确认后恢复执行。"""
        from langgraph.types import Command
        compiled = self.compile()
        cfg = {"configurable": {"thread_id": run_id}}
        result = await compiled.ainvoke(Command(resume=choice), cfg)
        return result

    # ------------------------------------------------------------------
    # 节点注册
    # ------------------------------------------------------------------

    def _register_nodes(self, graph: StateGraph) -> None:
        """将所有管道节点注册为 LangGraph 图节点。"""
        for node in self.pipeline.nodes:
            if node.type in ("decision", "ai_decision"):
                self._decision_nodes.add(node.id)
            elif node.type == "confirm":
                self._confirm_nodes.add(node.id)
            elif node.type == "parallel":
                self._parallel_nodes.add(node.id)

            # 每个节点只注册一个执行函数
            langgraph_name = f"{NODE_PREFIX}{node.id}"
            self._node_names[node.id] = langgraph_name

            executor = get_executor(node.type)
            self._executors[node.id] = executor

            graph.add_node(langgraph_name, self._make_node_func(node))

    # ------------------------------------------------------------------
    # 边连接
    # ------------------------------------------------------------------

    def _wire_edges(self, graph: StateGraph) -> None:
        """连接节点之间的边，处理条件和人工确认路由。"""
        nodes = self.pipeline.nodes

        if not nodes:
            graph.set_entry_point(f"{NODE_PREFIX}__empty__")

            async def _empty(state: PipelineState) -> dict:
                return {"status": "completed", "report": "空流水线"}

            graph.add_node(f"{NODE_PREFIX}__empty__", _empty)
            graph.add_edge(f"{NODE_PREFIX}__empty__", END)
            return

        # 入口：第一个节点
        start = self.pipeline.get_start_node()
        if start:
            graph.set_entry_point(self._node_names[start.id])

        for i, node in enumerate(nodes):
            lang_name = self._node_names[node.id]

            if node.type in ("decision", "ai_decision"):
                # 条件边：根据 route（由执行器写入）路由到对应分支。
                # decision 走规则求值，ai_decision 走 LLM 决策，统一读 state["route"]。
                graph.add_conditional_edges(
                    lang_name,
                    self._make_decision_router(node),
                    self._build_decision_path_map(node),
                )

            elif node.type == "confirm":
                # confirm 节点：完成后根据 confirm_branches 路由
                graph.add_conditional_edges(
                    lang_name,
                    self._make_confirm_router(node),
                    self._build_confirm_path_map(node),
                )

            elif node.type in ("agent", "transform", "subpipeline", "parallel", "receiver", "datatransformer"):
                # 普通节点：顺序连接到下一个节点
                next_node = self._find_next_node(node.id)
                if next_node:
                    graph.add_edge(lang_name, self._node_names[next_node.id])
                else:
                    graph.add_edge(lang_name, END)

    def _find_next_node(self, current_id: str) -> PipelineNode | None:
        """找到当前节点的下一个（线性）节点。

        规则：
        - 优先使用 node.target 显式指定的下一个节点
        - 否则按定义顺序取下一个节点（decision / ai_decision 也可作顺序边目标）
        - 跳过已被条件边（decision / ai_decision / confirm）作为分支目标的节点，
          避免线性推断导致分支节点被重复执行
        """
        node = self.pipeline.get_node(current_id)
        if node and node.target:
            return self.pipeline.get_node(node.target)

        found_current = False
        for n in self.pipeline.nodes:
            if n.id == current_id:
                found_current = True
                continue
            if found_current:
                if self._is_conditional_target(n.id):
                    continue
                return n
        return None

    def _is_conditional_target(self, node_id: str) -> bool:
        """该节点是否已被某个条件节点的分支作为目标。"""
        for n in self.pipeline.nodes:
            if n.type in ("decision", "ai_decision"):
                if node_id in {b.target for b in n.branches}:
                    return True
            elif n.type == "confirm":
                if node_id in set(n.confirm_branches.values()):
                    return True
        return False

    # ------------------------------------------------------------------
    # 路由函数工厂
    # ------------------------------------------------------------------

    def _make_node_func(self, node: PipelineNode) -> Callable:
        """为节点生成 LangGraph 节点执行函数。"""

        async def _execute_node(state: PipelineState) -> dict[str, Any]:
            executor = self._executors[node.id]
            result = await executor.execute(node, state)
            if isinstance(result, dict):
                result["current_node_type"] = node.type
            return result

        return _execute_node

    def _make_decision_router(self, node: PipelineNode) -> Callable:
        """生成 decision 节点的条件路由函数。"""

        def _route(state: PipelineState) -> str:
            route = state.get("route", "")
            if route:
                return route
            # 回退到默认分支
            for br in node.branches:
                if br.is_default:
                    return br.target
            return node.branches[0].target if node.branches else "__end__"

        return _route

    def _build_decision_path_map(self, node: PipelineNode) -> dict[str, str]:
        """构建 decision 节点的 path_map。"""
        path_map: dict[str, str] = {}
        for br in node.branches:
            target_name = self._node_names.get(br.target, END)
            path_map[br.target] = target_name
        path_map.setdefault("__end__", END)
        return path_map

    def _make_confirm_router(self, node: PipelineNode) -> Callable:
        """生成 confirm 节点的路由函数。"""

        def _route(state: PipelineState) -> str:
            choice = state.get("human_response", "")
            route = node.confirm_branches.get(choice, "")
            if route:
                return route
            # 默认回退
            return list(node.confirm_branches.values())[0] if node.confirm_branches else "__end__"

        return _route

    def _build_confirm_path_map(self, node: PipelineNode) -> dict[str, str]:
        """构建 confirm 节点的 path_map。"""
        path_map: dict[str, str] = {}
        for choice, target in node.confirm_branches.items():
            path_map[target] = self._node_names.get(target, END)
        path_map.setdefault("__end__", END)
        return path_map


# ====================================================================
# 便捷函数
# ====================================================================

def compile_pipeline(
    pipeline_def: PipelineDefinition,
    checkpointer: BaseCheckpointSaver | None = None,
) -> StateGraph:
    """快捷方法：将管道定义编译为 LangGraph 图。"""
    return PipelineGraphCompiler(pipeline_def, checkpointer).compile()


async def execute_pipeline(
    pipeline_def: PipelineDefinition,
    context: dict[str, Any],
    webhook: str = "",
    config: dict[str, Any] | None = None,
) -> PipelineState:
    """快捷方法：编译并执行一条流水线。"""
    compiler = PipelineGraphCompiler(pipeline_def)
    return await compiler.execute(context, webhook, config)

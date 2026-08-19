"""
执行记录持久化存储 — SQLite 实现。

存储每次流水线执行的生命周期记录和事件历史。
服务重启后可通过 list_by_status() 恢复中断的 run。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Literal

JobStatus = Literal["pending", "running", "completed", "failed", "paused"]

_DEFAULT_DB_PATH = Path.home() / ".baize" / "orchestration" / "runs.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    pipe_type   TEXT NOT NULL,
    status      TEXT NOT NULL,
    context     TEXT NOT NULL DEFAULT '{}',
    webhook     TEXT NOT NULL DEFAULT '',
    dedup_key   TEXT,
    created_at  REAL NOT NULL,
    started_at  REAL,
    ended_at    REAL,
    error       TEXT NOT NULL DEFAULT '',
    nodes       TEXT NOT NULL DEFAULT '{}',
    events      TEXT NOT NULL DEFAULT '[]',
    report      TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_dedup_key
    ON runs(dedup_key) WHERE dedup_key IS NOT NULL AND dedup_key != '';
"""


class RunRecord:
    """一次流水线执行记录。"""

    def __init__(
        self,
        run_id: str,
        pipeline_id: str,
        pipe_type: str,
        status: JobStatus = "pending",
    ):
        self.run_id = run_id
        self.pipeline_id = pipeline_id
        self.pipe_type = pipe_type
        self.status: JobStatus = status
        self.context: dict[str, Any] = {}
        self.webhook: str = ""
        self.dedup_key: str = ""
        self.created_at = time.time()
        self.started_at: float | None = None
        self.ended_at: float | None = None
        self.error: str = ""

        # 节点执行记录
        self.nodes: dict[str, dict[str, Any]] = {}

        # 事件历史（供重连时补齐）
        self.events: list[dict[str, Any]] = []

        # 最终输出
        self.report: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_id": self.pipeline_id,
            "pipe_type": self.pipe_type,
            "status": self.status,
            "context": self.context,
            "webhook": self.webhook,
            "dedup_key": self.dedup_key,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "error": self.error,
            "nodes": self.nodes,
            "events": self.events,
            "events_count": len(self.events),
            "report": self.report,
        }

    def brief(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_id": self.pipeline_id,
            "pipe_type": self.pipe_type,
            "status": self.status,
            "created_at": self.created_at,
            "error": self.error,
        }


class RunStore:
    """运行记录仓库 — SQLite 持久化，线程安全。"""

    def __init__(self, db_path: str | Path | None = None):
        self._db_path = str(db_path or os.environ.get("BAIZE_RUNS_DB") or _DEFAULT_DB_PATH)
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    # ------------------------------------------------------------ 内部工具
    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> RunRecord:
        rec = RunRecord(row["run_id"], row["pipeline_id"], row["pipe_type"], row["status"])
        rec.context = json.loads(row["context"] or "{}")
        rec.webhook = row["webhook"] or ""
        rec.dedup_key = row["dedup_key"] or ""
        rec.created_at = row["created_at"]
        rec.started_at = row["started_at"]
        rec.ended_at = row["ended_at"]
        rec.error = row["error"] or ""
        rec.nodes = json.loads(row["nodes"] or "{}")
        rec.events = json.loads(row["events"] or "[]")
        rec.report = row["report"] or ""
        return rec

    def _fetch(self, run_id: str) -> RunRecord | None:
        cur = self._conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        row = cur.fetchone()
        return self._row_to_record(row) if row else None

    def _persist(self, rec: RunRecord) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO runs
               (run_id, pipeline_id, pipe_type, status, context, webhook, dedup_key,
                created_at, started_at, ended_at, error, nodes, events, report)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                rec.run_id,
                rec.pipeline_id,
                rec.pipe_type,
                rec.status,
                json.dumps(rec.context, ensure_ascii=False),
                rec.webhook,
                rec.dedup_key or None,
                rec.created_at,
                rec.started_at,
                rec.ended_at,
                rec.error,
                json.dumps(rec.nodes, ensure_ascii=False),
                json.dumps(rec.events, ensure_ascii=False),
                rec.report,
            ),
        )
        self._conn.commit()

    # ------------------------------------------------------------ 基础操作
    def create(self, record: RunRecord) -> RunRecord:
        """创建记录；若 dedup_key 已存在则返回既有记录（幂等）。"""
        with self._lock:
            if record.dedup_key:
                existing = self._conn.execute(
                    "SELECT * FROM runs WHERE dedup_key = ?", (record.dedup_key,)
                ).fetchone()
                if existing:
                    return self._row_to_record(existing)
            self._persist(record)
            return record

    def get(self, run_id: str) -> RunRecord | None:
        with self._lock:
            return self._fetch(run_id)

    def update_status(self, run_id: str, status: JobStatus, error: str = "") -> None:
        with self._lock:
            rec = self._fetch(run_id)
            if not rec:
                return
            rec.status = status
            if error:
                rec.error = error
            if status == "running" and rec.started_at is None:
                rec.started_at = time.time()
            if status in ("completed", "failed") and rec.ended_at is None:
                rec.ended_at = time.time()
            self._persist(rec)

    def add_event(self, run_id: str, event: dict[str, Any]) -> None:
        """追加事件到历史列表。"""
        with self._lock:
            rec = self._fetch(run_id)
            if not rec:
                return
            rec.events.append(event)
            self._persist(rec)

    def add_node_record(self, run_id: str, node_id: str, record: dict[str, Any]) -> None:
        with self._lock:
            rec = self._fetch(run_id)
            if not rec:
                return
            rec.nodes[node_id] = record
            self._persist(rec)

    def set_final_state(self, run_id: str, state: dict[str, Any]) -> None:
        with self._lock:
            rec = self._fetch(run_id)
            if not rec:
                return
            final_status = state.get("status", "completed")
            rec.status = final_status
            rec.nodes = state.get("nodes", rec.nodes)
            rec.report = self._extract_report(state)
            rec.error = state.get("error", "")
            if rec.ended_at is None:
                rec.ended_at = time.time()
            self._persist(rec)

    @staticmethod
    def _extract_report(state: dict[str, Any]) -> str:
        """提取最终报告：
        1. 优先顶层 report；
        2. 其次名为 report 的节点输出（含内层 subpipeline 节点）；
        3. 回退到最后一个成功完成的节点输出。
        """
        report = state.get("report", "") or ""
        if report:
            return report
        nodes = state.get("nodes") or {}
        rep = nodes.get("report")
        if isinstance(rep, dict):
            report = rep.get("output", "") or ""
            if not report:
                inner = rep.get("nodes", {})
                if isinstance(inner, dict):
                    report = (inner.get("report") or {}).get("output", "") or ""
            if report:
                return report
        # 回退：取最后一个 status=completed 且含 output 的节点
        for rec in nodes.values():
            if not isinstance(rec, dict):
                continue
            if rec.get("status") == "completed" and rec.get("output"):
                report = rec.get("output", "")
        if not isinstance(report, str):
            report = str(report)
        return report

    def list_runs(
        self,
        pipeline_id: str | None = None,
        status: JobStatus | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """列出运行记录（按创建时间倒序）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?",
                (limit * 4,),
            ).fetchall()
        records = [self._row_to_record(r) for r in rows]
        if pipeline_id:
            records = [r for r in records if r.pipeline_id == pipeline_id]
        if status:
            records = [r for r in records if r.status == status]
        return [r.brief() for r in records[:limit]]

    def get_events_since(self, run_id: str, last_event_id: str = "") -> list[dict[str, Any]]:
        """获取指定事件之后的增量事件（用于重连补齐）。"""
        with self._lock:
            rec = self._fetch(run_id)
            if not rec:
                return []
            if last_event_id:
                try:
                    idx = next(
                        i for i, e in enumerate(rec.events)
                        if e.get("event_id") == last_event_id
                    )
                    return rec.events[idx + 1:]
                except StopIteration:
                    return []
            return rec.events

    # ------------------------------------------------------------ 去重 / 恢复
    def find_by_dedup_key(self, dedup_key: str) -> RunRecord | None:
        """按去重键查找既有 run（幂等提交用）。"""
        if not dedup_key:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE dedup_key = ?", (dedup_key,)
            ).fetchone()
            return self._row_to_record(row) if row else None

    def list_by_status(self, statuses: list[str]) -> list[RunRecord]:
        """按状态列表查询（服务重启后恢复中断 run 用）。"""
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM runs WHERE status IN ({placeholders}) ORDER BY created_at",
                statuses,
            ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass


# ====================================================================
# 流水线激活状态管理
# ====================================================================

class PipelineActivationStore:
    """自动化流水线激活状态管理。

    auto 类型的流水线创建后默认关闭，需要用户手动开启后才开始接收数据。
    manual 类型的流水线始终处于可用状态，供对话时选择。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._activations: dict[str, bool] = {}  # pipeline_id -> is_active

    def is_active(self, pipeline_id: str) -> bool:
        with self._lock:
            return self._activations.get(pipeline_id, False)

    def activate(self, pipeline_id: str) -> None:
        with self._lock:
            self._activations[pipeline_id] = True

    def deactivate(self, pipeline_id: str) -> None:
        with self._lock:
            self._activations[pipeline_id] = False

    def get_all_active(self) -> list[str]:
        with self._lock:
            return [pid for pid, active in self._activations.items() if active]

    def get_status(self, pipeline_id: str) -> dict[str, Any]:
        return {
            "pipeline_id": pipeline_id,
            "active": self.is_active(pipeline_id),
        }


# ====================================================================
# 全局实例
# ====================================================================

_default_store = RunStore()
_default_activation_store = PipelineActivationStore()


def get_run_store() -> RunStore:
    return _default_store


def get_activation_store() -> PipelineActivationStore:
    return _default_activation_store

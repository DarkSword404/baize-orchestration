# Baize 流水线设计评审报告（修订版）

> 版本: v2（含 AI-SOAR 方向修订） | 日期: 2026-08-18
> 范围: `baize-orchestration` 流水线（YAML 模板 → PipelineDefinition → LangGraph 编译 → 执行）

---

## 0. 产品定位：AI-SOAR（AI 编排安全响应）

用户明确的产品方向：**类似于 SOAR，但大部分决策由 AI 完成**。

与规则型 SOAR 相比：

| 维度 | 规则型 SOAR（XSOAR / Splunk SOAR / Shuffle） | Baize AI-SOAR |
|---|---|---|
| 决策方式 | 预置 if/else 条件匹配分支 | LLM 综合状态与证据，路由分支并输出 reasoning |
| 表达能力 | 条件写死，模糊场景退化 | 可综合工具输出、历史经验、上下文模糊推理 |
| 运维成本 | 改规则需改 YAML/代码 + 回归 | 改 prompt 描述即可，运营同学可直接维护 |
| 调试审计 | 分支命中原因不透明 | 每个决策自带 `reasoning`，天然审计日志 |

**关键推论**：原设计里 `decision` 节点"从不执行、条件从不求值"不是缺陷，而是核心特性未实现。
修复方向不是补规则引擎，而是实现 `ai_decision`（LLM 路由节点）。

---

## 1. 架构现状

```
YAML 模板 (templates/*.py)                    Python dict
        │  yaml_loader.load_pipeline()         ↓
        ▼                                     PipelineDefinition (node_types.py)
   validators.validate_pipeline()             ↓
        │                                     PipelineGraphCompiler (compiler.py)
        ▼                                     StateGraph + MemorySaver
   LangGraph graph ──execute_stream──▶ run events (SSE)
        │
        ▼
   node executors: agent / decision / parallel / confirm / transform / subpipeline
```

- 管道类型: `auto` / `manual`（含 confirm 人工确认节点，`interrupt()` 暂停）
- 路由: 顺序边 + `decision`/`confirm` 的条件边
- 状态: `PipelineState`（TypedDict + `operator.add` 合并 messages）
- 事件: 节点 start / done / failed / pipeline paused 等

---

## 2. P0 级问题（必须修，影响可用性）

### P0-1. runner 对 async generator 使用 `await`（流水线永远跑不起来）

`runner.py` 中 `_execute` 内：

```python
await compiler.execute_stream(pipeline, initial_state, cfg)
```

而 `compiler.execute_stream` 是 **async generator**（函数体内有 `yield`）。
`await` 一个 async generator 直接抛 `TypeError: object async_generator can't be used in 'await' expression`。

**修复**：改用 `async for run_id, final in compiler.execute_stream(...)` 消费流，
或在 runner 内收集事件。这是之前 pentest run 停在 `running` 的直接原因。

### P0-2. confirm 恢复因 MemorySaver 重建而失效

`compiler.py` 中 `compile()` 每次调用都新建 `StateGraph` + `MemorySaver`。
而 `get_confirm_node_state()` / `resume_after_confirm()` 每次调用都重新 `self.compile()`：

- `submit` 时构建的图 A（MemorySaver A）持有 checkpoints；
- 轮询 / resume 时重新编译的图 B（MemorySaver B）是空的 → `get_state` 返回 None，
  `ainvoke(Command(resume=...))` 报 `ValueError: Node __interrupt__` 或找不到 checkpoint。

**修复**：将 `checkpointer`（MemorySaver）提升为 compiler 实例级单例并复用；
或改为 SQLite checkpointer（跨重启），resume 不再重建图。

### P0-3. run_id 双轨制

- API 层 `POST /runs` 生成 `run_id`（提交用）；
- compiler `execute_stream` 内部又用 `config["configurable"]["thread_id"]` 作为 LangGraph 线程 id，
  事件流里的 run_id 与 API 的 run_id 不一致 → 前端 / 轮询对不上号。

**修复**：统一以 API 的 `run_id` 作为 thread_id 传入 compiler，事件流只透传该 id。

---

## 3. P1 级问题（影响功能完整，重构期修）

### P1-1. decision 节点从不执行，条件表达式从不求值

- `compiler._find_next_node()` 显式跳过 `decision` 类型的节点（"不作为顺序边的目标"），
  导致 decision 节点在实际图中不可达；
- `_evaluate_condition` 只做模板渲染不做 DSL 求值；
- 也没有 `ai_decision` 的概念。

**修复（按 AI-SOAR 方向）**：
1. 新增节点类型 `ai_decision`：LLM 输入 = 当前状态摘要 + 可选分支描述，
   输出 = `{branch, reasoning, confidence}`；`reasoning` 写入节点 record 供审计；
2. `decision` 保留为轻量规则分支（when 表达式 + 兜底 default），供无需 LLM 的确定性场景；
3. compiler 将 `ai_decision` 作为合法顺序目标 + 条件边目标，`ai_decision` 的输出 `branch` 决定路由。

### P1-2. 双编译器（engine.py / graph.py 与 compiler.py 并存）

`engine.py` + `graph.py` 是旧版同名 `PipelineGraphCompiler(node_factory)`，
与 `compiler.py` 的 `PipelineGraphCompiler(pipeline)` 并存，极易误用。

**修复**：删除旧实现，保留单一编译器。

### P1-3. 失败语义缺失

- 节点异常 → `_record_failed` → 写状态，但图上没有失败边，也没有
  retry / on_error 分支（如"扫描失败 → 走降级分支"）；
- run 的状态流转无失败终结路径（failed 后无法 resume / 重跑）。

### P1-4. parallel 节点是假的（顺序执行）

`parallel` 节点当前是顺序 await，未并发。

### P1-5. 事件模型粗糙

`on_chain_end` 只发 `node_completed` / `pipeline_paused`，缺 `node_failed`、
节点输入输出明细、token 用量等。AI-SOAR 的 reasoning 审计也需要进入事件流。

### P1-6. 条件表达式引擎缺失

`when` / branch `condition` 无解析器（无 DSL 文档）。AI-SOAR 下可弱化，但仍需
简单表达式（比较、contains、阈值）支持。

---

## 4. P2 安全与治理

- **auto 类型模板含 confirm 节点**（语义矛盾）：`pentest` 模板声明 `type: auto`
  却含 `exploit_confirm`。auto 管道不应包含人工确认，需校验。
- **Prompt 注入面**：agent 指令里拼入 `context` / 上游输出，未做注入防护与内容过滤。
- **无凭据 Vault**：工具 / agent 需要 API key 时硬编码在配置里。
- **仅内存 semaphore**：并发控制是进程内，多 worker 下失效。

---

## 5. P3 工程化

- 仅内存 store：MemorySaver + 内存 run_store，重启即丢 → Phase 2 换 SQLite。
- 无模板快照 / dry-run：模板变更无法审计，发布前无法试跑。
- DSL 未文档化：YAML 字段（branches、when、goto、confirm_branches）无 schema 文档。
- 无 trigger/事件源：还不能"告警到达 → 自动拉起 playbook"。

---

## 6. 业界对标（简要）

| 产品 | 模型 | 决策 | 值得借鉴 |
|---|---|---|---|
| Cortex XSOAR | Playbook = 可视化工作流 | 规则条件 | 审计事件、回滚、调试面板 |
| Splunk SOAR | 顺序 + 分支 playbook | 决策节点规则 | 调查时间线、证据对象模型 |
| Shuffle | 开源 SOAR | 逻辑节点 | 轻量、社区模板 |
| Airflow/Prefect | DAG | 传感器/条件 | 失败语义、重试、调度、可观测性 |
| Temporal | 长时工作流 | 决策代码化 | 持久执行、信号/查询、确定性重放 |
| LangGraph | 状态机图 | 条件边 | interrupt/resume、checkpoint 模型 |
| Dify | Agent 工作流 | LLM 节点 + 分支 | LLM 节点、变量系统、知识库接入 |

Baize 的差异化：**SOAR 的剧本骨架 + LangGraph 的执行引擎 + LLM 决策中枢**，
决策既有规则兜底（decision）又有 AI 增强（ai_decision），且自带 reasoning 审计。

---

## 7. 修复路线图

### Phase 1（本次）：让流水线"能跑 + 决策是 AI 的"

1. [x] P0-1: runner 用 `async for` 消费 `execute_stream`
2. [x] P0-2: compiler 复用同一个 checkpointer，confirm resume 生效
3. [x] P0-3: run_id 单轨制（thread_id = API run_id）
4. [x] P1-1: 新增 `ai_decision` 节点（LLM 路由 + reasoning 审计）
5. [x] P1-1: compiler/graph 支持 `ai_decision` 分支路由 + `decision` 条件求值兜底
6. [x] 删除旧编译器 engine.py / graph.py
7. [x] 更新 pentest 模板为 AI-SOAR 示例（ai_decision 路由 + confirm 人工确认）

### Phase 2：持久化 + 健壮性

- SQLite checkpointer / run_store（重启不丢、可回放）
- 失败语义：retry、on_error 分支、run 级失败终结
- parallel 真并发（asyncio.gather / 子图）
- 事件模型扩充：node_failed、输入输出快照、reasoning 进事件流
- 条件表达式 DSL + 校验器（比较 / contains / 阈值）

### Phase 3：安全 + 完整 SOAR 形态

- Vault 凭据管理；prompt 注入防护；auto/manual 语义校验
- 模板快照 + dry-run 试跑
- trigger/事件源（告警 → 自动拉起）
- 调度、重试、回滚、审计面板

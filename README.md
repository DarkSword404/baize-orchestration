# Baize Orchestration

基于 LangGraph 的流水线编排模块，作为 **Baize Core 的扩展模块**（通过 `baize.modules` entry point 动态注册）。

## 架构关系

- **baize-orchestration**：独立项目，提供流水线编排（Pipeline Orchestration），包含：
  - LangGraph 流水线图（`graph.py`、`engine.py`、`nodes/`）
  - YAML 模板加载与编译（`yaml_loader.py`、`compiler.py`）
  - 流水线 API 路由（`api.py`，挂载到 core 的 `/api/v1/pipelines/*`）
  - 内置模板（SOC告警研判、自动化渗透测试、漏洞扫描报告处理、钓鱼邮件智能分析）
  - 两级模型：流水线 模板 → 实例（实例化快照 / 启用停用 / 模板同步 / 运行历史）
  - 长驻会话：实例绑定接收器并启用后，Supervisor 自动 claim 告警收件箱并行研判（每次入站 = 独立 run / 独立对话）
- **baize-core**：核心框架项目，提供 API 服务、Agent 管理、存储与模块发现机制。

安装 orchestration 后，core 启动时通过 `entry_points(group="baize.modules")` 自动发现并调用 `register(app)`，无需修改 core 代码。

## 内置模板

| 模板 | 说明 |
|------|------|
| SOC告警研判 | `soc_triage.yml` |
| 自动化渗透测试 | `penetration_test.yml` |
| 漏洞扫描报告处理 | `vuln_scan.yml` |
| 钓鱼邮件智能分析 | `phishing_analysis.yml` |

## 接入 Baize Core

本模块作为 Baize Core 的扩展，通过 `baize.modules` entry point 自动发现并注册，**安装后无需修改 core 的任何代码**。

### 前置条件

| 依赖 | 要求 |
|------|------|
| Python | >= 3.10 |
| baize-core | >= 1.8.0（`baize.modules` 自动发现机制自 v1.3.1 引入；v1.6.0 的告警研判依赖 core v2.0 收件箱，推荐搭配 core v2.0.0） |

### 安装

**方式一：正式环境（PyPI）**

```bash
pip install baize-orchestration
```

**方式二：本地开发**：见下方「本地开发安装」章节。

安装后，`pyproject.toml` 声明的 entry point 即被 pip 写入环境中：

```toml
[project.entry-points."baize.modules"]
orchestration = "baize.orchestration:register"
```

### 接入原理

Baize Core 启动时调用 `_discover_and_load_modules(app)`（`src/baize/api/app.py`），扫描所有已安装包中的 `baize.modules` entry point，逐个调用 `register(app)` 将编排路由挂载到 FastAPI 应用（`/api/v1/pipelines/*`）。core 侧对未安装模块做了容错（`ModuleNotFoundError` 时跳过），未安装本模块不影响 core 正常运行。

### 启动与验证

1. 启动 Baize Core（后端默认端口 `8001`，前端 `5173`）：

```bash
./start.sh
```

2. 确认加载成功——后端日志中出现：

```
INFO  baize.api.app: 已加载模块: orchestration
```

3. 验证编排 API 可用（需要 API 密钥，即启动时输出的登录凭证）：

```bash
curl -H "X-Baize-API-Key: <你的API密钥>" \
     http://127.0.0.1:8001/api/v1/pipelines/templates
```

返回内置模板列表（`soc_triage`、`penetration_test`、`vuln_scan`、`phishing_analysis`）即表示接入成功。

### 版本兼容性

- **baize-core >= 1.8.0（推荐搭配 core v2.0.0）**：`baize.modules` 自动发现机制自 v1.3.1 起提供，更早版本不支持动态加载；
  自 v1.6.0 起告警研判 / 长驻会话依赖 core 的持久化告警收件箱（Alert Inbox，core v2.0 引入），
  未安装新 core 时自动降级为仅支持手动执行。
- `langgraph` / `langgraph-checkpoint` 等依赖由本模块声明并自动安装。
- 认证与 core 保持一致：开启认证后需携带 `X-Baize-API-Key` 或 `Authorization: Bearer <token>`。

## 本地开发安装

```bash
python3 -m venv .venv
.venv/bin/pip install -e ../baize-core   # 先安装 core 依赖
.venv/bin/pip install -e .
```

## 更新日志

### v1.6.0（当前）

- 🔄 **流水线两级模型（模板 → 实例）**：新增实例存储（`instance_store.py`），实例创建时快照模板定义，
  支持 启用/停用（绑定接收器、设置并行上限，默认 10）/ 删除 / 模板同步 / 运行历史；
  新增节点 `nodes/end.py`（结束对话：终态归档摘要、回收对话）
- 🤖 **长驻自动研判会话**：新增 `session.py` 的 Supervisor——为已启用实例启动长驻 asyncio 调度，
  从 core 持久化告警收件箱（Alert Inbox）claim 告警 → 独立 Worker / 独立 run / 独立对话执行研判 →
  写回状态；at-least-once + runs 去重幂等兜底；失败退避重试、超限进死信可 API 重放
- 📋 **激活状态持久化与恢复**：激活记录落 `runs.db` 的 activations 表，服务重启后自动 `resume_all()`
  恢复长驻会话；过期租约定时回收，崩溃不丢未完成告警
- 🛠️ **SOC 告警研判模板修复**：内置模板不再静态化——告警内容、绑定 agent 运行期动态注入，
  修复合流/绑定场景下研判收不到真实告警数据的问题
- ⚙️ **调度与模型加固**：`runner.py` 并发控制对齐实例级 `max_concurrency`（避免全局串行化）；
  `node_types` / `state` / `validators` / `compiler` 扩展新节点字段（`target` / 决策提示 /
  超时与重试 / 失败分支等）与校验
- 🔌 **API 扩展**：`/api/v1/pipelines/instances*`（CRUD / enable / disable / sync / test / status）、
  `/api/v1/alert-triage/*`（队列 / 告警列表 / 死信重放）、模板管理（删除 / 重置 / 统一模板库），
  挂载于 core `/api/v1` 下
- 🚀 **升级**：版本号 1.6.0；core 依赖放宽为 `baize-core>=1.8.0,<3.0.0`（推荐 core v2.0.0）

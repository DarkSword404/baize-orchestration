# Baize Orchestration

基于 LangGraph 的流水线编排模块，作为 **Baize Core 的扩展模块**（通过 `baize.modules` entry point 动态注册）。

## 架构关系

- **baize-orchestration**：独立项目，提供流水线编排（Pipeline Orchestration），包含：
  - LangGraph 流水线图（`graph.py`、`engine.py`、`nodes/`）
  - YAML 模板加载与编译（`yaml_loader.py`、`compiler.py`）
  - 流水线 API 路由（`api.py`，挂载到 core 的 `/api/v1/pipelines/*`）
  - 内置模板（SOC告警研判、自动化渗透测试、漏洞扫描报告处理、钓鱼邮件智能分析）
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
| baize-core | >= 1.3.1（模块发现机制自 v1.3.1 引入，推荐使用最新 v1.5.x） |

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

- **baize-core >= 1.3.1**：`baize.modules` 自动发现机制自 v1.3.1 起提供，更早版本不支持动态加载。
- `langgraph` / `langgraph-checkpoint` 等依赖由本模块声明并自动安装。
- 认证与 core 保持一致：开启认证后需携带 `X-Baize-API-Key` 或 `Authorization: Bearer <token>`。

## 本地开发安装

```bash
python3 -m venv .venv
.venv/bin/pip install -e ../baize-core   # 先安装 core 依赖
.venv/bin/pip install -e .
```

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

## 本地开发安装

```bash
python3 -m venv .venv
.venv/bin/pip install -e ../baize-core   # 先安装 core 依赖
.venv/bin/pip install -e .
```

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AIAC2025_v2 is a WorldQuant BRAIN quantitative factor (alpha) mining tool. It automates the workflow: authenticate → discover data → build alpha expressions → run backtests → archive results.

## Setup

```bash
pip install -r requirements.txt
```

Credentials go in `config/credentials.json` as `["email", "password"]`.

## Commands

```bash
# Interactive menu (8 options)
python main.py

# CLI modes
python main.py datasets --region USA
python main.py fields --region USA --dataset pv1
python main.py backtest --file alphas.json --region USA

# Run all workflow tests
python test_workflow.py

# Run a specific test step (1–9)
python test_workflow.py --step 6   # single backtest
python test_workflow.py --step 7   # batch backtest

# Alpha Factory Pipeline（全流程 + research_report.json）
python main.py pipeline --region USA --datasets pv1
python main.py pipeline -r USA -d pv1,analyst15 --output-dir research/my_run
python main.py pipeline -r USA -d pv1 --no-self-heal
```

**Alpha Factory Pipeline**（菜单 11 / `python main.py pipeline`）：AI 数据分析 → 模板调度(20~30) → Alpha 生成 → 去重 → 聚类 → 批量回测 → 筛选 → 错误自愈 → 保存高质量 Alpha；输出目录下生成 `research_report.json`（含 templates_used, alpha_generated, after_dedup, clusters, backtest_success）。各步骤可在 `ai/alpha_factory_pipeline.py` 中独立调用（`step_ai_analysis`, `step_template_schedule`, `step_alpha_generation`, `step_dedup`, `step_cluster`, `step_backtest`, `step_filter`, `step_self_heal`, `step_save_high_quality`）。

## Architecture

**Core workflow**: `SessionManager` → `DataManager` → `AlphaBuilder` → `BacktestRunner`

- `core/ace_lib.py` — Low-level WorldQuant BRAIN API SDK wrapper (`SingleSession` singleton, `simulate_single_alpha`, `get_simulation_result_json`)
- `core/session_manager.py` — Auth lifecycle on top of ace_lib; auto-reconnect on timeout
- `core/data_manager.py` — Fetches and caches operators, datasets, and fields to `cache/`
- `core/alpha_builder.py` — Builds/validates alpha expression configs; supports templates and batch generation
- `core/backtest_runner.py` — Executes single or concurrent batch backtests; saves markdown reports to `research/<REGION>_<name>_<timestamp>/`
- `core/helpful_functions.py` — Result formatting and persistence utilities
- `config/settings.py` — All constants: paths, region→universe defaults, backtest defaults (decay, truncation, neutralization), filter thresholds (MIN_SHARPE=1.0, MIN_FITNESS=0.5, MAX_TURNOVER=0.70), batch config (BATCH_SIZE=50, MAX_CONCURRENT_SIMS=3)

**Caching**: `DataManager` writes JSON to `cache/` keyed by region/dataset to avoid redundant API calls. Clear cache files manually to force refresh.

**Research output**: Each backtest run archives a `report.md` under `research/` with a timestamped directory name.

**Supported regions**: USA (TOP3000), CHN (TOP2000U), IND (TOP500), EUR, ASI, GLB, JPN, KOR, TWN — defaults defined in `config/settings.py`.

## 交互与开发准则

**语言偏好**：所有技术讨论、回复及代码注释一律使用中文。

**重大变更流程**：在进行任何大规模改动（新增功能、架构优化、重构）之前，必须先输出"改动设计文档"，包含：
- 修改目标
- 涉及的具体文件清单
- 核心逻辑变更说明
- 潜在风险点

待用户回复"确认"后，方可开始修改代码。

**故障复盘**：代码执行出错时，优先对比"改动设计文档"中的变更点，快速定位并说明出错位置。

# AIAC2025_v3 项目文档

> WorldQuant BRAIN 量化因子（Alpha）自动化挖掘工具

## 目录

- [项目概述](#项目概述)
- [快速开始](#快速开始)
- [功能模块](#功能模块)
- [命令行接口](#命令行接口)
- [核心架构](#核心架构)
- [模板系统](#模板系统)
- [配置说明](#配置说明)
- [修改指南](#修改指南)
- [常见问题](#常见问题)

---

## 项目概述

AIAC2025_v3 是一个 WorldQuant BRAIN 平台的量化因子挖掘自动化工具。核心功能：

1. **自动登录认证** - 管理 BRAIN 平台会话
2. **数据集探索** - 获取区域、数据集、字段元数据
3. **AI 数据分析** - 基于字段特征推荐研究方向
4. **Alpha 批量生成** - 模板化生成大量候选 Alpha 表达式
5. **批量回测** - 并发执行回测并收集结果
6. **智能筛选** - 按 Sharpe/Fitness/Turnover 筛选高质量因子
7. **错误自愈** - 自动修复语法错误并重试

### 技术栈

- Python 3.10+
- BRAIN API（通过 `core/ace_lib.py` 封装）
- AI 分析（支持多种 LLM 后端）

---

## 快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

### 配置凭证

在 `config/credentials.json` 中配置 BRAIN 平台登录凭证：

```json
["your_email@example.com", "your_password"]
```

### 运行 Pipeline

```bash
# 基础用法
python main.py pipeline --region USA --datasets pv1

# 使用针对性模板
python main.py pipeline --region USA --datasets analyst4 --template-mode specialized
```

---

## 功能模块

### 1. 会话管理 (`core/session_manager.py`)

管理 BRAIN 平台登录会话，支持自动重连。

```python
from core.session_manager import SessionManager

session = SessionManager.login()
```

### 2. 数据管理 (`core/data_manager.py`)

获取和缓存数据集、字段、操作符元数据。

```python
from core.data_manager import DataManager

# 获取数据集列表
datasets = DataManager.get_datasets(session, "USA")

# 获取字段列表
fields = DataManager.get_fields(session, "USA", "pv1")

# 获取操作符列表
operators = DataManager.get_operators(session)
```

### 3. Alpha 生成 (`ai/alpha_generator.py`)

基于模板批量生成 Alpha 表达式。

```python
from ai.alpha_generator import generate_alphas_from_expressions

alphas = generate_alphas_from_expressions(
    template_expressions=["rank(ts_delta({field}, {window}))"],
    recommended_fields=[{"field_id": "close", "normalized_type": "vector"}],
)
```

### 4. 批量回测 (`core/backtest_runner.py`)

并发执行多个 Alpha 的回测。

```python
from core.backtest_runner import BacktestRunner

runner = BacktestRunner(session)
results = runner.run_batch(alpha_items, region="USA")
```

### 5. Pipeline 流程 (`ai/alpha_factory_pipeline.py`)

完整的自动化流程：

```
AI 数据分析 → 模板调度 → Alpha 生成 → 去重 → 聚类 → 批量回测 → 筛选 → 错误自愈 → 保存
```

---

## 命令行接口

### 交互模式

```bash
python main.py
```

显示 8 个功能选项的交互菜单。

### CLI 模式

#### 数据集列表

```bash
python main.py datasets --region USA
python main.py datasets --region USA --refresh  # 强制刷新缓存
```

#### 字段列表

```bash
python main.py fields --region USA --dataset pv1
python main.py fields --region USA --dataset pv1 --refresh
```

#### 批量回测

```bash
python main.py backtest --file alphas.json --region USA
```

#### Alpha Factory Pipeline

```bash
# 基础用法
python main.py pipeline --region USA --datasets pv1

# 多数据集
python main.py pipeline --region USA --datasets pv1,analyst15

# 指定输出目录
python main.py pipeline --region USA --datasets pv1 --output-dir research/my_study

# 禁用错误自愈
python main.py pipeline --region USA --datasets pv1 --no-self-heal

# 使用针对性模板
python main.py pipeline --region USA --datasets analyst4 --template-mode specialized

# 使用自定义模板
python main.py pipeline --region USA --datasets pv1 --templates path/to/templates.json
```

### 参数说明

| 参数 | 说明 |
|------|------|
| `--region, -r` | 区域代码（USA/CHN/IND/EUR/ASI/GLB/JPN/KOR/TWN） |
| `--datasets, -d` | 数据集 ID，逗号分隔 |
| `--output-dir, -o` | 输出目录路径 |
| `--no-self-heal` | 禁用错误自愈 |
| `--template-mode` | 模板模式：`default`（通用）或 `specialized`（针对性） |
| `--templates` | 自定义模板文件路径 |
| `--refresh` | 强制刷新缓存 |

---

## 核心架构

### 目录结构

```
AIAC2025_v3/
├── main.py                    # 入口：交互菜单 + CLI
├── config/
│   ├── settings.py            # 全局配置（路径、阈值、默认值）
│   ├── alpha_config.py        # Alpha 相关配置
│   ├── templates.json         # 通用模板库（77 个）
│   └── dataset_templates/     # 针对性模板目录
│       ├── analyst4_templates.json
│       ├── analyst4_guidance.json
│       └── ...
├── core/
│   ├── ace_lib.py             # BRAIN API 底层封装
│   ├── session_manager.py     # 会话管理
│   ├── data_manager.py        # 数据获取与缓存
│   ├── alpha_builder.py       # Alpha 表达式构建
│   └── backtest_runner.py     # 批量回测执行
├── ai/
│   ├── alpha_factory_pipeline.py  # Pipeline 主流程
│   ├── template_loader.py     # 模板加载器
│   ├── template_scheduler.py  # 模板调度器
│   ├── alpha_generator.py     # Alpha 批量生成
│   ├── alpha_deduplicator.py  # Alpha 去重
│   ├── alpha_cluster.py       # Alpha 聚类
│   ├── data_analysis.py       # AI 数据分析
│   ├── backtest_loop.py       # 回测循环
│   └── researcher_brain.py    # AI 研究员
├── cache/                     # 元数据缓存目录
├── research/                  # 研究报告输出目录
└── docs/                      # 文档目录
```

### 数据流

```
用户输入
    ↓
SessionManager.login() → 获取会话
    ↓
DataManager.get_fields() → 获取字段元数据
    ↓
AI 分析 → 推荐字段 + 研究方向
    ↓
模板调度 → 选择 20-30 个模板
    ↓
Alpha 生成 → 生成候选表达式
    ↓
去重 + 聚类 → 减少冗余
    ↓
BacktestRunner.run_batch() → 批量回测
    ↓
筛选 → 保留高质量 Alpha
    ↓
错误自愈 → 修复失败项
    ↓
保存 → research_report.json
```

---

## 模板系统

### 三层模板架构

| 类型 | 位置 | 说明 |
|------|------|------|
| 通用模板 | `config/templates.json` | 77 个模板，适用于 MATRIX 类型字段 |
| VECTOR 模板 | `config/vector_templates.json` | 17 个模板，适用于 VECTOR 类型字段 |
| 针对性模板 | `config/dataset_templates/{dataset}_templates.json` | 按数据集定制，针对特定领域 |

### 字段类型与模板匹配

**BRAIN 平台字段类型：**
| 平台类型 | 内部类型 | 模板要求 |
|----------|----------|----------|
| MATRIX | `vector` | 通用模板（ts_*/rank/zscore 等） |
| VECTOR | `event` | 需要 `vec_*` 操作符转换 |

**VECTOR 字段处理示例：**
```
# 错误用法（会报错）
rank(vector_field)

# 正确用法
rank(vec_avg(vector_field))
rank(ts_delta(vec_sum(vector_field), 10))
```

### 研究方向引导

针对性模板配合研究方向引导（`{dataset}_guidance.json`），指导 AI 按特定策略推荐字段。

**示例：analyst4 引导**

```json
{
  "research_directions": [
    {"name": "预期修正信号", "field_patterns": ["*_flag"]},
    {"name": "预测分歧度", "field_patterns": ["*_high", "*_low"]}
  ],
  "priority_fields": [
    {"field_id": "anl4_adjusted_netincome_ft", "alpha_count": 32031}
  ],
  "guidance_prompt": "..."
}
```

### 模板加载优先级

1. **自定义路径**（`--templates` 参数）
2. **针对性模板**（`--template-mode specialized`，按数据集查找）
3. **通用模板**（默认）

### 扩展新数据集

在 `config/dataset_templates/` 下创建：

```
{dataset_id}_templates.json   # 针对性模板
{dataset_id}_guidance.json    # 研究方向引导
```

即可通过 `--template-mode specialized` 自动加载。

---

## 配置说明

### settings.py 关键配置

```python
# 默认 Universe
REGION_UNIVERSE_DEFAULTS = {
    "USA": "TOP3000",
    "CHN": "TOP2000U",
    "IND": "TOP500",
    ...
}

# 回测默认参数
BACKTEST_DEFAULTS = {
    "decay": 3,
    "truncation": 0.08,
    "neutralization": "INDUSTRY",
}

# 筛选阈值
MIN_SHARPE = 1.0
MIN_FITNESS = 0.5
MAX_TURNOVER = 0.70

# 批量回测
BATCH_SIZE = 50
MAX_CONCURRENT_SIMS = 3
```

### alpha_config.py 关键配置

```python
# 模板参数枚举
TEMPLATE_PARAMS = {
    "window": [5, 10, 20],
    "decay": [3, 5],
    "truncation": [0.01, 0.05],
    "neutralization": ["INDUSTRY", "SECTOR", "MARKET"],
}

# 每轮模板数量
templates_per_round = [20, 30]

# 模板调度分布
TEMPLATE_SCHEDULE_DISTRIBUTION = {
    "time_series": 10,
    "cross_section": 5,
    "pair": 5,
    "complex": 5,
}
```

---

## 修改指南

### 添加新的操作符支持

1. 在 `config/templates.json` 中添加使用新操作符的模板
2. 模板 `operators` 字段声明依赖的操作符
3. `alpha_generator.py` 会自动过滤缺少操作符的模板

### 添加新的数据集支持

1. 使用 `python main.py fields --region USA --dataset {new_dataset}` 获取字段
2. 在 `config/dataset_templates/` 创建针对性模板和引导（可选）
3. 运行 pipeline 测试

### 修改 AI 分析逻辑

- AI 分析入口：`ai/data_analysis.py` 的 `analyze_metadata()`
- Prompt 模板：`ai/prompt_templates.py` 的 `DATA_ANALYSIS_PROMPT`
- 研究方向引导：`config/dataset_templates/{dataset}_guidance.json`

### 修改回测参数

- 默认参数：`config/settings.py` 的 `BACKTEST_DEFAULTS`
- 筛选阈值：`config/settings.py` 的 `MIN_SHARPE`、`MIN_FITNESS`、`MAX_TURNOVER`

### 修改 Pipeline 流程

- 主流程：`ai/alpha_factory_pipeline.py` 的 `run_pipeline()`
- 各步骤可独立调用：`step_ai_analysis()`、`step_template_schedule()` 等

---

## 常见问题

### Q: 如何查看可用的数据集？

```bash
python main.py datasets --region USA
```

### Q: 如何查看数据集的字段？

```bash
python main.py fields --region USA --dataset pv1
```

### Q: 回测失败怎么办？

Pipeline 会自动执行错误自愈，尝试修复语法错误。如需禁用：

```bash
python main.py pipeline --region USA --datasets pv1 --no-self-heal
```

### Q: 如何清除缓存？

缓存位于 `cache/` 目录，可手动删除：

```bash
rm -rf cache/*.json
```

或使用 `--refresh` 参数强制刷新。

### Q: 输出报告在哪里？

默认输出到 `research/alpha_factory_<timestamp>/` 目录，包含：

- `research_report.json` - 汇总报告
- `ai_analysis_result.json` - AI 分析结果
- `metadata_snapshot.json` - 元数据快照
- `high_quality_alphas.json` - 高质量 Alpha

---

## 相关文档

- [CLAUDE.md](../CLAUDE.md) - Claude Code 项目指令
- [针对性模板系统设计](specialized_template_design.md) - 模板系统详细设计
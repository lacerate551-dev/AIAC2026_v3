# AIAC2025_v3 - BRAIN 量化因子挖掘平台

**版本**: v3.1

## 🎯 概览

AIAC2025_v3 是一个完整的 WorldQuant BRAIN 量化因子挖掘工具，提供：
- ✅ **统一 CLI 入口** — `python main.py` 完成全部工作流
- ✅ **核心模块** — 会话管理、数据获取、Alpha构建、回测执行
- ✅ **AI 增强** — 智能数据分析、Alpha生成、策略优化
- ✅ **Alpha Factory Pipeline** — 一键自动化全流程
- ✅ **针对性模板系统** — 按数据集定制的模板和研究方向引导
- ✅ **智能缓存** — 操作符/数据集/字段自动缓存
- ✅ **研究归档** — 回测结果自动归档到 `research/` 目录

## 📁 项目结构

```
AIAC2025_v3/
├── main.py                     # 统一入口 (交互式菜单 + CLI)
├── config/                     # 配置中心
│   ├── settings.py             # 全局设置常量
│   ├── alpha_config.py         # Alpha 相关配置
│   ├── templates.json          # 通用模板库（77 个）
│   ├── dataset_templates/      # 针对性模板目录
│   │   ├── analyst4_templates.json
│   │   ├── analyst4_guidance.json
│   │   └── ...
│   └── credentials.json        # BRAIN平台凭证
├── core/                       # 核心模块
│   ├── ace_lib.py              # WorldQuant BRAIN API SDK
│   ├── session_manager.py      # 会话管理 (登录/重连)
│   ├── data_manager.py         # 数据获取与缓存
│   ├── alpha_builder.py        # Alpha表达式构建
│   └── backtest_runner.py      # 回测执行与报告
├── ai/                         # AI 增强模块
│   ├── alpha_factory_pipeline.py   # Pipeline 主流程
│   ├── template_loader.py      # 模板加载器
│   ├── template_scheduler.py  # 模板调度器
│   ├── alpha_generator.py      # Alpha生成
│   ├── alpha_deduplicator.py   # Alpha去重
│   ├── alpha_cluster.py        # Alpha聚类
│   ├── data_analysis.py        # 数据分析
│   ├── backtest_loop.py        # 回测循环
│   └── researcher_brain.py     # AI研究员
├── cache/                      # 数据缓存
├── research/                   # 研究产出归档
├── docs/                       # 文档
│   ├── PROJECT_GUIDE.md        # 完整项目指南 ⭐
│   └── specialized_template_design.md
└── requirements.txt            # Python依赖
```

## 🚀 快速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置凭证
在 `config/` 目录下创建凭证文件（详见 CLAUDE.md）

### 3. 启动
```bash
# 交互式模式
python main.py

# 命令行模式
python main.py datasets --region USA
python main.py fields --region USA --dataset pv1

# Alpha Factory Pipeline（推荐）⭐
python main.py pipeline --region USA --datasets pv1

# 使用针对性模板
python main.py pipeline --region USA --datasets analyst4 --template-mode specialized
```

## 🤖 Alpha Factory Pipeline

一键自动化全流程：**AI分析 → 模板调度 → Alpha生成 → 去重 → 聚类 → 批量回测 → 筛选 → 错误自愈**

```bash
# 基础用法
python main.py pipeline --region USA --datasets pv1

# 多数据集
python main.py pipeline --region USA --datasets pv1,analyst15

# 使用针对性模板（推荐）⭐
python main.py pipeline --region USA --datasets analyst4 --template-mode specialized

# 指定输出目录
python main.py pipeline --region USA --datasets pv1 --output-dir research/my_study
```

### Pipeline 参数

| 参数 | 说明 |
|------|------|
| `--region, -r` | 区域代码（USA/CHN/IND等） |
| `--datasets, -d` | 数据集ID，逗号分隔 |
| `--output-dir, -o` | 输出目录路径 |
| `--template-mode` | 模板模式：`default`（通用）或 `specialized`（针对性） |
| `--templates` | 自定义模板文件路径 |
| `--no-self-heal` | 禁用错误自愈 |

### 输出报告

Pipeline 结束后输出 `research_report.json`：

```json
{
  "templates_used": 25,
  "alpha_generated": 150,
  "after_dedup": 80,
  "backtest_success": 75,
  "high_quality_count": 12
}
```

## 📖 核心工作流

```
登录 → 选择区域/数据集 → 获取字段和操作符 → 构建Alpha → 批量回测 → 分析报告
  1          2                  3,4               5           6           7
```

| 步骤 | 菜单选项 | 模块 | 说明 |
|------|---------|------|------|
| 登录 | 1 | `SessionManager` | 自动读取凭证, 超时自动重连 |
| 数据集 | 2 | `DataManager` | 获取区域数据集列表, 智能缓存 |
| 字段 | 3 | `DataManager` | 获取数据集字段, 智能缓存 |
| 操作符 | 4 | `DataManager` | 获取平台操作符, 智能缓存 |
| Alpha构建 | 5 | `AlphaBuilder` | 手动输入/模板生成, 表达式验证 |
| 批量回测 | 6 | `BacktestRunner` | 多重模拟加速, 自动归档 |
| 查看报告 | 7 | `BacktestRunner` | 解析报告, 筛选高价值Alpha |

## 🤖 AI 增强功能

### 数据分析
```python
from ai.data_analysis import analyze_metadata

# 分析数据集字段特征
analysis = analyze_metadata(
    dataset_metadata=datasets,
    field_metadata=fields,
    region="USA",
    ai_researcher=researcher,
)
```

### Alpha 生成
```python
from ai.alpha_generator import generate_alphas_from_expressions

# 基于模板生成 Alpha
alphas = generate_alphas_from_expressions(
    template_expressions=["rank(ts_delta({field}, {window}))"],
    recommended_fields=[{"field_id": "close"}],
)
```

## 🔧 高级功能

### 针对性模板系统

针对特定数据集定制的模板和研究方向引导：

```
config/dataset_templates/
├── analyst4_templates.json   # analyst4 针对性模板
├── analyst4_guidance.json    # analyst4 研究方向引导
└── ...
```

**扩展新数据集**：只需在 `config/dataset_templates/` 下创建：
- `{dataset_id}_templates.json` - 针对性模板
- `{dataset_id}_guidance.json` - 研究方向引导

即可通过 `--template-mode specialized` 自动加载。

详见 [docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md) 的「模板系统」章节。

## ⚠️ 重要注意事项

- **区域配置**: 必须使用正确的 universe (USA→TOP3000, IND→TOP500, CHN→TOP2000U)
- **字段类型**: 向量/事件字段需先用 `vec_avg()` 聚合再使用 `rank()/ts_*` 操作符
- **缓存管理**: 缓存位于 `cache/` 目录, 使用 `--refresh` 参数强制刷新

## 📚 文档

- **[docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md)** - 完整项目指南 ⭐
- [docs/specialized_template_design.md](docs/specialized_template_design.md) - 针对性模板系统设计

## 🧪 测试

```bash
# 运行工作流测试
python test_workflow.py

# 运行特定步骤测试
python test_workflow.py --step 6
```

## 📊 性能指标

| 功能 | 速度 | 说明 |
|------|------|------|
| 数据集获取 | <1s | 使用缓存 |
| 字段获取 | <1s | 使用缓存 |
| 单次回测 | 10-30s | 取决于表达式复杂度 |
| 批量回测 | 并发执行 | 最多3个并发 |
| Pipeline 全流程 | 3-5分钟 | 取决于 Alpha 数量 |

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可

MIT License

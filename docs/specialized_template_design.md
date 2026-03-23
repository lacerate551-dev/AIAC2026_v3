# 针对性模板系统设计文档

## 1. 目标

将针对性模板功能独立出来，支持：
- **通用模板**：现有 77 个模板，适用于所有数据集
- **针对性模板**：按数据集定制的模板和研究方向引导
- **命令行切换**：通过参数选择使用哪种模板

## 2. 目录结构设计

```
config/
├── templates.json                    # 通用模板（保留不动）
└── dataset_templates/                # 新建目录
    ├── analyst4_templates.json       # analyst4 针对性模板
    ├── analyst4_guidance.json        # analyst4 研究方向引导
    ├── analyst10_templates.json      # 未来扩展
    ├── analyst10_guidance.json       # 未来扩展（将现有内置 prompt 移出）
    └── ...
```

## 3. 命令行接口设计

```bash
# 通用模板（默认，保持向后兼容）
python main.py pipeline --region USA --datasets analyst4

# 针对性模板（自动查找 config/dataset_templates/{dataset}_templates.json）
python main.py pipeline --region USA --datasets analyst4 --template-mode specialized

# 指定模板文件路径
python main.py pipeline --region USA --datasets analyst4 --templates config/dataset_templates/analyst4_templates.json
```

## 4. 文件变更清单

### 4.1 新建文件

| 文件 | 说明 | 状态 |
|------|------|------|
| `config/dataset_templates/analyst4_templates.json` | analyst4 针对性模板 | ✅ |
| `config/dataset_templates/analyst4_guidance.json` | analyst4 研究方向引导 | ✅ |
| `ai/template_loader.py` | 模板加载器模块 | ✅ |

### 4.2 修改文件

| 文件 | 修改内容 | 状态 |
|------|------|------|
| `ai/data_analysis.py` | 支持从配置加载研究方向引导 | ✅ |
| `ai/alpha_generator.py` | 支持加载针对性模板 | ✅ |
| `ai/alpha_factory_pipeline.py` | 支持 template_mode 参数 | ✅ |
| `main.py` | CLI 增加 --template-mode 和 --templates 参数 | ✅ |

## 5. 实现步骤

### 步骤 1：创建目录和配置文件
- [x] 创建 `config/dataset_templates/` 目录
- [x] 创建 `analyst4_templates.json`（针对性模板）
- [x] 创建 `analyst4_guidance.json`（研究方向引导）
- [x] 测试：文件存在且格式正确

### 步骤 2：实现模板加载器
- [x] 创建 `ai/template_loader.py`
- [x] 实现 `load_templates()` 函数，支持：
  - 默认模板（通用）
  - 针对性模板（按数据集）
  - 自定义路径模板
- [x] 实现 `load_guidance()` 函数，按数据集加载研究方向引导
- [x] 测试：加载各类型模板成功

### 步骤 3：修改 data_analysis.py
- [x] 导入 template_loader
- [x] 修改 `analyze_metadata()` 函数，从配置加载研究方向引导
- [x] 移除硬编码的 analyst10 引导 prompt（后续可迁移到配置文件）
- [x] 测试：AI 分析时正确使用研究方向引导

### 步骤 4：修改 alpha_generator.py
- [x] 导入 template_loader
- [x] 修改相关函数，支持从指定路径加载模板
- [x] 测试：生成 Alpha 时使用正确的模板

### 步骤 5：修改 alpha_factory_pipeline.py
- [x] 增加 template_mode 参数
- [x] 增加 templates_path 参数
- [x] 在 step_template_schedule 中调用 template_loader
- [x] 测试：pipeline 运行时使用正确的模板

### 步骤 6：修改 main.py CLI
- [x] 增加 --template-mode 参数（default/specialized）
- [x] 增加 --templates 参数（自定义路径）
- [x] 传递参数到 pipeline
- [x] 测试：命令行参数正确传递

### 步骤 7：集成测试
- [x] 测试通用模板模式
- [x] 测试针对性模板模式
- [x] 测试自定义路径模式

## 6. analyst4 针对性模板设计

### 6.1 研究方向引导

```json
{
  "dataset_id": "analyst4",
  "description": "Analyst Estimate Data for Equity - 分析师预期数据",
  "research_directions": [
    {
      "name": "预期修正信号",
      "description": "分析师预测的变化趋势，捕捉预期修正方向",
      "field_patterns": ["*_flag", "est_*", "anl4_*_number"],
      "suggested_operators": ["ts_delta", "ts_mean", "rank"]
    },
    {
      "name": "预测分歧度",
      "description": "分析师预测的分歧程度，high-low 差异",
      "field_patterns": ["*_high", "*_low", "*_mean", "*_median"],
      "suggested_combinations": ["(high - low) / mean", "high / low"]
    },
    {
      "name": "预期意外",
      "description": "实际值与预期值的差异",
      "field_patterns": ["actual_*", "est_*", "*_surprise*"],
      "suggested_combinations": ["(actual - est) / est", "actual / est"]
    }
  ],
  "priority_fields": [
    "anl4_adjusted_netincome_ft",
    "anl4_ebit_value",
    "anl4_ebitda_value",
    "anl4_capex_high",
    "anl4_capex_low",
    "anl4_netprofit_flag",
    "anl4_ptp_flag"
  ]
}
```

### 6.2 针对性模板

```json
[
  {
    "name": "analyst_dispersion",
    "expression": "rank(({field1} - {field2}) / ({field3} + 0.001))",
    "description": "分析师预测分歧度：(high - low) / mean",
    "fields_required": 3,
    "field_types": [["vector"], ["vector"], ["vector"]],
    "field_hints": {"field1": "*_high", "field2": "*_low", "field3": "*_value"},
    "operators": ["rank"],
    "category": "pair"
  },
  {
    "name": "estimate_revision_momentum",
    "expression": "rank(ts_delta({field}, {window}))",
    "description": "预期修正动量：分析师预测变化",
    "fields_required": 1,
    "field_types": [["vector"]],
    "field_hints": {"field": "*_flag"},
    "operators": ["rank", "ts_delta"],
    "category": "time_series"
  },
  {
    "name": "high_low_ratio",
    "expression": "rank({field1} / ({field2} + 0.001))",
    "description": "最高/最低预测比率",
    "fields_required": 2,
    "field_types": [["vector"], ["vector"]],
    "field_hints": {"field1": "*_high", "field2": "*_low"},
    "operators": ["rank"],
    "category": "pair"
  },
  {
    "name": "actual_est_surprise",
    "expression": "rank(({field1} - {field2}) / (abs({field2}) + 0.001))",
    "description": "预期意外：(actual - est) / est",
    "fields_required": 2,
    "field_types": [["vector"], ["vector"]],
    "field_hints": {"field1": "actual_*", "field2": "est_*"},
    "operators": ["rank", "abs"],
    "category": "pair"
  },
  {
    "name": "estimate_zscore",
    "expression": "rank(ts_zscore({field}, {window}))",
    "description": "预期值的历史 Z-score",
    "fields_required": 1,
    "field_types": [["vector"]],
    "operators": ["rank", "ts_zscore"],
    "category": "time_series"
  },
  {
    "name": "revision_acceleration",
    "expression": "rank(ts_delta(ts_delta({field}, 1), {window}))",
    "description": "预期修正加速度：修正的变化率",
    "fields_required": 1,
    "field_types": [["vector"]],
    "field_hints": {"field": "*_flag"},
    "operators": ["rank", "ts_delta"],
    "category": "complex"
  }
]
```

## 7. 向后兼容性

- 默认行为不变（使用通用模板）
- 现有代码路径保持兼容
- 新参数均为可选
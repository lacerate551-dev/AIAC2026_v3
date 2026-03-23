# 字段频率自动检测系统

## 概述

基于 WorldQuant BRAIN 回测的字段更新频率自动检测系统。通过构建特定的 alpha 表达式并分析回测结果，精确判断数据字段的实际更新频率（日频、周频、月频、季频等）。

## 核心原理

### 使用的操作符

1. **`days_from_last_change(x)`** - 最直接的频率指标
   - 计算字段值上次变化以来的天数
   - 高 turnover → 频繁变化 → 高频数据

2. **`ts_count_nans(x, d)`** - 缺失值模式分析
   - 统计过去 d 天内的 NaN 值数量
   - 缺失值模式反映更新频率

3. **`ts_delta(x, d)`** - 变化检测
   - 计算当前值与 d 天前的差异
   - 不同时间窗口的变化模式揭示更新周期

### 判断逻辑

通过回测指标判断频率:

| 指标 | 日频 | 周频 | 月频 | 季频 |
|------|------|------|------|------|
| Turnover | >0.5 | 0.2-0.5 | 0.05-0.2 | <0.05 |
| Coverage | >0.8 | 0.6-0.8 | 0.3-0.6 | <0.3 |
| 20天缺失值 | <30% | 50-70% | >80% | >90% |

## 三种检测方法

### 1. 规则推断 (rule)
- **速度**: 毫秒级
- **准确性**: 中等（依赖关键词匹配）
- **适用**: 大批量初筛

### 2. 回测检测 (backtest)
- **速度**: 30-60秒/字段
- **准确性**: 高（基于实际数据）
- **适用**: 关键字段验证

### 3. 混合模式 (hybrid) ⭐ 推荐
- **速度**: 1-30秒/字段
- **准确性**: 高
- **逻辑**: 先规则推断，置信度低时自动回测

## 快速开始

### 命令行使用

```bash
# 最常用：混合模式检测并更新缓存
python tools/detect_field_frequency.py --region USA --dataset pv1 --method hybrid --update-cache

# 检测所有字段
python tools/detect_field_frequency.py --region USA --dataset pv1 --method hybrid

# 检测特定字段
python tools/detect_field_frequency.py --region USA --dataset pv1 --fields close open --method backtest

# 快速规则推断
python tools/detect_field_frequency.py --region USA --dataset analyst15 --method rule
```

### Python API

```python
from core.session_manager import SessionManager
from ai.frequency_inference import infer_field_frequency_hybrid

# 初始化
session_manager = SessionManager()
session = session_manager.get_session()

# 检测单个字段
result = infer_field_frequency_hybrid(
    field_name="close",
    field_description="Closing price",
    session=session,
    region="USA",
    dataset_id="pv1",
    prefer_backtest=False  # 混合模式
)

print(f"频率: {result['frequency']}")
print(f"置信度: {result['confidence']:.2f}")
print(f"方法: {result['method']}")

session_manager.close()
```

### 批量检测

```python
from ai.frequency_inference import batch_infer_field_frequencies

fields = [
    {"field_name": "close", "description": "Closing price"},
    {"field_name": "volume", "description": "Trading volume"},
]

results = batch_infer_field_frequencies(
    fields=fields,
    session=session,
    region="USA",
    dataset_id="pv1",
    prefer_backtest=False
)

for field_name, result in results.items():
    print(f"{field_name}: {result['frequency']} ({result['confidence']:.2f})")
```

## 输出结果

### 结果格式

```json
{
  "field_name": "close",
  "frequency": "daily",
  "confidence": 0.9,
  "method": "backtest",
  "reasoning": [
    "变化间隔 turnover 高 (0.8234), 表明日频更新",
    "20天缺失值少 (0.05%), 验证日频判断"
  ],
  "metrics": {
    "avg_coverage": 0.95,
    "test_results": [...]
  }
}
```

### 频率类型

- `daily` - 日频（每个交易日更新）
- `weekly` - 周频（每周更新）
- `monthly` - 月频（每月更新）
- `quarterly` - 季频（每季度更新）
- `semi-annual` - 半年频
- `annual` - 年频
- `irregular` - 不规则（事件驱动）
- `unknown` - 无法判断

## 集成到 metadata

检测结果可以自动更新到字段缓存:

```bash
python tools/detect_field_frequency.py \
  --region USA \
  --dataset pv1 \
  --method hybrid \
  --update-cache
```

更新后，`cache/dataset_fields/USA_pv1_fields.json` 中的字段会包含:

```json
{
  "field_id": "close",
  "field_name": "close",
  "description": "Closing price",
  "coverage": 0.95,
  "type": "MATRIX",
  "frequency": "daily",
  "frequency_confidence": 0.9,
  "frequency_method": "backtest"
}
```

## 性能考虑

### 速度对比

| 方法 | 单字段耗时 | 100字段耗时 | 适用场景 |
|------|-----------|------------|---------|
| rule | <1ms | <1s | 大批量初筛 |
| backtest | 30-60s | 50-100分钟 | 关键字段验证 |
| hybrid | 1-30s | 5-30分钟 | 日常使用 |

### 优化建议

1. **首次检测**: 使用 `hybrid` 模式
2. **大批量**: 先 `rule` 全量，再对低置信度字段补充 `backtest`
3. **关键字段**: 直接使用 `backtest` 确保准确性
4. **定期更新**: 使用 `--update-cache` 保持 metadata 最新

## 使用场景

### 场景 1: 新数据集初始化

```bash
# 1. 快速检测所有字段
python tools/detect_field_frequency.py --region USA --dataset new_dataset --method rule

# 2. 对低置信度字段补充回测
python tools/detect_field_frequency.py \
  --region USA \
  --dataset new_dataset \
  --fields field1 field2 field3 \
  --method backtest

# 3. 更新缓存
python tools/detect_field_frequency.py \
  --region USA \
  --dataset new_dataset \
  --method hybrid \
  --update-cache
```

### 场景 2: 验证现有频率标注

```bash
# 使用回测验证已标注的频率是否准确
python tools/detect_field_frequency.py \
  --region USA \
  --dataset pv1 \
  --method backtest \
  --fields close open high low
```

### 场景 3: 批量处理多个数据集

```bash
# 创建批处理脚本
for dataset in pv1 analyst15 fundamental17; do
  python tools/detect_field_frequency.py \
    --region USA \
    --dataset $dataset \
    --method hybrid \
    --update-cache
done
```

## 故障排查

### 问题 1: 回测检测失败

**症状**: 所有回测都返回错误

**解决方法**:
```bash
# 1. 先用规则推断验证字段存在
python tools/detect_field_frequency.py --region USA --dataset pv1 --method rule

# 2. 检查字段列表
python main.py fields --region USA --dataset pv1

# 3. 查看详细日志
python tools/detect_field_frequency.py --region USA --dataset pv1 --method backtest 2>&1 | tee debug.log
```

### 问题 2: 所有字段都是 "unknown"

**可能原因**:
- 字段数据质量差
- 缺失值过多
- 回测周期不够长

**解决方法**:
- 检查字段的 coverage
- 使用规则推断作为后备
- 增加回测天数（修改 `backtest_days` 参数）

### 问题 3: 检测速度太慢

**解决方法**:
```bash
# 1. 改用混合模式
python tools/detect_field_frequency.py --region USA --dataset pv1 --method hybrid

# 2. 只检测关键字段
python tools/detect_field_frequency.py \
  --region USA \
  --dataset pv1 \
  --fields field1 field2 field3 \
  --method backtest

# 3. 分批次检测
for dataset in pv1 analyst15 fundamental17; do
  python tools/detect_field_frequency.py --region USA --dataset $dataset --method rule &
done
wait
```

## 文件结构

```
AIAC2025_v3/
├── core/
│   └── frequency_detector.py          # 核心检测器类
├── ai/
│   └── frequency_inference.py         # 推断逻辑
├── tools/
│   └── detect_field_frequency.py      # 命令行工具
├── tests/
│   └── test_frequency_detection.py    # 功能测试
└── docs/
    ├── frequency_detection.md         # 本文档
    └── QUICK_REFERENCE.md             # 快速参考
```

## 缓存机制

### 回测结果缓存

检测结果自动缓存到 `cache/frequency_detection/`:

```
cache/frequency_detection/
├── USA_pv1_frequencies_20260315_143022.json
├── USA_analyst15_frequencies_20260315_144530.json
└── detection_results_20260315_150000.json
```

### 使用缓存

```python
# 自动使用缓存（如果存在）
result = infer_field_frequency_by_backtest(
    session, "close", region="USA", use_cache=True
)
```

## 扩展开发

### 添加新的检测逻辑

编辑 `core/frequency_detector.py`:

```python
def build_frequency_test_alphas(self, field_name: str, dataset_id: str = None):
    # 添加新的测试 alpha
    test_alphas.append({
        "name": f"{prefix}{field_name}_custom_test",
        "expression": f"your_custom_operator({field_name})",
        "description": "自定义频率检测逻辑"
    })
    return test_alphas
```

### 自定义判断规则

编辑 `core/frequency_detector.py` 中的 `_analyze_frequency_results`:

```python
def _analyze_frequency_results(self, field_name: str, results: List[Dict]):
    # 添加自定义判断逻辑
    if custom_condition:
        frequency = "custom_frequency"
        confidence = 0.9
        reasoning.append("自定义判断逻辑")

    return {...}
```

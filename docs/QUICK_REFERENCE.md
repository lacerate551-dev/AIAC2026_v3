# 字段频率检测 - 快速参考

## 一行命令

```bash
# 最常用：混合模式检测并更新缓存
python tools/detect_field_frequency.py --region USA --dataset pv1 --method hybrid --update-cache
```

## 三种方法对比

| 方法 | 速度 | 准确性 | 命令 |
|------|------|--------|------|
| rule | ⚡⚡⚡ | ⭐⭐ | `--method rule` |
| backtest | ⚡ | ⭐⭐⭐ | `--method backtest` |
| hybrid | ⚡⚡ | ⭐⭐⭐ | `--method hybrid` ⭐推荐 |

## 常用命令

```bash
# 检测所有字段
python tools/detect_field_frequency.py --region USA --dataset pv1 --method hybrid

# 检测特定字段
python tools/detect_field_frequency.py --region USA --dataset pv1 --fields close open --method backtest

# 更新缓存
python tools/detect_field_frequency.py --region USA --dataset pv1 --method hybrid --update-cache

# 运行测试
python tests/test_frequency_detection.py
```

## Python API

```python
from core.session_manager import SessionManager
from ai.frequency_inference import infer_field_frequency_hybrid

session_manager = SessionManager()
session = session_manager.get_session()

result = infer_field_frequency_hybrid(
    field_name="close",
    field_description="Closing price",
    session=session,
    region="USA",
    prefer_backtest=False  # 混合模式
)

print(f"{result['frequency']} (置信度: {result['confidence']:.2f})")
session_manager.close()
```

## 频率类型

- `daily` - 日频
- `weekly` - 周频
- `monthly` - 月频
- `quarterly` - 季频
- `annual` - 年频
- `irregular` - 不规则
- `unknown` - 未知

## 判断依据

| Turnover | 频率 |
|----------|------|
| >0.5 | daily |
| 0.2-0.5 | weekly |
| 0.05-0.2 | monthly |
| <0.05 | quarterly |

## 核心操作符

1. `days_from_last_change(field)` - 变化间隔
2. `ts_count_nans(field, d)` - 缺失值模式
3. `ts_delta(field, d)` - 变化幅度

## 结果位置

- 检测结果: `cache/frequency_detection/`
- 字段缓存: `cache/dataset_fields/`

## 故障排查

| 问题 | 解决方法 |
|------|---------|
| 回测失败 | 先用 `--method rule` 验证 |
| 全是 unknown | 检查字段 coverage |
| 速度太慢 | 改用 `--method hybrid` |

## 文档

- 详细文档: `docs/field_frequency_detection.md`
- 实现总结: `docs/IMPLEMENTATION_SUMMARY.md`
- README: `docs/README_frequency_detection.md`

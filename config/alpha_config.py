# -*- coding: utf-8 -*-
"""
Alpha 与模板相关配置
- coverage 短阈值（用于 AI 分析与中和推荐）
- 模板参数枚举（window / decay / truncation / neutralization）
"""

# ==================== Coverage 阈值 ====================
# coverage < COVERAGE_THRESHOLD 视为 coverage 短，可推荐中和数据集弥补
COVERAGE_THRESHOLD = 0.6

# ==================== 自动 Dataset 扩展（Pipeline） ====================
# 当 AI 输出 neutralization_datasets 且 coverage 不足时，是否自动扩展数据集并重新跑 metadata + AI 分析
AUTO_DATASET_EXPANSION = True
# 最多自动扩展轮数，防止无限循环
MAX_EXPANSION_ROUNDS = 2

# ==================== 动态字段数配置 ====================
# 根据数据集字段数量动态调整最大推荐字段数，确保研究时间与字段丰富程度正相关
# 目标：每天 3 小时回测时间（约 900 个 Alpha）
# 字段数阈值和对应的最大推荐字段数
FIELD_COUNT_THRESHOLDS = [
    (100, 12),   # > 100 字段 → 最多 12 个推荐字段
    (50, 10),    # 50-100 字段 → 最多 10 个推荐字段
    (20, 8),     # 20-50 字段 → 最多 8 个推荐字段
    (0, 6),      # < 20 字段 → 最多 6 个推荐字段
]

# 默认最大推荐字段数（当无法获取字段数时使用）
DEFAULT_MAX_RECOMMENDED_FIELDS = 10

# ==================== 模板调度（动态） ====================
# 每轮模板数也根据字段数动态调整
TEMPLATE_COUNT_THRESHOLDS = [
    (100, 35),   # > 100 字段 → 35 个模板
    (50, 30),    # 50-100 字段 → 30 个模板
    (20, 25),    # 20-50 字段 → 25 个模板
    (0, 20),     # < 20 字段 → 20 个模板
]

# 每轮各类别目标数量（随机采样 + 覆盖不同类别）
TEMPLATE_SCHEDULE_DISTRIBUTION = {
    "time_series": 10,
    "cross_section": 6,
    "pair": 8,
    "complex": 6,
}

# 去重后每个结构保留的参数组合数
MAX_PER_STRUCTURE = 3

# 双字段模板最多使用的 (field1, field2) 组合数
MAX_TWO_FIELD_PAIRS = 30

# ==================== 模板参数（配置驱动） ====================
# Alpha 生成：signal × template × 以下参数组合
# 参数组合：3×2×2×3 = 36 种（符合 3 小时限制）
TEMPLATE_PARAMS = {
    "window": [5, 10, 20],              # 3 种时间窗口
    "decay": [3, 5],                    # 2 种衰减系数
    "truncation": [0.01, 0.05],         # 2 种截断比例
    "neutralization": ["INDUSTRY", "SECTOR", "MARKET"],  # 3 种中性化
}


def get_dynamic_limits(field_count: int) -> dict:
    """
    根据数据集字段数量动态计算推荐字段上限和模板数。

    Args:
        field_count: 数据集的字段数量

    Returns:
        {"max_fields": int, "templates_per_round": int}
    """
    max_fields = DEFAULT_MAX_RECOMMENDED_FIELDS
    templates_per_round = 25

    for threshold, limit in FIELD_COUNT_THRESHOLDS:
        if field_count > threshold:
            max_fields = limit
            break

    for threshold, count in TEMPLATE_COUNT_THRESHOLDS:
        if field_count > threshold:
            templates_per_round = count
            break

    return {
        "max_fields": max_fields,
        "templates_per_round": templates_per_round,
        "field_count": field_count,
    }


# ==================== 推荐字段数量上限 ====================
# AI 推荐字段按 priority 排序后只保留前 N 个，避免 Alpha 生成规模过大
# 注意：此值为默认上限，实际使用时会根据数据集字段数动态调整
MAX_RECOMMENDED_FIELDS = DEFAULT_MAX_RECOMMENDED_FIELDS

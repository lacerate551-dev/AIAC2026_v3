# -*- coding: utf-8 -*-
"""
模板调度系统
每轮从 100 个模板中动态选择 20~30 个，随机采样 + 覆盖 time_series / cross_section / pair / complex，保证 Alpha 结构多样性。
"""
import random
import logging
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# 时间序列操作符（用于判定 time_series / complex）
TS_OPS = (
    "ts_mean", "ts_delta", "ts_std_dev", "ts_stddev", "ts_rank", "ts_delay",
    "ts_decay_linear", "decay_linear", "ts_sum", "ts_arg_max", "ts_arg_min",
    "ts_argmax", "ts_argmin", "ts_corr", "corr", "ts_covariance", "cov",
)


def get_template_category(expression: str) -> str:
    """
    按表达式内容将模板分为四类：time_series / cross_section / pair / complex。

    - pair: 含 {field1} 与 {field2}
    - complex: 单字段但有多层 ts_* 嵌套（如 rank(ts_mean(ts_delta(...)))）
    - time_series: 单字段且含 ts_*，但嵌套层数较浅
    - cross_section: 其余（如 rank({field}), zscore({field})）
    """
    if not expression or not expression.strip():
        return "cross_section"
    expr = expression.strip()
    has_field1 = "{field1}" in expr
    has_field2 = "{field2}" in expr
    if has_field1 and has_field2:
        return "pair"

    # 统计 ts_* 出现次数（含别名）
    ts_count = 0
    for op in TS_OPS:
        if op in expr:
            ts_count += expr.count(op)
    # 多层嵌套：同一表达式里出现至少 2 处 ts_* 调用（如 ts_mean(...) 与 ts_delta(...)）
    op_names_found = [op for op in TS_OPS if op in expr]
    if len(op_names_found) >= 2 or (ts_count >= 2 and "ts_" in expr):
        return "complex"
    if ts_count >= 1 or op_names_found:
        return "time_series"
    return "cross_section"


def group_templates_by_category(
    templates: List[str],
) -> Dict[str, List[str]]:
    """将模板列表按类别分组。"""
    groups: Dict[str, List[str]] = {
        "time_series": [],
        "cross_section": [],
        "pair": [],
        "complex": [],
    }
    for t in templates:
        cat = get_template_category(t)
        if cat in groups:
            groups[cat].append(t)
        else:
            groups["cross_section"].append(t)
    return groups


def schedule_templates(
    templates: List[str],
    templates_per_round: Optional[int] = None,
    distribution: Optional[Dict[str, int]] = None,
    rng: Optional[random.Random] = None,
) -> Tuple[List[str], Dict[str, int]]:
    """
    每轮从全部模板中按类别分布随机抽取模板，保证结构多样性。

    Args:
        templates: 完整模板表达式列表（如 AlphaBuilder.TEMPLATES）
        templates_per_round: 每轮目标数量，默认从 config 读取（20~30 可在此或 config 中设置）
        distribution: 各类别目标数量，如 {"time_series": 10, "cross_section": 5, "pair": 5, "complex": 5}；缺省从 config 读取
        rng: 随机数生成器，缺省用 random 全局

    Returns:
        (selected_templates, actual_distribution)
        actual_distribution: 本轮实际各类别选中数量
    """
    if rng is None:
        rng = random
    try:
        from config.alpha_config import templates_per_round as cfg_per_round
        from config.alpha_config import TEMPLATE_SCHEDULE_DISTRIBUTION as cfg_dist
    except Exception:
        cfg_per_round = 25
        cfg_dist = {"time_series": 10, "cross_section": 5, "pair": 5, "complex": 5}

    raw_per_round = templates_per_round if templates_per_round is not None else cfg_per_round
    if isinstance(raw_per_round, (list, tuple)) and len(raw_per_round) >= 2:
        per_round = rng.randint(int(raw_per_round[0]), int(raw_per_round[1]))
    else:
        per_round = int(raw_per_round)
    dist = distribution if distribution is not None else dict(cfg_dist)
    dist_sum = sum(dist.values())
    if dist_sum != per_round and dist_sum > 0:
        scale = per_round / dist_sum
        dist = {k: max(0, int(round(v * scale))) for k, v in dist.items()}
        if sum(dist.values()) != per_round:
            k_max = max(dist, key=dist.get)
            dist[k_max] = dist[k_max] + (per_round - sum(dist.values()))

    groups = group_templates_by_category(templates)
    selected: List[str] = []
    actual: Dict[str, int] = {}

    for category in ("time_series", "cross_section", "pair", "complex"):
        pool = groups.get(category, [])
        n_want = min(dist.get(category, 0), len(pool))
        if n_want <= 0 or not pool:
            actual[category] = 0
            continue
        if n_want >= len(pool):
            chosen = list(pool)
        else:
            chosen = rng.sample(pool, n_want)
        selected.extend(chosen)
        actual[category] = len(chosen)

    # 若因某类不足导致总数 < per_round，从剩余模板中随机补足
    shortfall = per_round - len(selected)
    if shortfall > 0:
        remaining = [t for t in templates if t not in selected]
        if remaining:
            n_extra = min(shortfall, len(remaining))
            extra = rng.sample(remaining, n_extra)
            selected.extend(extra)
            for t in extra:
                cat = get_template_category(t)
                actual[cat] = actual.get(cat, 0) + 1

    rng.shuffle(selected)
    logger.info(
        "模板调度: 本轮 %s 个 (time_series=%s, cross_section=%s, pair=%s, complex=%s)",
        len(selected),
        actual.get("time_series", 0),
        actual.get("cross_section", 0),
        actual.get("pair", 0),
        actual.get("complex", 0),
    )
    return selected, actual


def get_scheduled_templates(
    templates_per_round: Optional[int] = None,
    distribution: Optional[Dict[str, int]] = None,
) -> List[str]:
    """
    从 AlphaBuilder.TEMPLATES 中按配置调度本轮模板，便于直接接入生成流程。

    Returns:
        本轮选中的模板表达式列表
    """
    from core.alpha_builder import AlphaBuilder
    all_templates = getattr(AlphaBuilder, "TEMPLATES", None)
    if not all_templates:
        return []
    selected, _ = schedule_templates(
        all_templates,
        templates_per_round=templates_per_round,
        distribution=distribution,
    )
    return selected


def schedule_templates_with_metadata(
    templates: List[Dict],
    templates_per_round: Optional[int] = None,
    distribution: Optional[Dict[str, int]] = None,
    rng: Optional[random.Random] = None,
) -> Tuple[List[Dict], Dict[str, int]]:
    """
    调度模板（保留完整元数据，包括 field_hints）。

    与 schedule_templates 类似，但返回完整的模板对象而非仅表达式。
    这样可以保留 field_hints 等元数据，用于智能字段匹配。

    Args:
        templates: 完整模板对象列表，每个元素包含 expression, field_hints 等
        templates_per_round: 每轮目标数量
        distribution: 各类别目标数量
        rng: 随机数生成器

    Returns:
        (selected_templates, actual_distribution)
        - selected_templates: 选中的模板对象列表（保留 field_hints）
        - actual_distribution: 本轮实际各类别选中数量
    """
    if rng is None:
        rng = random
    try:
        from config.alpha_config import templates_per_round as cfg_per_round
        from config.alpha_config import TEMPLATE_SCHEDULE_DISTRIBUTION as cfg_dist
    except Exception:
        cfg_per_round = 25
        cfg_dist = {"time_series": 10, "cross_section": 5, "pair": 5, "complex": 5}

    raw_per_round = templates_per_round if templates_per_round is not None else cfg_per_round
    if isinstance(raw_per_round, (list, tuple)) and len(raw_per_round) >= 2:
        per_round = rng.randint(int(raw_per_round[0]), int(raw_per_round[1]))
    else:
        per_round = int(raw_per_round)
    dist = distribution if distribution is not None else dict(cfg_dist)
    dist_sum = sum(dist.values())
    if dist_sum != per_round and dist_sum > 0:
        scale = per_round / dist_sum
        dist = {k: max(0, int(round(v * scale))) for k, v in dist.items()}
        if sum(dist.values()) != per_round:
            k_max = max(dist, key=dist.get)
            dist[k_max] = dist[k_max] + (per_round - sum(dist.values()))

    # 按类别分组模板对象
    groups: Dict[str, List[Dict]] = {
        "time_series": [],
        "cross_section": [],
        "pair": [],
        "complex": [],
    }
    for t in templates:
        expr = t.get("expression", "")
        cat = get_template_category(expr)
        if cat in groups:
            groups[cat].append(t)
        else:
            groups["cross_section"].append(t)

    selected: List[Dict] = []
    actual: Dict[str, int] = {}

    for category in ("time_series", "cross_section", "pair", "complex"):
        pool = groups.get(category, [])
        n_want = min(dist.get(category, 0), len(pool))
        if n_want <= 0 or not pool:
            actual[category] = 0
            continue
        if n_want >= len(pool):
            chosen = list(pool)
        else:
            chosen = rng.sample(pool, n_want)
        selected.extend(chosen)
        actual[category] = len(chosen)

    # 若因某类不足导致总数 < per_round，从剩余模板中随机补足
    selected_exprs = {t.get("expression") for t in selected}
    shortfall = per_round - len(selected)
    if shortfall > 0:
        remaining = [t for t in templates if t.get("expression") not in selected_exprs]
        if remaining:
            n_extra = min(shortfall, len(remaining))
            extra = rng.sample(remaining, n_extra)
            selected.extend(extra)
            for t in extra:
                cat = get_template_category(t.get("expression", ""))
                actual[cat] = actual.get(cat, 0) + 1

    rng.shuffle(selected)
    logger.info(
        "模板调度(带元数据): 本轮 %s 个 (time_series=%s, cross_section=%s, pair=%s, complex=%s)",
        len(selected),
        actual.get("time_series", 0),
        actual.get("cross_section", 0),
        actual.get("pair", 0),
        actual.get("complex", 0),
    )
    return selected, actual

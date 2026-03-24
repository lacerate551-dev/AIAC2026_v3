# -*- coding: utf-8 -*-
"""
Alpha 去重模块
在批量生成后按「表达式结构」标准化去重，每种结构只保留 1~3 个参数组合，减少高度相关的 Alpha 浪费回测资源。
"""
import re
import logging
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

# 不作为「字段」替换的保留字（如 group_neutralize 中的 industry）
RESERVED_WORDS = {"industry", "subindustry", "sector"}


def _get_operator_set(expression: str) -> set:
    """从表达式中解析出所有「名称(」形式的操作符名（小写）。"""
    return set(re.findall(r"([a-z_][a-z0-9_]*)\s*\(", expression))


def normalize_expression_structure(expression: str, keep_fields: bool = True) -> str:
    """
    将表达式标准化为结构形式。

    Args:
        expression: 原始表达式
        keep_fields: 是否保留字段名。True则不同字段算不同结构，False则忽略字段差异

    例如：
        keep_fields=True:
            rank(ts_mean(nws17_d1_ssc,5))   -> rank(ts_mean(nws17_d1_ssc,{window}))
            rank(ts_mean(nws17_d1_qmb,10))  -> rank(ts_mean(nws17_d1_qmb,{window}))
            两者结构不同（字段不同）

        keep_fields=False:
            rank(ts_mean(nws17_d1_ssc,5))   -> rank(ts_mean({field},{window}))
            rank(ts_mean(nws17_d1_qmb,10))  -> rank(ts_mean({field},{window}))
            两者结构相同
    """
    if not expression or not expression.strip():
        return ""
    expr = expression.strip()
    operators = _get_operator_set(expr)

    if keep_fields:
        # 保留字段名，只替换数字为 {window}
        def replace_numbers(match: re.Match) -> str:
            s = match.group(0)
            if re.match(r"^\d+\.?\d*$|^\.\d+$", s):
                return "{window}"
            if s in operators or s in RESERVED_WORDS:
                return s
            return s
        pattern = r"[a-z_][a-z0-9_]*|\d+\.?\d*|\.\d+"
        return re.sub(pattern, replace_numbers, expr)
    else:
        # 原有逻辑：字段也替换为 {field}
        field_seen: Dict[str, str] = {}
        field_counter = [0]

        def replace_token(match: re.Match) -> str:
            s = match.group(0)
            if re.match(r"^\d+\.?\d*$|^\.\d+$", s):
                return "{window}"
            if s in operators or s in RESERVED_WORDS:
                return s
            if s not in field_seen:
                field_counter[0] += 1
                if field_counter[0] == 1:
                    field_seen[s] = "{field}"
                else:
                    field_seen[s] = f"{{field{field_counter[0]}}}"
            return field_seen[s]

        pattern = r"[a-z_][a-z0-9_]*|\d+\.?\d*|\.\d+"
        return re.sub(pattern, replace_token, expr)


def deduplicate(
    alpha_items: List[Dict[str, Any]],
    max_per_structure: int = 3,
    keep_fields_in_structure: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    按结构去重：每种结构只保留至多 max_per_structure 个参数组合。

    Args:
        alpha_items: 生成的 Alpha 列表，每项至少含 "expression"，可含 decay/truncation/neutralization 等
        max_per_structure: 每种结构保留的最大数量，默认 3
        keep_fields_in_structure: 是否将不同字段视为不同结构。True则保留更多字段多样性

    Returns:
        (deduped_list, stats)
        stats: {"generated_alpha": N, "after_dedup": M}
    """
    generated = len(alpha_items)
    if generated == 0:
        return [], {"generated_alpha": 0, "after_dedup": 0}

    # 按结构分组：structure -> [item, ...]
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for item in alpha_items:
        expr = item.get("expression") or ""
        structure = normalize_expression_structure(expr, keep_fields=keep_fields_in_structure)
        if structure not in groups:
            groups[structure] = []
        groups[structure].append(item)

    # 每种结构只保留前 max_per_structure 个
    deduped: List[Dict[str, Any]] = []
    for structure, items in groups.items():
        keep = items[:max_per_structure]
        deduped.extend(keep)

    after_dedup = len(deduped)
    stats = {
        "generated_alpha": generated,
        "after_dedup": after_dedup,
    }
    if after_dedup < generated:
        logger.info(
            "Alpha 去重: 生成 %s 个 -> 去重后 %s 个 (结构数 %s, 每种最多 %s 个)",
            generated,
            after_dedup,
            len(groups),
            max_per_structure,
        )
    return deduped, stats

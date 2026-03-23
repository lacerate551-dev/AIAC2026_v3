# -*- coding: utf-8 -*-
"""
Alpha 模板批量生成模块
- 基于 AI 输出的 recommended_fields，套用固定模板库生成表达式
- 模板按操作符过滤（依赖操作符缺失则整模板丢弃）
- 生成方式：signal × template × parameter combinations
- 生成后经 alpha_deduplicator 按结构去重，每种结构保留 1~3 个参数组合

类型约束（v3）：
- vector/matrix 类型字段：可直接用于数值计算（BRAIN 平台的 MATRIX 类型）
- event 类型字段：需要 vec_* 操作符转换后使用（BRAIN 平台的 VECTOR 类型）
- group 类型字段仅用于 group_neutralize 等分组操作
- symbol 类型字段不参与因子生成

v3 增强：
- 支持 template_mode 参数选择模板来源
- 支持 templates_path 自定义模板路径
- 支持 event 类型字段（使用针对性模板中的 vec_* 操作符）
"""
import itertools
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from config.settings import CONFIG_DIR
from config.alpha_config import TEMPLATE_PARAMS
from ai.template_loader import load_templates as load_templates_from_loader

logger = logging.getLogger(__name__)

# 最近一次去重统计（供 main/workflow 等输出）
_last_dedup_stats: Optional[Dict[str, int]] = None

TEMPLATES_PATH = CONFIG_DIR / "templates.json"

# 可用于数值因子计算的字段类型（包括 event 类型，需要 vec_* 操作符处理）
NUMERIC_FIELD_TYPES = {"vector", "matrix", "event"}
# 分组类型字段
GROUP_FIELD_TYPES = {"group"}


def filter_fields_by_type(
    recommended_fields: List[Dict[str, Any]],
    allowed_types: List[str] = None,
) -> List[Dict[str, Any]]:
    """
    按类型过滤推荐字段。

    Args:
        recommended_fields: 推荐字段列表
        allowed_types: 允许的类型列表，默认为 NUMERIC_FIELD_TYPES

    Returns:
        过滤后的字段列表
    """
    if allowed_types is None:
        allowed_types = list(NUMERIC_FIELD_TYPES)

    allowed_set = {t.lower() for t in allowed_types}
    filtered = []

    for f in recommended_fields:
        fid = (f.get("field_id") or f.get("field_name") or "").strip()
        if not fid:
            continue
        nt = str(f.get("normalized_type") or "").strip().lower()
        # 无类型信息时默认允许（由下游类型检查器处理）
        if not nt or nt in allowed_set:
            filtered.append(f)
        else:
            logger.debug(f"字段 {fid} 类型 {nt} 不在允许列表 {allowed_types} 中，已过滤")

    return filtered


def load_templates(
    path: Optional[Path] = None,
    template_mode: str = "default",
    dataset_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    从 JSON 加载模板列表。

    Args:
        path: 自定义模板文件路径（优先级最高）
        template_mode: 模板模式 ("default" 或 "specialized")
        dataset_id: 数据集 ID（用于 specialized 模式）

    Returns:
        模板列表
    """
    # 使用 template_loader 加载
    templates, source = load_templates_from_loader(
        template_mode=template_mode,
        dataset_id=dataset_id,
        templates_path=str(path) if path else None,
    )
    return templates


def get_operator_names(operators_df) -> set:
    """从 DataManager.get_operators() 返回的 DataFrame 中提取操作符名称集合。"""
    if operators_df is None or operators_df.empty:
        return set()
    col = "name" if "name" in operators_df.columns else "id"
    if col not in operators_df.columns:
        return set()
    return set(operators_df[col].astype(str).str.lower().unique())


def filter_templates_by_operators(
    templates: List[Dict[str, Any]],
    operator_names: set,
) -> List[Dict[str, Any]]:
    """只保留所有依赖操作符均存在的模板；否则整模板丢弃。"""
    op_set = {x.lower() for x in operator_names}
    out = []
    for t in templates:
        required = t.get("operators") or []
        if not required:
            out.append(t)
            continue
        if all(str(o).lower() in op_set for o in required):
            out.append(t)
        else:
            logger.debug(f"丢弃模板 {t.get('name')}：缺少操作符 {required}")
    return out


def _substitute_expression(expr: str, subs: Dict[str, str]) -> str:
    """替换表达式中的占位符 {field}, {field1}, {field2}, {window} 等。"""
    for k, v in subs.items():
        expr = expr.replace("{" + k + "}", str(v))
    return expr


def generate_alphas(
    recommended_fields: List[Dict[str, Any]],
    templates: List[Dict[str, Any]],
    template_params: Optional[Dict[str, List]] = None,
    max_two_field_pairs: int = 100,
) -> List[Dict[str, Any]]:
    """
    根据推荐字段与模板生成 Alpha 列表（表达式 + 回测参数）。

    Args:
        recommended_fields: [{"dataset_id", "field_id", "reason", "priority"}, ...]
        templates: 已按操作符过滤的模板列表，每项含 name, expression, fields_required, operators
        template_params: 参数枚举，默认 TEMPLATE_PARAMS（window, decay, truncation, neutralization）
        max_two_field_pairs: 双字段模板最多使用的 (field1, field2) 组合数，避免组合爆炸

    Returns:
        [{"expression": str, "decay": int, "truncation": float, "neutralization": str"}, ...]
    """
    if template_params is None:
        template_params = TEMPLATE_PARAMS.copy()
    windows = template_params.get("window", [5, 10, 20])
    decays = template_params.get("decay", [3, 5])
    truncations = template_params.get("truncation", [0.01, 0.05])
    neutralizations = template_params.get("neutralization", ["INDUSTRY", "SECTOR", "MARKET"])

    # 单字段列表：每个推荐字段的 field_id 可直接用于表达式
    single_fields = [(f.get("field_id") or f.get("field_name") or "").strip() for f in recommended_fields if (f.get("field_id") or f.get("field_name"))]
    single_fields = [x for x in single_fields if x]

    # 双字段组合（用于 fields_required==2 的模板）
    two_field_pairs = []
    if len(single_fields) >= 2:
        for i, a in enumerate(single_fields):
            for b in single_fields[i + 1:]:
                two_field_pairs.append((a, b))
                if len(two_field_pairs) >= max_two_field_pairs:
                    break
            if len(two_field_pairs) >= max_two_field_pairs:
                break

    result = []
    for t in templates:
        expr_tpl = t.get("expression") or ""
        n_fields = int(t.get("fields_required") or 1)
        if n_fields == 1:
            for field in single_fields:
                for w in windows:
                    sub_expr = _substitute_expression(expr_tpl, {"field": field, "window": w})
                    for dec, trun, neut in itertools.product(decays, truncations, neutralizations):
                        result.append({
                            "expression": sub_expr,
                            "decay": dec,
                            "truncation": trun,
                            "neutralization": neut,
                        })
        elif n_fields == 2:
            for (f1, f2) in two_field_pairs:
                for w in windows:
                    sub_expr = _substitute_expression(expr_tpl, {"field1": f1, "field2": f2, "window": w})
                    for dec, trun, neut in itertools.product(decays, truncations, neutralizations):
                        result.append({
                            "expression": sub_expr,
                            "decay": dec,
                            "truncation": trun,
                            "neutralization": neut,
                        })

    # 按结构去重：每种结构只保留 1~3 个参数组合
    from ai.alpha_deduplicator import deduplicate
    global _last_dedup_stats
    result, _last_dedup_stats = deduplicate(result, max_per_structure=3)
    # 聚类并输出 alpha_cluster_report.json
    if result:
        try:
            from ai.alpha_cluster import run_cluster_and_report
            run_cluster_and_report(result)
        except Exception as e:
            logger.warning("Alpha 聚类报告写入跳过: %s", e)
    return result


def get_last_dedup_stats() -> Optional[Dict[str, int]]:
    """返回最近一次 generate_alphas 去重后的统计，便于上层输出。"""
    return _last_dedup_stats


def generate_alphas_from_expressions(
    template_expressions: List[str],
    recommended_fields: List[Dict[str, Any]],
    template_params: Optional[Dict[str, List]] = None,
    max_two_field_pairs: int = 80,
) -> List[Dict[str, Any]]:
    """
    从「表达式模板字符串」+ 推荐字段生成 Alpha 列表（不做去重，供 Pipeline 统计 alpha_generated）。

    类型过滤规则：
    - 仅使用 normalized_type 为 vector/matrix 的字段用于数值计算
    - symbol/group 类型字段不参与因子生成

    Args:
        template_expressions: 模板表达式列表，如 AlphaBuilder.TEMPLATES 的子集（含 {field},{window} 或 {field1},{field2}）
        recommended_fields: [{"field_id", "dataset_id", "normalized_type", ...}, ...]
        template_params: 同 generate_alphas
        max_two_field_pairs: 双字段模板最多 (field1, field2) 组合数

    Returns:
        [{"expression", "decay", "truncation", "neutralization"}, ...]
    """
    if template_params is None:
        template_params = TEMPLATE_PARAMS.copy()
    windows = template_params.get("window", [5, 10, 20])
    decays = template_params.get("decay", [3, 5])
    truncations = template_params.get("truncation", [0.01, 0.05])
    neutralizations = template_params.get("neutralization", ["INDUSTRY", "SECTOR", "MARKET"])

    # 按类型过滤：仅使用 vector 类型字段用于数值计算
    numeric_fields = filter_fields_by_type(recommended_fields, list(NUMERIC_FIELD_TYPES))

    # 提取字段 ID 列表
    single_fields = []
    for f in numeric_fields:
        fid = (f.get("field_id") or f.get("field_name") or "").strip()
        if fid:
            single_fields.append(fid)

    # 双字段组合
    two_field_pairs = []
    if len(single_fields) >= 2:
        for i, a in enumerate(single_fields):
            for b in single_fields[i + 1:]:
                two_field_pairs.append((a, b))
                if len(two_field_pairs) >= max_two_field_pairs:
                    break
            if len(two_field_pairs) >= max_two_field_pairs:
                break

    result = []
    for expr_tpl in template_expressions:
        if not expr_tpl or not isinstance(expr_tpl, str):
            continue
        is_pair = "{field1}" in expr_tpl and "{field2}" in expr_tpl
        if is_pair:
            for (f1, f2) in two_field_pairs:
                for w in windows:
                    sub = _substitute_expression(
                        expr_tpl,
                        {"field1": f1, "field2": f2, "window": w, "window1": w, "window2": w},
                    )
                    for dec, trun, neut in itertools.product(decays, truncations, neutralizations):
                        result.append({"expression": sub, "decay": dec, "truncation": trun, "neutralization": neut})
        else:
            for field in single_fields:
                for w in windows:
                    sub = _substitute_expression(expr_tpl, {"field": field, "window": w, "window2": w})
                    for dec, trun, neut in itertools.product(decays, truncations, neutralizations):
                        result.append({"expression": sub, "decay": dec, "truncation": trun, "neutralization": neut})
    return result


def generate_alphas_with_operators(
    recommended_fields: List[Dict[str, Any]],
    operators_df,
    templates_path: Optional[Path] = None,
    template_params: Optional[Dict[str, List]] = None,
    max_two_field_pairs: int = 100,
    template_mode: str = "default",
    dataset_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    一站式：加载模板 → 按操作符过滤 → 生成 Alpha 列表。

    Args:
        recommended_fields: 推荐字段列表
        operators_df: 操作符 DataFrame
        templates_path: 自定义模板路径（优先级最高）
        template_params: 模板参数配置
        max_two_field_pairs: 双字段组合上限
        template_mode: 模板模式 ("default" 或 "specialized")
        dataset_id: 数据集 ID（用于 specialized 模式）

    Returns:
        Alpha 列表
    """
    templates = load_templates(
        path=templates_path,
        template_mode=template_mode,
        dataset_id=dataset_id,
    )
    operator_names = get_operator_names(operators_df)
    filtered = filter_templates_by_operators(templates, operator_names)
    return generate_alphas(
        recommended_fields,
        filtered,
        template_params=template_params,
        max_two_field_pairs=max_two_field_pairs,
    )

# -*- coding: utf-8 -*-
"""
类型错误修复模块

读取 type_check_report.json，对类型错误的 alpha 表达式进行自动修复或 AI 辅助修复，
然后返回可回测的 alpha_items。

修复策略：
1. 自动修复（规则匹配）：
   - rank(event_field) → rank(vec_avg(event_field))
   - ts_*(event_field) → ts_*(vec_avg(event_field))
   - vec_avg(matrix_field) → matrix_field（移除多余的 vec_avg）

2. AI 辅助修复（复杂错误）：
   - 调用 AI 分析错误并重构表达式
"""
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)


def classify_type_error(error: Dict) -> str:
    """
    分类类型错误，返回修复策略。

    Returns:
        'need_vec_avg': 需要添加 vec_avg 包装
        'remove_vec_avg': 需要移除 vec_avg
        'replace_operator': 需要替换操作符
        'unknown': 无法自动修复
    """
    error_type = error.get("error_type", "")
    operator = error.get("operator", "")
    actual = error.get("actual", "")
    expected = error.get("expected", [])

    if error_type != "type_mismatch":
        return "unknown"

    # 场景1：event 字段被直接用于需要 vector 的操作符
    # 如：rank(event_field), ts_mean(event_field, 5)
    if actual == "event" and "vector" in expected:
        # 这些操作符需要 vector 输入，需要用 vec_avg 包装
        vector_operators = {
            "rank", "ts_mean", "ts_sum", "ts_std_dev", "ts_delta",
            "ts_rank", "ts_zscore", "ts_decay_linear", "ts_arg_max",
            "ts_arg_min", "ts_min", "ts_max", "ts_corr", "abs",
            "scale", "zscore", "sign", "log", "pow"
        }
        if operator.lower() in vector_operators:
            return "need_vec_avg"

    # 场景2：vector 字段被错误地用 vec_avg 包装
    # 如：vec_avg(matrix_field)
    if actual == "vector" and operator.lower() in ["vec_avg", "vec_sum", "vec_count"]:
        return "remove_vec_avg"

    # 场景3：event 字段被用于需要 vector 的操作符（嵌套情况）
    # 如：group_rank(event_field, industry)
    if actual == "event" and operator.lower() == "group_rank":
        return "need_vec_avg"

    return "unknown"


def find_field_in_expression(expr: str, error: Dict) -> Optional[str]:
    """
    从错误信息中提取出问题的字段名。
    """
    subexpr = error.get("subexpr", "")
    # 从 subexpr 中提取字段名
    # 例如：rank(anl44_best_pe_ratio) -> anl44_best_pe_ratio
    # 例如：vec_avg(anl44_bps_best_eeps_cur_yr) -> anl44_bps_best_eeps_cur_yr

    # 匹配 anl44_ 开头的字段
    match = re.search(r'anl44_[a-zA-Z0-9_]+', subexpr)
    if match:
        return match.group(0)

    return None


def add_vec_avg_wrapper(expr: str, error: Dict, field_type_index: Dict) -> Optional[str]:
    """
    为 event 类型字段添加 vec_avg 包装。

    例如：
    - rank(anl44_best_pe_ratio) → rank(vec_avg(anl44_best_pe_ratio))
    - ts_mean(anl44_best_sales_4wk_up, 5) → ts_mean(vec_avg(anl44_best_sales_4wk_up), 5)
    """
    field = find_field_in_expression(expr, error)
    if not field:
        return None

    # 检查字段是否确实是 event 类型
    field_type = field_type_index.get(field, {}).get("normalized_type", "")
    if field_type != "event":
        return None

    # 检查是否已经有 vec_avg 包装
    if f"vec_avg({field})" in expr or f"vec_sum({field})" in expr:
        return None  # 已经包装过了

    # 找到字段在表达式中的位置，添加 vec_avg 包装
    # 使用正则替换，确保只替换字段本身，不替换包含该字段名的其他部分
    pattern = r'\b(' + re.escape(field) + r')\b'
    new_expr = re.sub(pattern, r'vec_avg(\1)', expr)

    return new_expr


def remove_vec_avg_wrapper(expr: str, error: Dict, field_type_index: Dict) -> Optional[str]:
    """
    移除对 matrix 字段多余的 vec_avg 包装。

    例如：
    - rank(vec_avg(anl44_bps_best_eeps_cur_yr)) → rank(anl44_bps_best_eeps_cur_yr)
    """
    field = find_field_in_expression(expr, error)
    if not field:
        return None

    # 检查字段是否确实是 vector 类型（MATRIX）
    field_type = field_type_index.get(field, {}).get("normalized_type", "")
    if field_type != "vector":
        return None

    # 移除 vec_avg 或 vec_sum 包装
    # vec_avg(field) -> field
    new_expr = expr.replace(f"vec_avg({field})", field)
    new_expr = new_expr.replace(f"vec_sum({field})", field)

    return new_expr if new_expr != expr else None


def auto_repair_expression(
    expr: str,
    error: Dict,
    field_type_index: Dict,
) -> Tuple[Optional[str], str]:
    """
    尝试自动修复表达式。

    Returns:
        (fixed_expr, repair_type)
        - fixed_expr: 修复后的表达式，None 表示无法自动修复
        - repair_type: 'need_vec_avg', 'remove_vec_avg', 'unknown'
    """
    repair_type = classify_type_error(error)

    if repair_type == "need_vec_avg":
        fixed = add_vec_avg_wrapper(expr, error, field_type_index)
        return fixed, "need_vec_avg" if fixed else "unknown"

    elif repair_type == "remove_vec_avg":
        fixed = remove_vec_avg_wrapper(expr, error, field_type_index)
        return fixed, "remove_vec_avg" if fixed else "unknown"

    return None, "unknown"


def ai_repair_expression(
    expr: str,
    error: Dict,
    field_type_index: Dict,
    ai_researcher,
) -> Optional[str]:
    """
    调用 AI 辅助修复表达式（用于无法自动修复的复杂错误）。
    """
    if not ai_researcher:
        return None

    try:
        # 构建 prompt
        error_desc = json.dumps(error, ensure_ascii=False, indent=2)
        prompt = f"""请修复以下 Alpha 表达式的类型错误。

原始表达式：{expr}

错误信息：
{error_desc}

修复规则：
1. 如果字段是 event 类型（VECTOR），需要用 vec_avg() 包装后才能用于 rank、ts_* 等操作符
2. 如果字段是 vector 类型（MATRIX），可以直接使用，不需要 vec_avg()
3. 只返回修复后的表达式，不要解释

修复后的表达式："""

        # 调用 AI
        if hasattr(ai_researcher, 'repair_expression'):
            return ai_researcher.repair_expression(prompt)
        elif hasattr(ai_researcher, 'ask'):
            response = ai_researcher.ask(prompt)
            # 提取表达式
            if response:
                # 清理响应，提取表达式
                lines = response.strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith('#') and not line.startswith('//'):
                        # 移除可能的 markdown 标记
                        if line.startswith('```'):
                            continue
                        if line.endswith('```'):
                            continue
                        return line
        return None

    except Exception as e:
        logger.warning(f"AI 修复失败: {e}")
        return None


def step_type_error_repair(
    type_check_report_path: Path,
    field_type_index: Dict,
    ai_researcher=None,
    enable_ai_repair: bool = True,
) -> Tuple[List[Dict], Dict]:
    """
    读取 type_check_report.json，修复错误表达式，返回可回测的 alpha_items。

    Args:
        type_check_report_path: type_check_report.json 文件路径
        field_type_index: 字段类型索引 {field_id: {normalized_type, ...}}
        ai_researcher: AI 研究员实例（可选，用于复杂错误修复）
        enable_ai_repair: 是否启用 AI 辅助修复

    Returns:
        (fixed_items, stats)
        - fixed_items: 修复后的 alpha_items 列表
        - stats: 统计信息 {total, auto_fixed, ai_fixed, failed}
    """
    if not type_check_report_path.exists():
        logger.warning(f"类型检查报告不存在: {type_check_report_path}")
        return [], {"total": 0, "auto_fixed": 0, "ai_fixed": 0, "failed": 0}

    try:
        with open(type_check_report_path, "r", encoding="utf-8") as f:
            report = json.load(f)
    except Exception as e:
        logger.error(f"读取类型检查报告失败: {e}")
        return [], {"total": 0, "auto_fixed": 0, "ai_fixed": 0, "failed": 0}

    errors = report.get("errors", [])
    stats = {
        "total": len(errors),
        "auto_fixed": 0,
        "ai_fixed": 0,
        "failed": 0,
    }

    if not errors:
        return [], stats

    fixed_items = []
    repaired_expressions = set()  # 去重

    logger.info(f"开始修复 {len(errors)} 个类型错误...")

    for err in errors:
        expr = err.get("expression", "")
        if not expr:
            continue

        error_info = err.get("error", {})

        # 第一层：自动修复
        fixed_expr, repair_type = auto_repair_expression(expr, error_info, field_type_index)

        if fixed_expr:
            stats["auto_fixed"] += 1
            logger.debug(f"自动修复成功: {expr[:50]}... → {fixed_expr[:50]}...")
        else:
            # 第二层：AI 修复
            if enable_ai_repair and ai_researcher:
                fixed_expr = ai_repair_expression(expr, error_info, field_type_index, ai_researcher)
                if fixed_expr:
                    stats["ai_fixed"] += 1
                    logger.debug(f"AI 修复成功: {expr[:50]}... → {fixed_expr[:50]}...")

        if fixed_expr and fixed_expr not in repaired_expressions:
            repaired_expressions.add(fixed_expr)
            fixed_items.append({
                "expression": fixed_expr,
                "original_expression": expr,
                "repair_type": repair_type if repair_type != "unknown" else "ai",
                "error_info": error_info,
            })
        else:
            stats["failed"] += 1

    logger.info(f"修复完成: 自动修复 {stats['auto_fixed']} 个, AI 修复 {stats['ai_fixed']} 个, 失败 {stats['failed']} 个")

    return fixed_items, stats


def save_type_repair_report(output_dir: Path, fixed_items: List[Dict], stats: Dict) -> Path:
    """
    保存类型修复报告。
    """
    report = {
        "stats": stats,
        "repaired": [
            {
                "expression": item["expression"],
                "original_expression": item["original_expression"],
                "repair_type": item["repair_type"],
            }
            for item in fixed_items[:100]  # 限制数量
        ],
    }

    report_path = output_dir / "type_repair_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    logger.info(f"类型修复报告已保存: {report_path}")
    return report_path


if __name__ == "__main__":
    # 测试
    print("=== 类型错误修复模块测试 ===")

    # 测试错误分类
    test_errors = [
        {"error_type": "type_mismatch", "operator": "rank", "actual": "event", "expected": ["vector"]},
        {"error_type": "type_mismatch", "operator": "vec_avg", "actual": "vector", "expected": ["event"]},
        {"error_type": "type_mismatch", "operator": "ts_mean", "actual": "event", "expected": ["vector"]},
        {"error_type": "type_mismatch", "operator": "group_rank", "actual": "event", "expected": ["vector"]},
    ]

    for err in test_errors:
        result = classify_type_error(err)
        print(f"错误: {err} → 修复策略: {result}")
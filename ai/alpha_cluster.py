# -*- coding: utf-8 -*-
"""
Alpha 聚类模块
基于表达式「操作符序列」将 Alpha 聚类，避免回测大量相似 Alpha；输出 alpha_cluster_report.json。
"""
import json
import re
import logging
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

logger = logging.getLogger(__name__)

# 操作符序列 -> 聚类名称（首层 → 二层，小写）
# 与用户约定：mean_factor/mean_reversion、momentum、volatility、correlation 等
OPERATOR_SEQUENCE_TO_CLUSTER: Dict[Tuple[str, str], str] = {
    ("rank", "ts_mean"): "mean_reversion",
    ("rank", "ts_delta"): "momentum",
    ("rank", "ts_std_dev"): "volatility",
    ("rank", "ts_stddev"): "volatility",
    ("rank", "ts_corr"): "correlation",
    ("rank", "corr"): "correlation",
    ("rank", "ts_covariance"): "correlation",
    ("rank", "cov"): "correlation",
    ("rank", "ts_decay_linear"): "decay",
    ("rank", "decay_linear"): "decay",
    ("rank", "ts_rank"): "rank_factor",
    ("rank", "ts_zscore"): "zscore",
    ("rank", "zscore"): "zscore",
    ("rank", "scale"): "scale",
    ("rank", "ts_delay"): "delay",
    ("rank", "delay"): "delay",
    ("rank", "ts_sum"): "sum",
    ("rank", "ts_arg_max"): "argmax",
    ("rank", "ts_arg_min"): "argmin",
    ("rank", "ts_argmax"): "argmax",
    ("rank", "ts_argmin"): "argmin",
    ("rank", "abs"): "abs",
    ("rank", "log"): "log",
    ("zscore", "ts_mean"): "zscore_mean",
    ("scale", "ts_mean"): "scale_mean",
}
# 单层 rank({field}) 等
SINGLE_OP_CLUSTER: Dict[str, str] = {
    "rank": "rank_only",
    "zscore": "zscore_only",
    "scale": "scale_only",
}


def _get_known_operators() -> set:
    """获取已知操作符集合（用于过滤非操作符标识符）。"""
    try:
        from config.ai_config import OPERATOR_WHITELIST, OPERATOR_ALIASES
        known = set()
        for ops in OPERATOR_WHITELIST.values():
            known.update(str(o).lower() for o in ops)
        for k in OPERATOR_ALIASES:
            known.add(k.lower())
            known.add(OPERATOR_ALIASES[k].lower())
        return known
    except Exception:
        return set()


def extract_operator_sequence(expression: str, known_operators: Optional[set] = None) -> List[str]:
    """
    从表达式中按出现顺序提取「操作符序列」（仅保留已知操作符，用于聚类）。

    例如：rank(ts_mean(close,5)) -> ['rank', 'ts_mean']
         rank(ts_delta(volume,10)) -> ['rank', 'ts_delta']
    """
    if not expression or not expression.strip():
        return []
    expr = expression.strip()
    if known_operators is None:
        known_operators = _get_known_operators()
    # 所有 "name(" 的 name，按位置顺序
    pattern = re.compile(r"([a-z_][a-z0-9_]*)\s*\(")
    sequence = []
    for m in pattern.finditer(expr):
        name = m.group(1).lower()
        if name in known_operators:
            sequence.append(name)
    return sequence


def sequence_to_cluster_name(sequence: List[str]) -> str:
    """
    将操作符序列映射为聚类名称。
    使用首层、二层操作符查表；否则单层或未知归为 other。
    """
    if not sequence:
        return "other"
    if len(sequence) >= 2:
        key = (sequence[0], sequence[1])
        if key in OPERATOR_SEQUENCE_TO_CLUSTER:
            return OPERATOR_SEQUENCE_TO_CLUSTER[key]
    if len(sequence) == 1:
        if sequence[0] in SINGLE_OP_CLUSTER:
            return SINGLE_OP_CLUSTER[sequence[0]]
    return "other"


def get_cluster_name(expression: str, known_operators: Optional[set] = None) -> str:
    """单条表达式的聚类名称。"""
    seq = extract_operator_sequence(expression, known_operators)
    return sequence_to_cluster_name(seq)


def cluster_alphas(
    alpha_items: List[Dict[str, Any]],
    known_operators: Optional[set] = None,
) -> Tuple[Dict[str, int], Dict[str, List[Dict[str, Any]]]]:
    """
    对 Alpha 列表按操作符序列聚类，返回各簇数量及分簇列表。

    Args:
        alpha_items: 每项至少含 "expression"
        known_operators: 可选，已知操作符集合；缺省时从 config 加载

    Returns:
        (counts, clusters)
        counts: {"momentum": 120, "mean_reversion": 80, ...}
        clusters: {"momentum": [item, ...], ...}
    """
    if known_operators is None:
        known_operators = _get_known_operators()
    clusters: Dict[str, List[Dict[str, Any]]] = {}
    for item in alpha_items:
        expr = item.get("expression") or ""
        name = get_cluster_name(expr, known_operators)
        if name not in clusters:
            clusters[name] = []
        clusters[name].append(item)
    counts = {k: len(v) for k, v in clusters.items()}
    return counts, clusters


def run_cluster_and_report(
    alpha_items: List[Dict[str, Any]],
    report_path: Optional[Path] = None,
) -> Dict[str, int]:
    """
    执行聚类并写入 alpha_cluster_report.json。

    Args:
        alpha_items: Alpha 列表
        report_path: 报告路径，默认项目根目录 alpha_cluster_report.json

    Returns:
        各聚类数量 {"momentum": 120, "mean_reversion": 80, ...}
    """
    counts, _ = cluster_alphas(alpha_items)
    if report_path is None:
        try:
            _root = Path(__file__).resolve().parent.parent
            report_path = _root / "alpha_cluster_report.json"
        except Exception:
            report_path = Path("alpha_cluster_report.json")
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(counts, f, ensure_ascii=False, indent=2)
    logger.info("Alpha 聚类报告已写入: %s", report_path)
    return counts

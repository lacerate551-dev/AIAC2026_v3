# -*- coding: utf-8 -*-
"""
数据频率推断模块
基于数据集名称、描述、类别等信息智能推断数据频率
支持两种模式:
1. 基于规则的快速推断 (默认)
2. 基于回测的精确检测 (可选，需要 session)
"""
import re
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


# 频率关键词映射
FREQUENCY_KEYWORDS = {
    "daily": [
        "daily", "day", "intraday", "price", "volume", "pv", "market",
        "trading", "tick", "quote", "high frequency", "hf"
    ],
    "weekly": [
        "weekly", "week"
    ],
    "monthly": [
        "monthly", "month"
    ],
    "quarterly": [
        "quarterly", "quarter", "q1", "q2", "q3", "q4", "fiscal quarter"
    ],
    "annual": [
        "annual", "yearly", "year", "fy", "fiscal year"
    ],
    "semi-annual": [
        "semi-annual", "semi annual", "half-year", "half year"
    ],
    "irregular": [
        "event", "announcement", "filing", "news", "sentiment",
        "earnings call", "conference", "irregular"
    ]
}

# 数据集类别到频率的映射
CATEGORY_FREQUENCY_MAP = {
    "pv": "daily",  # Price Volume
    "price": "daily",
    "volume": "daily",
    "market": "daily",
    "technical": "daily",
    "fundamental": "quarterly",  # 大部分基本面数据是季度
    "analyst": "quarterly",  # 分析师预测通常是季度/年度
    "financial": "quarterly",
    "balance": "quarterly",
    "income": "quarterly",
    "cashflow": "quarterly",
    "sentiment": "irregular",
    "news": "irregular",
    "event": "irregular",
    "esg": "annual",
}


def infer_frequency_from_text(text: str) -> Optional[str]:
    """
    从文本中推断频率

    Args:
        text: 数据集名称或描述

    Returns:
        推断的频率，如果无法推断则返回 None
    """
    if not text:
        return None

    text_lower = text.lower()

    # 按优先级检查关键词
    for freq, keywords in FREQUENCY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text_lower:
                return freq

    return None


def infer_frequency_from_category(category_info: Dict[str, Any]) -> Optional[str]:
    """
    从数据集类别推断频率

    Args:
        category_info: 类别信息字典，包含 id 和 name

    Returns:
        推断的频率，如果无法推断则返回 None
    """
    if not category_info:
        return None

    category_id = str(category_info.get("id", "")).lower()
    category_name = str(category_info.get("name", "")).lower()

    # 检查类别 ID
    for cat_key, freq in CATEGORY_FREQUENCY_MAP.items():
        if cat_key in category_id or cat_key in category_name:
            return freq

    return None


def infer_dataset_frequency(
    dataset_id: str,
    dataset_name: str,
    description: str = "",
    category: Optional[Dict[str, Any]] = None,
    subcategory: Optional[Dict[str, Any]] = None,
) -> str:
    """
    综合推断数据集频率

    优先级:
    1. 数据集 ID (如 pv1 -> daily)
    2. 数据集名称
    3. 数据集描述
    4. 子类别
    5. 主类别
    6. 默认 daily

    Args:
        dataset_id: 数据集 ID
        dataset_name: 数据集名称
        description: 数据集描述
        category: 主类别信息
        subcategory: 子类别信息

    Returns:
        推断的频率 (daily/weekly/monthly/quarterly/annual/semi-annual/irregular)
    """
    # 1. 从数据集 ID 推断
    freq = infer_frequency_from_text(dataset_id)
    if freq:
        return freq

    # 2. 从数据集名称推断
    freq = infer_frequency_from_text(dataset_name)
    if freq:
        return freq

    # 3. 从描述推断
    freq = infer_frequency_from_text(description)
    if freq:
        return freq

    # 4. 从子类别推断
    if subcategory:
        freq = infer_frequency_from_category(subcategory)
        if freq:
            return freq

    # 5. 从主类别推断
    if category:
        freq = infer_frequency_from_category(category)
        if freq:
            return freq

    # 6. 默认返回 daily (大部分 BRAIN 数据是日频)
    return "daily"


def get_frequency_priority(frequency: str) -> int:
    """
    获取频率的优先级 (数字越小优先级越高)
    用于排序和选择
    """
    priority_map = {
        "daily": 1,
        "weekly": 2,
        "monthly": 3,
        "quarterly": 4,
        "semi-annual": 5,
        "annual": 6,
        "irregular": 7,
    }
    return priority_map.get(frequency, 99)


def infer_field_frequency_by_backtest(
    session,
    field_name: str,
    region: str = "USA",
    dataset_id: str = None,
    use_cache: bool = True,
) -> Dict[str, Any]:
    """
    通过回测精确检测字段的更新频率

    Args:
        session: 已认证的 BRAIN 会话
        field_name: 字段名称
        region: 区域代码
        dataset_id: 数据集 ID（可选）
        use_cache: 是否使用缓存结果

    Returns:
        {
            "field_name": str,
            "frequency": str,
            "confidence": float,
            "reasoning": List[str],
            "method": "backtest"
        }
    """
    from core.frequency_detector import FrequencyDetector

    # 检查缓存
    if use_cache:
        cache_dir = Path(__file__).parent.parent / "cache" / "frequency_detection"
        if cache_dir.exists():
            # 查找最新的缓存文件
            cache_files = sorted(cache_dir.glob("detection_results_*.json"), reverse=True)
            if cache_files:
                detector = FrequencyDetector(session, region)
                cached_results = detector.load_detection_results(cache_files[0])
                if field_name in cached_results:
                    logger.info(f"使用缓存的频率检测结果: {field_name}")
                    result = cached_results[field_name]
                    result["method"] = "backtest_cached"
                    return result

    # 运行回测检测
    logger.info(f"运行回测检测字段频率: {field_name}")
    detector = FrequencyDetector(session, region)
    result = detector.run_frequency_detection(field_name, dataset_id)
    result["method"] = "backtest"

    return result


def infer_field_frequency_hybrid(
    field_name: str,
    field_description: str = "",
    dataset_frequency: str = None,
    session = None,
    region: str = "USA",
    dataset_id: str = None,
    prefer_backtest: bool = False,
) -> Dict[str, Any]:
    """
    混合模式: 结合规则推断和回测检测

    优先级:
    1. 如果 prefer_backtest=True 且提供了 session，使用回测检测
    2. 否则使用规则推断
    3. 如果规则推断置信度低，且提供了 session，回退到回测检测

    Args:
        field_name: 字段名称
        field_description: 字段描述
        dataset_frequency: 数据集频率（作为参考）
        session: BRAIN 会话（可选）
        region: 区域代码
        dataset_id: 数据集 ID
        prefer_backtest: 是否优先使用回测检测

    Returns:
        {
            "field_name": str,
            "frequency": str,
            "confidence": float,
            "method": str,  # "rule" / "backtest" / "hybrid"
            "reasoning": List[str]
        }
    """
    # 模式 1: 优先回测
    if prefer_backtest and session:
        try:
            result = infer_field_frequency_by_backtest(
                session, field_name, region, dataset_id
            )
            if result["confidence"] >= 0.6:
                return result
            logger.info(f"回测检测置信度较低 ({result['confidence']:.2f}), 回退到规则推断")
        except Exception as e:
            logger.warning(f"回测检测失败: {e}, 回退到规则推断")

    # 模式 2: 规则推断
    freq_from_name = infer_frequency_from_text(field_name)
    freq_from_desc = infer_frequency_from_text(field_description)

    reasoning = []
    confidence = 0.0

    # 优先使用字段名称推断
    if freq_from_name:
        frequency = freq_from_name
        confidence = 0.8
        reasoning.append(f"从字段名称推断: {field_name} -> {frequency}")
    elif freq_from_desc:
        frequency = freq_from_desc
        confidence = 0.7
        reasoning.append(f"从字段描述推断: {frequency}")
    elif dataset_frequency:
        frequency = dataset_frequency
        confidence = 0.5
        reasoning.append(f"继承数据集频率: {dataset_frequency}")
    else:
        frequency = "daily"
        confidence = 0.3
        reasoning.append("使用默认频率: daily")

    # 模式 3: 如果规则推断置信度低，且有 session，尝试回测
    if confidence < 0.6 and session:
        try:
            logger.info(f"规则推断置信度较低 ({confidence:.2f}), 尝试回测检测")
            backtest_result = infer_field_frequency_by_backtest(
                session, field_name, region, dataset_id
            )
            if backtest_result["confidence"] > confidence:
                backtest_result["method"] = "hybrid"
                backtest_result["reasoning"] = reasoning + backtest_result.get("reasoning", [])
                return backtest_result
        except Exception as e:
            logger.warning(f"回测检测失败: {e}")

    return {
        "field_name": field_name,
        "frequency": frequency,
        "confidence": confidence,
        "method": "rule",
        "reasoning": reasoning,
    }


def batch_infer_field_frequencies(
    fields: List[Dict[str, str]],
    dataset_frequency: str = None,
    session = None,
    region: str = "USA",
    dataset_id: str = None,
    prefer_backtest: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """
    批量推断字段频率

    Args:
        fields: 字段列表，每个元素为 {"field_name": str, "description": str}
        dataset_frequency: 数据集频率
        session: BRAIN 会话
        region: 区域代码
        dataset_id: 数据集 ID
        prefer_backtest: 是否优先使用回测

    Returns:
        {field_name: frequency_result} 字典
    """
    results = {}

    for field in fields:
        field_name = field.get("field_name") or field.get("field_id")
        field_desc = field.get("description", "")

        try:
            result = infer_field_frequency_hybrid(
                field_name=field_name,
                field_description=field_desc,
                dataset_frequency=dataset_frequency,
                session=session,
                region=region,
                dataset_id=dataset_id,
                prefer_backtest=prefer_backtest,
            )
            results[field_name] = result
        except Exception as e:
            logger.error(f"推断字段 {field_name} 频率失败: {e}")
            results[field_name] = {
                "field_name": field_name,
                "frequency": dataset_frequency or "daily",
                "confidence": 0.0,
                "method": "fallback",
                "error": str(e),
            }

    return results


# 示例用法
if __name__ == "__main__":
    # 测试案例
    test_cases = [
        {
            "dataset_id": "pv1",
            "dataset_name": "Price Volume Data for Equity",
            "description": "Daily price and volume data",
            "category": {"id": "pv", "name": "Price Volume"},
            "expected": "daily"
        },
        {
            "dataset_id": "analyst10",
            "dataset_name": "Performance-Weighted Analyst Estimates",
            "description": "Quarterly earnings estimates",
            "category": {"id": "analyst", "name": "Analyst"},
            "expected": "quarterly"
        },
        {
            "dataset_id": "fundamental5",
            "dataset_name": "Balance Sheet Items",
            "description": "Annual and quarterly balance sheet data",
            "category": {"id": "fundamental", "name": "Fundamental"},
            "expected": "quarterly"
        },
        {
            "dataset_id": "news42",
            "dataset_name": "News Sentiment",
            "description": "Real-time news sentiment scores",
            "category": {"id": "sentiment", "name": "Sentiment"},
            "expected": "irregular"
        },
    ]

    print("频率推断测试:\n")
    for i, case in enumerate(test_cases, 1):
        inferred = infer_dataset_frequency(
            case["dataset_id"],
            case["dataset_name"],
            case.get("description", ""),
            case.get("category")
        )
        status = "✓" if inferred == case["expected"] else "✗"
        print(f"{status} 测试 {i}: {case['dataset_id']}")
        print(f"  预期: {case['expected']}, 推断: {inferred}")
        print()

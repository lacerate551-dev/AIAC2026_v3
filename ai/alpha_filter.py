# -*- coding: utf-8 -*-
"""
Alpha 筛选器模块
- 从本地回测结果文件中筛选符合条件的 Alpha
- 支持自定义筛选条件
- 支持批量获取最新状态
"""
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime

logger = logging.getLogger(__name__)

# 默认筛选条件（与 settings.py 一致）
DEFAULT_MIN_SHARPE = 1.0
DEFAULT_MIN_FITNESS = 0.5
DEFAULT_MAX_TURNOVER = 0.70


def load_backtest_results(research_dir: str) -> List[Dict[str, Any]]:
    """
    从 research 目录加载所有回测结果。

    Args:
        research_dir: research 目录路径

    Returns:
        所有回测结果列表
    """
    research_path = Path(research_dir)
    if not research_path.exists():
        logger.warning(f"research 目录不存在: {research_dir}")
        return []

    all_results = []

    # 遍历所有子目录
    for subdir in research_path.iterdir():
        if not subdir.is_dir():
            continue

        backtest_file = subdir / "backtest_results.json"
        if not backtest_file.exists():
            continue

        try:
            with open(backtest_file, "r", encoding="utf-8") as f:
                results = json.load(f)

            # 添加来源信息
            for r in results:
                r["_source_dir"] = str(subdir.name)

            all_results.extend(results)
            logger.info(f"加载 {subdir.name}: {len(results)} 条结果")
        except Exception as e:
            logger.warning(f"加载失败 {backtest_file}: {e}")

    logger.info(f"总计加载: {len(all_results)} 条回测结果")
    return all_results


def filter_alphas(
    results: List[Dict[str, Any]],
    min_sharpe: float = DEFAULT_MIN_SHARPE,
    min_fitness: float = DEFAULT_MIN_FITNESS,
    max_turnover: float = DEFAULT_MAX_TURNOVER,
    require_alpha_id: bool = True,
    additional_filter: Optional[Callable[[Dict], bool]] = None,
) -> List[Dict[str, Any]]:
    """
    根据条件筛选 Alpha。

    Args:
        results: 回测结果列表
        min_sharpe: 最小 Sharpe 比率
        min_fitness: 最小 Fitness
        max_turnover: 最大换手率
        require_alpha_id: 是否要求必须有 alpha_id
        additional_filter: 额外的筛选函数

    Returns:
        筛选后的 Alpha 列表
    """
    filtered = []

    for r in results:
        # 检查是否有 alpha_id
        if require_alpha_id and not r.get("alpha_id"):
            continue

        # 检查是否成功
        if not r.get("success", False):
            continue

        # 检查 Sharpe
        sharpe = r.get("sharpe", 0) or 0
        if sharpe < min_sharpe:
            continue

        # 检查 Fitness
        fitness = r.get("fitness", 0) or 0
        if fitness < min_fitness:
            continue

        # 检查 Turnover
        turnover = r.get("turnover", 1) or 1
        if turnover > max_turnover:
            continue

        # 额外筛选
        if additional_filter and not additional_filter(r):
            continue

        filtered.append(r)

    return filtered


def filter_by_expression(
    results: List[Dict[str, Any]],
    pattern: str,
    exclude: bool = False,
) -> List[Dict[str, Any]]:
    """
    根据表达式模式筛选 Alpha。

    Args:
        results: 回测结果列表
        pattern: 表达式模式（支持部分匹配）
        exclude: 是否排除匹配的（True 则保留不匹配的）

    Returns:
        筛选后的 Alpha 列表
    """
    filtered = []
    for r in results:
        expr = r.get("expression", "") or ""
        matches = pattern.lower() in expr.lower()
        if exclude:
            if not matches:
                filtered.append(r)
        else:
            if matches:
                filtered.append(r)
    return filtered


def sort_alphas(
    results: List[Dict[str, Any]],
    key: str = "sharpe",
    descending: bool = True,
) -> List[Dict[str, Any]]:
    """
    排序 Alpha 列表。

    Args:
        results: 回测结果列表
        key: 排序字段（sharpe, fitness, turnover）
        descending: 是否降序

    Returns:
        排序后的 Alpha 列表
    """
    def get_sort_value(r):
        v = r.get(key, 0)
        return v if v is not None else 0

    return sorted(results, key=get_sort_value, reverse=descending)


def print_alpha_table(
    results: List[Dict[str, Any]],
    limit: int = 20,
    show_expression: bool = False,
) -> None:
    """
    打印 Alpha 表格。

    Args:
        results: 回测结果列表
        limit: 显示数量限制
        show_expression: 是否显示表达式
    """
    if not results:
        print("无符合条件的 Alpha")
        return

    # 按Sharpe降序排序
    sorted_results = sort_alphas(results, "sharpe", descending=True)

    print(f"\n共 {len(sorted_results)} 个符合条件的 Alpha：")
    print("-" * 80)

    if show_expression:
        header = f"{'#':<4} {'Alpha ID':<12} {'Sharpe':>8} {'Fitness':>8} {'Turnover':>10} {'Expression'}"
    else:
        header = f"{'#':<4} {'Alpha ID':<12} {'Sharpe':>8} {'Fitness':>8} {'Turnover':>10}"
    print(header)
    print("-" * 80)

    for i, r in enumerate(sorted_results[:limit], 1):
        alpha_id = r.get("alpha_id", "N/A")
        sharpe = r.get("sharpe", 0) or 0
        fitness = r.get("fitness", 0) or 0
        turnover = r.get("turnover", 0) or 0

        if show_expression:
            expr = r.get("expression", "N/A")
            if len(expr) > 50:
                expr = expr[:47] + "..."
            print(f"{i:<4} {alpha_id:<12} {sharpe:>8.3f} {fitness:>8.3f} {turnover:>9.2%} {expr}")
        else:
            print(f"{i:<4} {alpha_id:<12} {sharpe:>8.3f} {fitness:>8.3f} {turnover:>9.2%}")

    if len(sorted_results) > limit:
        print(f"... 还有 {len(sorted_results) - limit} 个未显示")


def export_alpha_ids(
    results: List[Dict[str, Any]],
    output_path: str,
) -> int:
    """
    导出 alpha_id 列表到文件。

    Args:
        results: 回测结果列表
        output_path: 输出文件路径

    Returns:
        导出的 alpha 数量
    """
    alpha_ids = [r.get("alpha_id") for r in results if r.get("alpha_id")]

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(alpha_ids, f, indent=2)

    logger.info(f"已导出 {len(alpha_ids)} 个 alpha_id 到 {output_path}")
    return len(alpha_ids)


def get_alpha_stats(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    统计回测结果。

    Args:
        results: 回测结果列表

    Returns:
        统计信息字典
    """
    if not results:
        return {"total": 0}

    sharpes = [r.get("sharpe", 0) or 0 for r in results if r.get("success")]
    fitnesses = [r.get("fitness", 0) or 0 for r in results if r.get("success")]
    turnovers = [r.get("turnover", 0) or 0 for r in results if r.get("success")]

    return {
        "total": len(results),
        "success": len([r for r in results if r.get("success")]),
        "failed": len([r for r in results if not r.get("success")]),
        "sharpe": {
            "min": min(sharpes) if sharpes else 0,
            "max": max(sharpes) if sharpes else 0,
            "mean": sum(sharpes) / len(sharpes) if sharpes else 0,
        },
        "fitness": {
            "min": min(fitnesses) if fitnesses else 0,
            "max": max(fitnesses) if fitnesses else 0,
            "mean": sum(fitnesses) / len(fitnesses) if fitnesses else 0,
        },
        "turnover": {
            "min": min(turnovers) if turnovers else 0,
            "max": max(turnovers) if turnovers else 0,
            "mean": sum(turnovers) / len(turnovers) if turnovers else 0,
        },
    }


def interactive_filter(research_dir: str = "research") -> None:
    """
    交互式 Alpha 筛选器。
    """
    print("=" * 60)
    print("Alpha 筛选器")
    print("=" * 60)

    # 加载结果
    results = load_backtest_results(research_dir)
    if not results:
        print("未找到任何回测结果")
        return

    stats = get_alpha_stats(results)
    print(f"\n已加载 {stats['total']} 条回测结果（成功: {stats['success']}, 失败: {stats['failed']}）")
    print(f"Sharpe 范围: {stats['sharpe']['min']:.3f} ~ {stats['sharpe']['max']:.3f}")
    print(f"Fitness 范围: {stats['fitness']['min']:.3f} ~ {stats['fitness']['max']:.3f}")
    print(f"Turnover 范围: {stats['turnover']['min']:.2%} ~ {stats['turnover']['max']:.2%}")

    while True:
        print("\n" + "-" * 40)
        print("筛选条件设置：")
        print(f"  1. 最小 Sharpe: {DEFAULT_MIN_SHARPE}")
        print(f"  2. 最小 Fitness: {DEFAULT_MIN_FITNESS}")
        print(f"  3. 最大 Turnover: {DEFAULT_MAX_TURNOVER:.0%}")
        print("  4. 执行筛选")
        print("  5. 按表达式筛选")
        print("  0. 退出")

        choice = input("\n请选择: ").strip()

        if choice == "0":
            break
        elif choice == "4":
            filtered = filter_alphas(results)
            print_alpha_table(filtered, limit=30, show_expression=True)

            if filtered:
                export = input("\n是否导出 alpha_id 列表? (y/n): ").strip().lower()
                if export == "y":
                    output_path = f"research/filtered_alphas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    export_alpha_ids(filtered, output_path)
        elif choice == "5":
            pattern = input("输入表达式关键词: ").strip()
            if pattern:
                filtered = filter_by_expression(results, pattern)
                print_alpha_table(filtered, limit=20, show_expression=True)


if __name__ == "__main__":
    interactive_filter()
# -*- coding: utf-8 -*-
"""
批量回测闭环模块
流程：生成 Alpha 配置 → 批量回测 → 筛选（Sharpe/Fitness/Turnover）→ 失败自愈（单条）→ 可选下一轮
- 单 Alpha 失败：ErrorAnalyzer + AI 修复表达式 + 补考
- 批量失败：可调整参数或重新生成，由调用方控制下一轮
"""
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

from core.alpha_builder import AlphaBuilder
from core.backtest_runner import BacktestRunner
from config.settings import MIN_SHARPE, MIN_FITNESS, MAX_TURNOVER, BATCH_SIZE

logger = logging.getLogger(__name__)


def build_configs_from_alpha_items(
    alpha_items: List[Dict[str, Any]],
    region: str,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
) -> List[Dict]:
    """
    将 alpha_generator 输出的列表转为 BRAIN 回测用配置列表。
    alpha_items 每项: {"expression", "decay", "truncation", "neutralization"}
    """
    configs = []
    for item in alpha_items:
        expr = item.get("expression", "")
        if not expr:
            continue
        configs.append(AlphaBuilder.build_config(
            expr,
            region,
            universe=universe,
            delay=delay,
            decay=item.get("decay"),
            truncation=item.get("truncation"),
            neutralization=item.get("neutralization"),
        ))
    return configs


def filter_high_value(results: List[Dict]) -> List[Dict]:
    """筛选通过 MIN_SHARPE / MIN_FITNESS / MAX_TURNOVER 的结果。"""
    return [
        r for r in results
        if r.get("success") and r.get("sharpe", 0) >= MIN_SHARPE
        and r.get("fitness", 0) >= MIN_FITNESS
        and r.get("turnover", 1) <= MAX_TURNOVER
    ]


def run_batch_backtest(
    session,
    alpha_items: List[Dict[str, Any]],
    region: str,
    output_dir: Optional[str] = None,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
    batch_size: int = None,
    resume: bool = False,
) -> List[Dict]:
    """
    批量回测：alpha_items → 构建配置 → BacktestRunner.run_batch。
    """
    configs = build_configs_from_alpha_items(alpha_items, region, universe=universe, delay=delay)
    if not configs:
        return []
    runner = BacktestRunner()
    return runner.run_batch(
        session,
        configs,
        output_dir=output_dir,
        batch_size=batch_size or BATCH_SIZE,
        resume=resume,
    )


def run_self_heal(
    failed_results: List[Dict],
    session,
    region: str,
    dataset_id: str,
    ai_researcher,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
    min_confidence: float = 0.6,
) -> List[Dict]:
    """
    对失败结果中有 error_analysis 的项做 AI 自愈（单条修复 + 补考）。
    返回补考成功的结果列表（用于合并到总结果）。
    """
    from core.backtest_runner import BacktestRunner
    fixed_configs = []
    for r in failed_results:
        if not r.get("error_analysis"):
            continue
        try:
            fix_result = ai_researcher.diagnose_and_fix(
                r["error_analysis"],
                r,
                session,
                region,
                dataset_id,
                universe=universe,
                delay=delay,
            )
        except Exception as e:
            logger.warning(f"自愈调用异常: {e}")
            continue
        if not fix_result.get("success") or fix_result.get("confidence", 0) < min_confidence:
            continue
        fixed = fix_result.get("fixed_alpha")
        if not fixed or not fixed.get("expression"):
            continue
        cfg = AlphaBuilder.build_config(
            fixed.get("expression"),
            region,
            universe=universe,
            delay=delay,
            decay=fixed.get("decay"),
            truncation=fixed.get("truncation"),
            neutralization=fixed.get("neutralization"),
        )
        fixed_configs.append(cfg)
    if not fixed_configs:
        return []
    runner = BacktestRunner()
    return runner.run_batch(session, fixed_configs, sample_mode="all")


def run_full_loop(
    session,
    alpha_items: List[Dict[str, Any]],
    region: str,
    output_dir: Optional[str] = None,
    dataset_id: Optional[str] = None,
    ai_researcher=None,
    run_self_heal_flag: bool = True,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
) -> Dict[str, Any]:
    """
    单轮闭环：回测 → 筛选 → 失败自愈（若提供 ai_researcher 与 dataset_id）。

    Returns:
        {
            "results": 全部回测结果,
            "high_value": 筛选后的高价值结果,
            "failed": 失败列表,
            "retry_results": 自愈补考结果（若有）,
        }
    """
    results = run_batch_backtest(
        session, alpha_items, region,
        output_dir=output_dir, universe=universe, delay=delay,
    )
    high_value = filter_high_value(results)
    failed = [r for r in results if not r.get("success")]

    retry_results = []
    if run_self_heal_flag and failed and ai_researcher and dataset_id:
        retry_results = run_self_heal(
            failed, session, region, dataset_id, ai_researcher,
            universe=universe, delay=delay,
        )
    return {
        "results": results,
        "high_value": high_value,
        "failed": failed,
        "retry_results": retry_results,
    }

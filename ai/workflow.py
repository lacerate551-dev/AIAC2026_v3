# -*- coding: utf-8 -*-
"""
统一 AI 挖掘工作流入口
供菜单 9 与 Agent 共用：metadata → AI 分析 → 模板生成 → 回测闭环
"""
from typing import Dict, Any, List, Optional

from ai.metadata_builder import build_metadata_for_region_datasets
from ai.data_analysis import analyze_metadata
from ai.alpha_generator import generate_alphas_with_operators
from ai.backtest_loop import run_full_loop


def run_ai_mining_workflow(
    session,
    region: str,
    dataset_ids: List[str],
    data_manager,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
    ai_researcher=None,
    output_dir: Optional[str] = None,
    max_alphas: int = 200,
    run_backtest: bool = True,
    run_self_heal: bool = True,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """
    一站式 AI 挖掘工作流（供 main 菜单 9 与 Agent 调用）。

    Args:
        session: BRAIN 会话
        region: 区域代码
        dataset_ids: 数据集 ID 列表
        data_manager: DataManager 类或实例
        universe: 可选
        delay: 可选
        ai_researcher: AIResearcher 实例（分析 + 自愈用）；若不传且需分析/自愈会内建
        output_dir: 回测结果输出目录
        max_alphas: 最多生成 Alpha 数量（截断）
        run_backtest: 是否执行回测
        run_self_heal: 是否对失败项执行自愈
        force_refresh: 是否强制刷新 metadata 缓存

    Returns:
        {
            "metadata": { dataset_metadata, field_metadata, region },
            "analysis": { recommended_fields, neutralization_datasets },
            "alpha_items": [ { expression, decay, truncation, neutralization }, ... ],
            "loop_result": { results, high_value, failed, retry_results } 或 None,
        }
    """
    if not dataset_ids:
        return {"metadata": None, "analysis": None, "alpha_items": [], "loop_result": None}

    # 1. 构建 metadata
    meta = build_metadata_for_region_datasets(
        session, region, dataset_ids, data_manager,
        universe=universe, delay=delay, force_refresh=force_refresh,
    )
    dm, fm = meta["dataset_metadata"], meta["field_metadata"]
    if not fm:
        return {"metadata": meta, "analysis": None, "alpha_items": [], "loop_result": None}

    # 2. AI 分析（若未传 researcher 则只做 metadata，不分析）
    analysis = None
    if ai_researcher:
        analysis = analyze_metadata(dm, fm, region, ai_researcher)
        if analysis.get("error"):
            return {"metadata": meta, "analysis": analysis, "alpha_items": [], "loop_result": None}
    else:
        analysis = {"recommended_fields": [], "neutralization_datasets": []}

    recommended = analysis.get("recommended_fields") or []
    if not recommended:
        return {"metadata": meta, "analysis": analysis, "alpha_items": [], "loop_result": None}

    # 3. 模板生成
    operators_df = data_manager.get_operators(session)
    alpha_items = generate_alphas_with_operators(recommended, operators_df)
    if len(alpha_items) > max_alphas:
        alpha_items = alpha_items[:max_alphas]

    # 4. 回测闭环（可选）
    loop_result = None
    if run_backtest and alpha_items:
        loop_result = run_full_loop(
            session,
            alpha_items,
            region,
            output_dir=output_dir,
            dataset_id=dataset_ids[0],
            ai_researcher=ai_researcher if run_self_heal else None,
            run_self_heal_flag=run_self_heal and ai_researcher is not None,
            universe=universe,
            delay=delay,
        )

    return {
        "metadata": meta,
        "analysis": analysis,
        "alpha_items": alpha_items,
        "loop_result": loop_result,
    }

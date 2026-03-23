# -*- coding: utf-8 -*-
"""
工具注册表 — 将现有组件包装为 Agent 可调用的标准化工具函数
每个工具接收 ResearchContext，返回 ToolResult
"""

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from ai.agent.research_context import ResearchContext

logger = logging.getLogger(__name__)


@dataclass
class ToolResult:
    """工具执行结果"""
    success: bool
    data: Any = None
    message: str = ""
    error: str = ""


# ==================== 工具实现 ====================

def tool_analyze_dataset(ctx: ResearchContext, **kwargs) -> ToolResult:
    """数据分析（单数据集）：构建 metadata → AI 分析 → 推荐字段 + 中和数据集"""
    try:
        from core.session_manager import SessionManager
        from core.data_manager import DataManager
        from ai.researcher_brain import AIResearcher
        from ai.metadata_builder import build_metadata_for_region_datasets
        from ai.data_analysis import analyze_metadata

        session = SessionManager.get_session()
        dataset_ids = ctx.dataset_ids or []
        if not dataset_ids:
            return ToolResult(success=False, error="请先选择至少一个数据集")
        meta = build_metadata_for_region_datasets(
            session, ctx.region, dataset_ids, DataManager,
            universe=ctx.universe, delay=ctx.delay, force_refresh=False,
        )
        if not meta.get("field_metadata"):
            return ToolResult(success=False, error="无字段数据")
        researcher = AIResearcher()
        result = analyze_metadata(
            meta["dataset_metadata"], meta["field_metadata"],
            ctx.region, researcher,
        )
        if result.get("error"):
            return ToolResult(success=False, error=result["error"])
        ctx.analysis_result = result
        n = len(result.get("recommended_fields") or [])
        return ToolResult(success=True, data=result, message=f"数据分析完成，推荐 {n} 个字段")
    except Exception as e:
        logger.error(f"数据分析失败: {e}")
        return ToolResult(success=False, error=str(e))


def tool_analyze_multi_datasets(ctx: ResearchContext, **kwargs) -> ToolResult:
    """多数据集联合分析：同一套 metadata + AI 分析，支持跨 dataset 推荐字段"""
    try:
        from core.session_manager import SessionManager
        from core.data_manager import DataManager
        from ai.researcher_brain import AIResearcher
        from ai.metadata_builder import build_metadata_for_region_datasets
        from ai.data_analysis import analyze_metadata

        if len(ctx.dataset_ids) < 2:
            return ToolResult(success=False, error="多数据集联合至少需要 2 个数据集")
        session = SessionManager.get_session()
        meta = build_metadata_for_region_datasets(
            session, ctx.region, ctx.dataset_ids, DataManager,
            universe=ctx.universe, delay=ctx.delay, force_refresh=False,
        )
        if not meta.get("field_metadata"):
            return ToolResult(success=False, error="无字段数据")
        researcher = AIResearcher()
        result = analyze_metadata(
            meta["dataset_metadata"], meta["field_metadata"],
            ctx.region, researcher,
        )
        if result.get("error"):
            return ToolResult(success=False, error=result["error"])
        ctx.analysis_result = result
        n = len(result.get("recommended_fields") or [])
        return ToolResult(success=True, data=result, message=f"多数据集分析完成，推荐 {n} 个字段")
    except Exception as e:
        logger.error(f"多数据集分析失败: {e}")
        return ToolResult(success=False, error=str(e))


def tool_recommend_strategy(ctx: ResearchContext, **kwargs) -> ToolResult:
    """策略方向推荐"""
    try:
        from ai.researcher_brain import AIResearcher
        researcher = AIResearcher()
        if not ctx.analysis_result:
            return ToolResult(success=False, error="请先完成数据分析")
        result = researcher.recommend_strategy(ctx.analysis_result)
        return ToolResult(success=True, data=result, message="策略推荐完成")
    except Exception as e:
        logger.error(f"策略推荐失败: {e}")
        return ToolResult(success=False, error=str(e))


def tool_build_strategy(ctx: ResearchContext, **kwargs) -> ToolResult:
    """策略构建"""
    try:
        from ai.researcher_brain import AIResearcher
        researcher = AIResearcher()
        if not ctx.analysis_result:
            return ToolResult(success=False, error="请先完成数据分析")
        strategy_focus = kwargs.get("strategy_focus", "")
        result = researcher.build_strategy(
            analysis_result=ctx.analysis_result,
            strategy_focus=strategy_focus,
            region=ctx.region,
            universe=ctx.universe,
            delay=ctx.delay,
        )
        ctx.strategy_config = result
        return ToolResult(success=True, data=result, message="策略构建完成")
    except Exception as e:
        logger.error(f"策略构建失败: {e}")
        return ToolResult(success=False, error=str(e))


def tool_generate_alphas(ctx: ResearchContext, **kwargs) -> ToolResult:
    """基于推荐字段 + 模板批量生成 Alpha 配置（供回测调用）"""
    try:
        from core.session_manager import SessionManager
        from core.data_manager import DataManager
        from core.alpha_builder import AlphaBuilder
        from ai.alpha_generator import generate_alphas_with_operators, get_last_dedup_stats

        if not ctx.analysis_result or not ctx.analysis_result.get("recommended_fields"):
            return ToolResult(success=False, error="请先完成数据分析（analyze_dataset / analyze_multi_datasets）")

        session = SessionManager.get_session()
        recommended = ctx.analysis_result["recommended_fields"]
        operators_df = DataManager.get_operators(session)
        alpha_items = generate_alphas_with_operators(
            recommended, operators_df,
            max_two_field_pairs=kwargs.get("max_two_field_pairs", 80),
        )
        max_alphas = kwargs.get("max_alphas", 200)
        if len(alpha_items) > max_alphas:
            alpha_items = alpha_items[:max_alphas]
        if not alpha_items:
            return ToolResult(success=False, error="无可用模板或操作符不匹配")

        configs = []
        for item in alpha_items:
            configs.append(AlphaBuilder.build_config(
                item["expression"], ctx.region,
                universe=ctx.universe, delay=ctx.delay,
                decay=item.get("decay"), truncation=item.get("truncation"),
                neutralization=item.get("neutralization"),
            ))
        ctx.alpha_configs = configs
        dedup_stats = get_last_dedup_stats()
        if dedup_stats and dedup_stats.get("generated_alpha") != dedup_stats.get("after_dedup"):
            msg = f"模板生成 Alpha：生成 {dedup_stats['generated_alpha']} 个 → 去重后 {len(configs)} 个配置"
        else:
            msg = f"模板生成 {len(configs)} 个 Alpha 配置"
        return ToolResult(
            success=True,
            data={"count": len(configs), "dedup_stats": dedup_stats},
            message=msg,
        )
    except Exception as e:
        logger.error(f"Alpha 生成失败: {e}")
        return ToolResult(success=False, error=str(e))


def tool_run_backtest(ctx: ResearchContext, **kwargs) -> ToolResult:
    """执行批量回测（支持自愈：失败项 AI 诊断修复后补考）"""
    try:
        from core.session_manager import SessionManager
        from core.backtest_runner import BacktestRunner
        from config.settings import RESEARCH_DIR
        from datetime import datetime
        from ai.backtest_loop import run_self_heal

        if not ctx.alpha_configs:
            return ToolResult(success=False, error="请先生成 Alpha（generate_alphas）")

        session = SessionManager.get_session()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ds_name = "_".join(ctx.dataset_ids) if ctx.dataset_ids else "mixed"
        output_dir = str(RESEARCH_DIR / f"{ctx.region}_agent_{ds_name}_{timestamp}")

        runner = BacktestRunner()
        results = runner.run_batch(
            session, ctx.alpha_configs,
            output_dir=output_dir,
            batch_size=kwargs.get("batch_size", 10),
        )
        # 可选自愈：失败且有 error_analysis 的项
        if kwargs.get("run_self_heal") and ctx.analysis_result and ctx.dataset_ids:
            from ai.researcher_brain import AIResearcher
            failed = [r for r in results if not r.get("success")]
            if failed and ctx.dataset_ids:
                researcher = AIResearcher()
                retry_results = run_self_heal(
                    failed, session, ctx.region, ctx.dataset_ids[0], researcher,
                    universe=ctx.universe, delay=ctx.delay,
                )
                if retry_results:
                    results = results + retry_results
        ctx.backtest_results = results
        return ToolResult(
            success=True,
            data={"total": len(results), "output_dir": output_dir},
            message=f"回测完成，共 {len(results)} 个结果"
        )
    except Exception as e:
        logger.error(f"回测失败: {e}")
        return ToolResult(success=False, error=str(e))


def tool_optimize(ctx: ResearchContext, **kwargs) -> ToolResult:
    """闭环优化"""
    try:
        from ai.researcher_brain import AIResearcher
        researcher = AIResearcher()
        if not ctx.backtest_results:
            return ToolResult(success=False, error="请先完成回测")
        result = researcher.optimize_strategy(
            backtest_results=ctx.backtest_results,
            original_strategy=ctx.strategy_config,
        )
        # 优化结果中包含 updated_strategy
        if result.get("updated_strategy"):
            ctx.strategy_config = result["updated_strategy"]
        ctx.optimization_round += 1
        return ToolResult(success=True, data=result, message=f"第 {ctx.optimization_round} 轮优化完成")
    except Exception as e:
        logger.error(f"优化失败: {e}")
        return ToolResult(success=False, error=str(e))


def tool_query_memory(ctx: ResearchContext, **kwargs) -> ToolResult:
    """查询研究记忆"""
    try:
        from ai.agent.memory_store import MemoryStore
        store = MemoryStore()
        research = store.query_research(ctx.region, ctx.dataset_ids)
        knowledge = store.query_knowledge(ctx.region)
        return ToolResult(
            success=True,
            data={"research_history": research, "knowledge": knowledge},
            message=f"找到 {len(research)} 条研究记录，{len(knowledge)} 条知识"
        )
    except Exception as e:
        logger.error(f"记忆查询失败: {e}")
        return ToolResult(success=False, error=str(e))


# ==================== 工具注册表 ====================

TOOL_REGISTRY: Dict[str, Callable] = {
    "analyze_dataset": tool_analyze_dataset,
    "analyze_multi_datasets": tool_analyze_multi_datasets,
    "recommend_strategy": tool_recommend_strategy,
    "build_strategy": tool_build_strategy,
    "generate_alphas": tool_generate_alphas,
    "run_backtest": tool_run_backtest,
    "optimize": tool_optimize,
    "query_memory": tool_query_memory,
}

TOOL_DESCRIPTIONS = {
    "analyze_dataset": "分析单个数据集的字段和特征",
    "analyze_multi_datasets": "多数据集联合分析",
    "recommend_strategy": "基于分析结果推荐策略方向",
    "build_strategy": "构建完整的策略配置和生成脚本",
    "generate_alphas": "执行策略脚本批量生成 Alpha 表达式",
    "run_backtest": "对生成的 Alpha 执行回测",
    "optimize": "基于回测结果进行闭环优化",
    "query_memory": "查询历史研究记录和经验知识",
}


def execute_tool(tool_name: str, ctx: ResearchContext, **kwargs) -> ToolResult:
    """执行指定工具"""
    if tool_name not in TOOL_REGISTRY:
        return ToolResult(success=False, error=f"未知工具: {tool_name}")
    logger.info(f"执行工具: {tool_name}")
    return TOOL_REGISTRY[tool_name](ctx, **kwargs)

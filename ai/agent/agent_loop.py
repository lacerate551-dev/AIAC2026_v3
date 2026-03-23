# -*- coding: utf-8 -*-
"""
AgentLoop — plan → execute → evaluate → adjust 主循环
状态机驱动的研究流程引擎
"""

import json
import logging
from typing import Optional

from config.agent_config import AGENT_CONFIG, INTERVENTION_POLICY
from config.settings import MIN_SHARPE, MIN_FITNESS, MAX_TURNOVER, MEMORY_DIR
from ai.agent.research_context import ResearchContext, ResearchPhase
from ai.agent.planner import Planner
from ai.agent.tools import execute_tool, ToolResult, TOOL_DESCRIPTIONS
from ai.agent.memory_store import MemoryStore
from ai.prompt_templates_agent import (
    RESEARCH_SUMMARY_PROMPT,
    MEMORY_EXTRACTION_PROMPT,
)

logger = logging.getLogger(__name__)

# 阶段 → 工具名映射（用于干预策略查找）
PHASE_TOOL_MAP = {
    ResearchPhase.DATA_ANALYSIS: "data_analysis",
    ResearchPhase.STRATEGY_BUILD: "strategy_build",
    ResearchPhase.ALPHA_GENERATION: "alpha_generation",
    ResearchPhase.BACKTEST: "backtest",
    ResearchPhase.OPTIMIZATION: "optimization",
}

# 工具 → 完成后进入的阶段
TOOL_PHASE_TRANSITION = {
    "analyze_dataset": ResearchPhase.DATA_ANALYSIS,
    "analyze_multi_datasets": ResearchPhase.DATA_ANALYSIS,
    "recommend_strategy": ResearchPhase.STRATEGY_RECOMMENDATION,
    "build_strategy": ResearchPhase.STRATEGY_BUILD,
    "generate_alphas": ResearchPhase.ALPHA_GENERATION,
    "run_backtest": ResearchPhase.BACKTEST,
    "optimize": ResearchPhase.OPTIMIZATION,
}


class AgentLoop:
    """Agent 主循环"""

    def __init__(self):
        self.planner = Planner()
        self.memory = MemoryStore()
        self.running = False

    def run(self, ctx: ResearchContext) -> ResearchContext:
        """
        执行完整的研究流程
        INIT → DATA_ANALYSIS → STRATEGY → BUILD → GENERATE → BACKTEST → EVALUATE
        → (达标) REPORT → COMPLETED
        → (未达标) OPTIMIZATION → STRATEGY_BUILD (最多 N 轮)
        """
        self.running = True
        max_rounds = AGENT_CONFIG["max_optimization_rounds"]

        print(f"\n🚀 开始研究: {ctx.goal}")
        print(f"   区域: {ctx.region} / {ctx.universe} / delay={ctx.delay}")
        print(f"   数据集: {', '.join(ctx.dataset_ids) if ctx.dataset_ids else '待选择'}")

        try:
            while self.running and ctx.current_phase != ResearchPhase.COMPLETED:
                # 1. Plan — AI 决定下一步
                plan = self.planner.generate_plan(ctx)
                action = plan["next_action"]
                params = plan.get("action_params", {})

                print(f"\n{'=' * 60}")
                print(f"📋 阶段: {ctx.current_phase.value} → 计划执行: {action}")
                if plan.get("user_message"):
                    print(f"   {plan['user_message']}")
                print(f"{'=' * 60}")

                # 2. 干预检查
                if not self._check_intervention(ctx, action):
                    print("⏸️ 用户取消，流程暂停")
                    ctx.add_decision(ctx.current_phase.value, "user_cancel", "用户取消操作")
                    break

                # 3. Execute — 调用工具
                result = execute_tool(action, ctx, **params)

                if not result.success:
                    print(f"❌ 执行失败: {result.error}")
                    ctx.add_decision(ctx.current_phase.value, f"{action}_failed", result.error)
                    # 尝试诊断
                    user_choice = input("\n[R]重试 / [S]跳过 / [Q]退出: ").strip().upper()
                    if user_choice == "R":
                        continue
                    elif user_choice == "S":
                        pass
                    else:
                        break
                else:
                    print(f"✅ {result.message}")
                    ctx.add_decision(ctx.current_phase.value, f"{action}_success", result.message)

                # 4. 状态转换
                if action in TOOL_PHASE_TRANSITION:
                    ctx.current_phase = TOOL_PHASE_TRANSITION[action]

                # 5. 评估（回测后）
                if ctx.current_phase == ResearchPhase.BACKTEST and ctx.backtest_results:
                    evaluation = self._evaluate_results(ctx)
                    ctx.evaluation_summary = evaluation
                    ctx.current_phase = ResearchPhase.EVALUATION

                    if evaluation["meets_threshold"]:
                        print(f"\n🎉 回测达标！最佳 Sharpe: {evaluation['best_sharpe']:.4f}")
                        ctx.current_phase = ResearchPhase.REPORT
                    elif ctx.optimization_round >= max_rounds:
                        print(f"\n⚠️ 已达最大优化轮次 ({max_rounds})，生成报告")
                        ctx.current_phase = ResearchPhase.REPORT
                    else:
                        print(f"\n📊 未达标（最佳 Sharpe: {evaluation['best_sharpe']:.4f}），进入优化")
                        # 优化后回到 STRATEGY_BUILD 重新生成
                        continue

                # 6. 优化后重新进入构建
                if ctx.current_phase == ResearchPhase.OPTIMIZATION:
                    if ctx.optimization_round < max_rounds:
                        ctx.current_phase = ResearchPhase.STRATEGY_BUILD
                    else:
                        ctx.current_phase = ResearchPhase.REPORT

                # 7. 报告阶段
                if ctx.current_phase == ResearchPhase.REPORT:
                    self._generate_report(ctx)
                    self._extract_and_save_memory(ctx)
                    ctx.current_phase = ResearchPhase.COMPLETED

        except KeyboardInterrupt:
            print("\n\n⚠️ 用户中断，保存当前状态...")
        finally:
            # 保存上下文
            ctx.save(MEMORY_DIR)
            print(f"💾 研究状态已保存 (ID: {ctx.research_id})")

        return ctx

    def _check_intervention(self, ctx: ResearchContext, action: str) -> bool:
        """根据干预策略决定是否需要用户确认"""
        # 查找该工具对应的干预策略
        policy = None
        for phase, tool_key in PHASE_TOOL_MAP.items():
            if action in TOOL_DESCRIPTIONS and tool_key in INTERVENTION_POLICY:
                if ctx.current_phase == phase or action.startswith(tool_key.split("_")[0]):
                    policy = INTERVENTION_POLICY[tool_key]
                    break

        if policy is None:
            # 默认策略：根据工具名推断
            for key, pol in INTERVENTION_POLICY.items():
                if key in action:
                    policy = pol
                    break

        if policy == "auto":
            return True
        elif policy == "require_confirm":
            choice = input(f"\n⚡ 即将执行: {TOOL_DESCRIPTIONS.get(action, action)}\n   确认? (Y/n): ").strip().lower()
            return choice != "n"
        elif policy == "auto_with_review":
            print(f"   ℹ️ 自动执行: {TOOL_DESCRIPTIONS.get(action, action)}")
            return True
        else:
            return True

    def _evaluate_results(self, ctx: ResearchContext) -> dict:
        """评估回测结果是否达标"""
        if not ctx.backtest_results:
            return {"meets_threshold": False, "best_sharpe": 0, "summary": "无回测结果"}

        successful = [r for r in ctx.backtest_results if r.get("success")]
        if not successful:
            return {"meets_threshold": False, "best_sharpe": 0, "summary": "所有回测均失败"}

        sharpes = [r.get("sharpe", 0) for r in successful if r.get("sharpe") is not None]
        fitnesses = [r.get("fitness", 0) for r in successful if r.get("fitness") is not None]

        best_sharpe = max(sharpes) if sharpes else 0
        best_fitness = max(fitnesses) if fitnesses else 0
        n_pass = sum(
            1 for r in successful
            if r.get("sharpe", 0) >= MIN_SHARPE
            and r.get("fitness", 0) >= MIN_FITNESS
            and r.get("turnover", 1) <= MAX_TURNOVER
        )

        meets = best_sharpe >= MIN_SHARPE and n_pass > 0

        return {
            "meets_threshold": meets,
            "best_sharpe": best_sharpe,
            "best_fitness": best_fitness,
            "total": len(ctx.backtest_results),
            "successful": len(successful),
            "passing": n_pass,
            "summary": f"总计 {len(ctx.backtest_results)} 个，成功 {len(successful)} 个，达标 {n_pass} 个",
        }

    def _generate_report(self, ctx: ResearchContext):
        """生成研究总结报告"""
        print(f"\n{'=' * 60}")
        print("📝 研究总结")
        print(f"{'=' * 60}")
        print(ctx.get_status_summary())

        if ctx.evaluation_summary:
            ev = ctx.evaluation_summary
            print(f"\n回测评估: {ev.get('summary', 'N/A')}")
            print(f"最佳 Sharpe: {ev.get('best_sharpe', 0):.4f}")
            print(f"达标数量: {ev.get('passing', 0)}")

        # 尝试用 AI 生成总结
        try:
            from ai.researcher_brain import AIResearcher
            researcher = AIResearcher()

            backtest_summary = json.dumps(ctx.evaluation_summary or {}, ensure_ascii=False)
            decision_log = "\n".join(
                f"[{d.phase}] {d.action}: {d.reason}" for d in ctx.decisions[-10:]
            )

            prompt = RESEARCH_SUMMARY_PROMPT.format(
                goal=ctx.goal,
                region=ctx.region,
                universe=ctx.universe,
                delay=ctx.delay,
                dataset_ids=", ".join(ctx.dataset_ids),
                optimization_rounds=ctx.optimization_round,
                backtest_summary=backtest_summary,
                decision_log=decision_log,
            )

            summary = researcher._call_ai_with_history(
                [{"role": "user", "content": prompt}],
                json_mode=True,
            )

            if summary.get("overall_assessment"):
                print(f"\nAI 评价: {summary['overall_assessment']}")
            if summary.get("key_findings"):
                print(f"关键发现: {summary['key_findings']}")
            if summary.get("recommendations"):
                print(f"后续建议: {summary['recommendations']}")

        except Exception as e:
            logger.warning(f"AI 总结生成失败: {e}")

    def _extract_and_save_memory(self, ctx: ResearchContext):
        """从研究结果提取经验并保存到记忆"""
        # 保存研究记录
        best_sharpe = 0
        if ctx.evaluation_summary:
            best_sharpe = ctx.evaluation_summary.get("best_sharpe", 0)

        self.memory.save_research({
            "research_id": ctx.research_id,
            "goal": ctx.goal,
            "region": ctx.region,
            "dataset_ids": ctx.dataset_ids,
            "best_sharpe": best_sharpe,
            "optimization_rounds": ctx.optimization_round,
            "key_findings": ctx.evaluation_summary.get("summary", "") if ctx.evaluation_summary else "",
        })

        # 尝试用 AI 提取知识
        try:
            from ai.researcher_brain import AIResearcher
            researcher = AIResearcher()

            backtest_summary = json.dumps(ctx.evaluation_summary or {}, ensure_ascii=False)
            prompt = MEMORY_EXTRACTION_PROMPT.format(
                region=ctx.region,
                dataset_ids=", ".join(ctx.dataset_ids),
                backtest_summary=backtest_summary,
            )

            result = researcher._call_ai_with_history(
                [{"role": "user", "content": prompt}],
                json_mode=True,
            )

            for entry in result.get("knowledge_entries", []):
                self.memory.save_knowledge(entry)

            logger.info(f"已提取 {len(result.get('knowledge_entries', []))} 条知识")

        except Exception as e:
            logger.warning(f"知识提取失败: {e}")

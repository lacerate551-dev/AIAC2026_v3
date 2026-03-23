# -*- coding: utf-8 -*-
"""
Planner — 意图解析 + 执行计划生成
"""

import json
import logging
from typing import Dict, Any, Optional

from ai.agent.research_context import ResearchContext, ResearchPhase
from ai.agent.memory_store import MemoryStore
from ai.agent.tools import TOOL_DESCRIPTIONS
from ai.prompt_templates_agent import (
    INTENT_PARSE_PROMPT,
    PLAN_GENERATION_PROMPT,
    PROACTIVE_SUGGESTION_PROMPT,
)

logger = logging.getLogger(__name__)


class Planner:
    """意图解析 + 执行计划生成"""

    def __init__(self):
        self.memory = MemoryStore()

    def parse_intent(self, user_input: str, ctx: ResearchContext) -> Dict[str, Any]:
        """
        自然语言 → 结构化意图
        返回: {"intent": str, "params": dict, "confidence": float, "reasoning": str}
        """
        from ai.researcher_brain import AIResearcher
        import re

        researcher = AIResearcher()

        prompt = INTENT_PARSE_PROMPT.format(
            context_summary=ctx.get_status_summary(),
            user_input=user_input,
        )

        try:
            result = researcher._call_ai_with_history(
                [{"role": "user", "content": prompt}],
                system_prompt="",
                json_mode=True,
            )
            # 确保必要字段
            result.setdefault("intent", "GENERAL_QUESTION")
            result.setdefault("params", {})
            result.setdefault("confidence", 0.5)

            # Debug: 打印解析结果
            logger.info(f"意图解析结果: intent={result.get('intent')}, params={result.get('params')}, confidence={result.get('confidence')}")

            # 规则 fallback：如果 AI 没提取到 dataset_ids，用正则补救
            if result.get("intent") in ["START_RESEARCH", "MODIFY_DIRECTION"]:
                params = result.get("params", {})
                if not params.get("dataset_ids"):
                    # 正则提取数据集名称（常见模式：pv1, analyst15, fundamental17 等）
                    dataset_pattern = r'\b(pv\d+|analyst\d+|fundamental\d+|model\d+|shrt\d+[a-z_]*|[a-z]+\d+[a-z_]*)\b'
                    matches = re.findall(dataset_pattern, user_input.lower())
                    if matches:
                        params["dataset_ids"] = list(set(matches))  # 去重
                        result["params"] = params
                        logger.info(f"规则 fallback 提取到数据集: {params['dataset_ids']}")

            return result
        except Exception as e:
            logger.error(f"意图解析失败: {e}")
            return {
                "intent": "GENERAL_QUESTION",
                "params": {"question": user_input},
                "confidence": 0.3,
                "reasoning": f"解析失败，作为通用问题处理: {e}",
            }

    def generate_plan(self, ctx: ResearchContext) -> Dict[str, Any]:
        """
        根据当前状态 + 历史经验生成下一步计划
        返回: {"next_action": str, "action_params": dict, "reasoning": str, "user_message": str}
        """
        from ai.researcher_brain import AIResearcher

        # 获取记忆上下文
        memory_context = self.memory.get_context_for_planning(ctx.region, ctx.dataset_ids)

        # 工具描述
        tool_desc_lines = [f"- {name}: {desc}" for name, desc in TOOL_DESCRIPTIONS.items()]
        tool_desc_str = "\n".join(tool_desc_lines)

        prompt = PLAN_GENERATION_PROMPT.format(
            context_summary=ctx.get_status_summary(),
            memory_context=memory_context,
            tool_descriptions=tool_desc_str,
        )

        try:
            researcher = AIResearcher()
            result = researcher._call_ai_with_history(
                [{"role": "user", "content": prompt}],
                json_mode=True,
            )
            result.setdefault("next_action", self._default_next_action(ctx))
            result.setdefault("action_params", {})
            result.setdefault("reasoning", "")
            result.setdefault("user_message", "")
            return result
        except Exception as e:
            logger.error(f"计划生成失败: {e}")
            # 回退到规则驱动
            action = self._default_next_action(ctx)
            return {
                "next_action": action,
                "action_params": {},
                "reasoning": f"AI 规划失败，使用默认流程: {e}",
                "user_message": f"正在执行: {TOOL_DESCRIPTIONS.get(action, action)}",
            }

    def generate_suggestion(self, ctx: ResearchContext, completed_phase: str,
                            phase_result_summary: str) -> Dict[str, Any]:
        """阶段完成后生成主动建议"""
        from ai.researcher_brain import AIResearcher

        memory_context = self.memory.get_context_for_planning(ctx.region, ctx.dataset_ids)

        prompt = PROACTIVE_SUGGESTION_PROMPT.format(
            context_summary=ctx.get_status_summary(),
            completed_phase=completed_phase,
            phase_result_summary=phase_result_summary,
            memory_context=memory_context,
        )

        try:
            researcher = AIResearcher()
            result = researcher._call_ai_with_history(
                [{"role": "user", "content": prompt}],
                json_mode=True,
            )
            return result
        except Exception as e:
            logger.error(f"建议生成失败: {e}")
            return {
                "summary": "阶段已完成",
                "suggestions": [],
                "recommended_next": self._default_next_action(ctx),
                "concerns": [],
            }

    def _default_next_action(self, ctx: ResearchContext) -> str:
        """基于当前阶段的默认下一步（规则驱动 fallback）"""
        phase_to_action = {
            ResearchPhase.INIT: "analyze_dataset",
            ResearchPhase.DATASET_SELECTION: "analyze_dataset",
            ResearchPhase.DATA_ANALYSIS: "recommend_strategy",
            ResearchPhase.STRATEGY_RECOMMENDATION: "build_strategy",
            ResearchPhase.STRATEGY_BUILD: "generate_alphas",
            ResearchPhase.ALPHA_GENERATION: "run_backtest",
            ResearchPhase.BACKTEST: "optimize",
            ResearchPhase.EVALUATION: "optimize",
            ResearchPhase.OPTIMIZATION: "build_strategy",
        }
        return phase_to_action.get(ctx.current_phase, "query_memory")

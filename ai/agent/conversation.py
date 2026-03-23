# -*- coding: utf-8 -*-
"""
ConversationEngine — 对话式交互引擎
"""

import logging
from typing import Optional

from config.settings import REGION_DEFAULTS, MEMORY_DIR
from config.agent_config import AGENT_CONFIG
from ai.agent.research_context import ResearchContext, ResearchPhase
from ai.agent.agent_loop import AgentLoop
from ai.agent.planner import Planner

logger = logging.getLogger(__name__)


class ConversationEngine:
    """对话式研究交互引擎"""

    def __init__(self):
        self.planner = Planner()
        self.agent_loop = AgentLoop()
        self.ctx: Optional[ResearchContext] = None

    def start(self) -> str:
        """
        启动对话模式
        返回: "menu" 表示切回菜单模式, "exit" 表示退出
        """
        print("\n" + "=" * 60)
        print("  🤖 BRAIN 量化研究助手 (Agent 模式)")
        print("=" * 60)
        print("你好，我是 BRAIN 量化研究助手。告诉我你想研究什么？")
        print("提示: 输入 'menu' 切回菜单模式, 'quit' 退出\n")

        while True:
            try:
                user_input = input("\n你: ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\n\n👋 再见！")
                return "exit"

            if not user_input:
                continue
            if user_input.lower() == "menu":
                return "menu"
            if user_input.lower() in ("quit", "exit", "q"):
                print("\n👋 再见！")
                return "exit"
            if user_input.lower() == "status":
                self._show_status()
                continue

            response = self.process_input(user_input)
            print(f"\nAI: {response}")

    def process_input(self, user_input: str) -> str:
        """处理用户输入，返回 AI 回复"""
        # 初始化上下文（如果还没有）
        if self.ctx is None:
            self.ctx = ResearchContext()

        self.ctx.add_message("user", user_input)

        # 解析意图
        intent_result = self.planner.parse_intent(user_input, self.ctx)
        intent = intent_result.get("intent", "GENERAL_QUESTION")
        params = intent_result.get("params", {})

        logger.info(f"意图: {intent}, 置信度: {intent_result.get('confidence', 0)}")

        # 根据意图分发
        if intent == "START_RESEARCH":
            return self._handle_start_research(params)
        elif intent == "MODIFY_DIRECTION":
            return self._handle_modify(params)
        elif intent == "QUERY_STATUS":
            return self._handle_query_status()
        elif intent == "QUERY_MEMORY":
            return self._handle_query_memory(params)
        elif intent == "ADJUST_PARAMS":
            return self._handle_adjust_params(params)
        elif intent == "CONFIRM":
            return self._handle_confirm()
        elif intent == "CANCEL":
            return self._handle_cancel()
        else:
            return self._handle_general_question(params)

    def _handle_start_research(self, params: dict) -> str:
        """处理开始研究意图"""
        region = (params.get("region") or "USA").upper()
        dataset_ids = params.get("dataset_ids") or []
        goal = params.get("goal") or "量化因子挖掘"

        # 设置区域参数
        defaults = REGION_DEFAULTS.get(region, {"universe": "TOP3000", "delay": 1})

        self.ctx = ResearchContext(
            goal=goal,
            region=region,
            universe=defaults["universe"],
            delay=defaults["delay"],
            dataset_ids=dataset_ids,
            current_phase=ResearchPhase.INIT,
        )

        # 如果有数据集，直接开始
        if dataset_ids:
            print(f"\n📋 研究配置:")
            print(f"   目标: {goal}")
            print(f"   区域: {region} / {defaults['universe']} / delay={defaults['delay']}")
            print(f"   数据集: {', '.join(dataset_ids)}")

            confirm = input("\n确认开始? (Y/n): ").strip().lower()
            if confirm == "n":
                return "已取消。你可以调整参数后重新开始。"

            # 启动 Agent Loop
            self.ctx = self.agent_loop.run(self.ctx)
            return "研究流程已完成。输入 'status' 查看结果，或告诉我下一步想做什么。"
        else:
            return (
                f"好的，准备在 {region} 区域进行研究。\n"
                f"请告诉我要分析哪些数据集？例如：'分析 pv1 数据集' 或 '联合分析 pv1 和 analyst15'"
            )

    def _handle_modify(self, params: dict) -> str:
        """处理修改方向意图"""
        if self.ctx is None:
            return "当前没有进行中的研究。请先告诉我你想研究什么。"

        if params.get("strategy_focus"):
            self.ctx.strategy_config = self.ctx.strategy_config or {}
            self.ctx.strategy_config["focus"] = params["strategy_focus"]
            return f"已调整策略方向为: {params['strategy_focus']}。继续执行？"

        if params.get("dataset_ids"):
            self.ctx.dataset_ids = params["dataset_ids"]
            return f"已更新数据集为: {', '.join(params['dataset_ids'])}。继续执行？"

        return "请具体说明要修改什么？例如：'换成动量策略' 或 '改用 analyst15 数据集'"

    def _handle_query_status(self) -> str:
        """处理查询状态意图"""
        if self.ctx is None:
            return "当前没有进行中的研究。"
        return self.ctx.get_status_summary()

    def _handle_query_memory(self, params: dict) -> str:
        """处理查询记忆意图"""
        from ai.agent.memory_store import MemoryStore
        store = MemoryStore()
        region = params.get("region", self.ctx.region if self.ctx else "")
        context = store.get_context_for_planning(region)
        return f"历史经验:\n{context}"

    def _handle_adjust_params(self, params: dict) -> str:
        """处理参数调整意图"""
        if self.ctx is None:
            return "当前没有进行中的研究。"

        if params.get("region"):
            self.ctx.region = params["region"].upper()
            defaults = REGION_DEFAULTS.get(self.ctx.region, {"universe": "TOP3000", "delay": 1})
            self.ctx.universe = defaults["universe"]
            self.ctx.delay = defaults["delay"]
            return f"已切换到 {self.ctx.region} 区域 ({self.ctx.universe})"

        return "请具体说明要调整什么参数？"

    def _handle_confirm(self) -> str:
        """处理确认意图"""
        if self.ctx is None or self.ctx.current_phase == ResearchPhase.COMPLETED:
            return "当前没有待确认的操作。"

        # 继续执行 Agent Loop
        self.ctx = self.agent_loop.run(self.ctx)
        return "执行完成。输入 'status' 查看结果。"

    def _handle_cancel(self) -> str:
        """处理取消意图"""
        if self.ctx is None:
            return "当前没有进行中的研究。"
        self.ctx.add_decision(self.ctx.current_phase.value, "user_cancel", "用户主动取消")
        phase = self.ctx.current_phase.value
        self.ctx.current_phase = ResearchPhase.COMPLETED
        return f"已取消 {phase} 阶段的操作。你可以开始新的研究。"

    def _handle_general_question(self, params: dict) -> str:
        """处理通用问题"""
        question = params.get("question", "")
        return (
            f"这个问题我暂时无法直接回答。\n"
            f"我可以帮你：\n"
            f"  - 分析数据集（如 '分析 USA 的 pv1 数据集'）\n"
            f"  - 构建策略（如 '用动量策略挖掘 alpha'）\n"
            f"  - 查看历史经验（如 '之前 USA 的研究结果'）\n"
            f"  - 查看当前状态（输入 'status'）"
        )

    def _show_status(self):
        """显示当前状态"""
        if self.ctx is None:
            print("\nAI: 当前没有进行中的研究。")
        else:
            print(f"\nAI: {self.ctx.get_status_summary()}")


def start_agent_auto(region: str, dataset_ids: list, goal: str = "自动量化因子挖掘"):
    """全自动模式入口（CLI: python main.py auto USA pv1）"""
    defaults = REGION_DEFAULTS.get(region, {"universe": "TOP3000", "delay": 1})

    ctx = ResearchContext(
        goal=goal,
        region=region,
        universe=defaults["universe"],
        delay=defaults["delay"],
        dataset_ids=dataset_ids,
        current_phase=ResearchPhase.INIT,
    )

    loop = AgentLoop()
    ctx = loop.run(ctx)
    return ctx

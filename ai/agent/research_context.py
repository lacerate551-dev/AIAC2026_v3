# -*- coding: utf-8 -*-
"""
ResearchContext — 贯穿全流程的状态容器
"""

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any


class ResearchPhase(Enum):
    """研究阶段枚举"""
    INIT = "init"
    DATASET_SELECTION = "dataset_selection"
    DATA_ANALYSIS = "data_analysis"
    STRATEGY_RECOMMENDATION = "strategy_recommendation"
    STRATEGY_BUILD = "strategy_build"
    ALPHA_GENERATION = "alpha_generation"
    BACKTEST = "backtest"
    EVALUATION = "evaluation"
    OPTIMIZATION = "optimization"
    REPORT = "report"
    COMPLETED = "completed"


@dataclass
class Decision:
    """决策记录"""
    phase: str
    action: str
    reason: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "Decision":
        return cls(**d)


@dataclass
class ResearchContext:
    """贯穿全流程的研究状态容器"""

    # 基础标识
    research_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    goal: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

    # 区域参数
    region: str = "USA"
    universe: str = "TOP3000"
    delay: int = 1

    # 数据集
    dataset_ids: List[str] = field(default_factory=list)

    # 当前阶段
    current_phase: ResearchPhase = ResearchPhase.INIT

    # 各阶段产出
    analysis_result: Optional[Dict[str, Any]] = None
    strategy_config: Optional[Dict[str, Any]] = None
    alpha_configs: Optional[List[Dict[str, Any]]] = None
    backtest_results: Optional[List[Dict[str, Any]]] = None
    evaluation_summary: Optional[Dict[str, Any]] = None

    # 优化轮次
    optimization_round: int = 0

    # 决策日志
    decisions: List[Decision] = field(default_factory=list)

    # 对话历史（滑动窗口）
    conversation_history: List[Dict[str, str]] = field(default_factory=list)

    def add_decision(self, phase: str, action: str, reason: str):
        self.decisions.append(Decision(phase=phase, action=action, reason=reason))

    def add_message(self, role: str, content: str):
        from config.agent_config import AGENT_CONFIG
        self.conversation_history.append({"role": role, "content": content})
        limit = AGENT_CONFIG["conversation_history_limit"]
        if len(self.conversation_history) > limit:
            self.conversation_history = self.conversation_history[-limit:]

    def to_json(self) -> str:
        d = {
            "research_id": self.research_id,
            "goal": self.goal,
            "created_at": self.created_at,
            "region": self.region,
            "universe": self.universe,
            "delay": self.delay,
            "dataset_ids": self.dataset_ids,
            "current_phase": self.current_phase.value,
            "analysis_result": self.analysis_result,
            "strategy_config": self.strategy_config,
            "alpha_configs": self.alpha_configs,
            "backtest_results": self.backtest_results,
            "evaluation_summary": self.evaluation_summary,
            "optimization_round": self.optimization_round,
            "decisions": [d.to_dict() for d in self.decisions],
            "conversation_history": self.conversation_history,
        }
        return json.dumps(d, ensure_ascii=False, indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> "ResearchContext":
        d = json.loads(json_str)
        ctx = cls(
            research_id=d["research_id"],
            goal=d["goal"],
            created_at=d.get("created_at", ""),
            region=d["region"],
            universe=d["universe"],
            delay=d["delay"],
            dataset_ids=d.get("dataset_ids", []),
            current_phase=ResearchPhase(d["current_phase"]),
            analysis_result=d.get("analysis_result"),
            strategy_config=d.get("strategy_config"),
            alpha_configs=d.get("alpha_configs"),
            backtest_results=d.get("backtest_results"),
            evaluation_summary=d.get("evaluation_summary"),
            optimization_round=d.get("optimization_round", 0),
            decisions=[Decision.from_dict(x) for x in d.get("decisions", [])],
            conversation_history=d.get("conversation_history", []),
        )
        return ctx

    def save(self, directory: Path):
        directory.mkdir(parents=True, exist_ok=True)
        filepath = directory / f"context_{self.research_id}.json"
        filepath.write_text(self.to_json(), encoding="utf-8")
        return filepath

    @classmethod
    def load(cls, filepath: Path) -> "ResearchContext":
        return cls.from_json(filepath.read_text(encoding="utf-8"))

    def get_status_summary(self) -> str:
        """返回当前状态的简要摘要"""
        lines = [
            f"研究ID: {self.research_id}",
            f"目标: {self.goal}",
            f"区域: {self.region} / {self.universe} / delay={self.delay}",
            f"数据集: {', '.join(self.dataset_ids) if self.dataset_ids else '未选择'}",
            f"当前阶段: {self.current_phase.value}",
            f"优化轮次: {self.optimization_round}",
        ]
        if self.backtest_results:
            n_total = len(self.backtest_results)
            n_success = sum(1 for r in self.backtest_results if r.get("success"))
            lines.append(f"回测结果: {n_success}/{n_total} 成功")
        return "\n".join(lines)

# -*- coding: utf-8 -*-
"""
Agent 模式配置常量
"""

# Agent 主循环配置
AGENT_CONFIG = {
    "max_optimization_rounds": 3,       # 最大优化轮次
    "conversation_history_limit": 10,   # 对话历史滑动窗口
    "auto_confirm_phases": ["alpha_generation"],
    "require_confirm_phases": ["strategy_build", "optimization"],
}

# 各阶段干预策略
INTERVENTION_POLICY = {
    "data_analysis": "auto_with_review",     # 自动执行，展示结果供审阅
    "strategy_build": "require_confirm",     # 需要用户确认
    "alpha_generation": "auto",              # 全自动
    "backtest": "auto_with_review",          # 自动执行，展示结果
    "optimization": "require_confirm",       # 需要用户确认
}

# 记忆系统配置
MEMORY_CONFIG = {
    "max_research_records": 100,    # 最多保存的研究记录数
    "max_knowledge_entries": 200,   # 最多保存的知识条目数
}

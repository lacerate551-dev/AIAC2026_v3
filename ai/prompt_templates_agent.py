# -*- coding: utf-8 -*-
"""
Agent 专用 Prompt 模板
"""

# ==================== 意图解析 ====================
INTENT_PARSE_PROMPT = """你是 BRAIN 量化研究助手的意图解析器。
请分析用户输入，返回结构化意图。

当前研究状态：
{context_summary}

用户输入：{user_input}

**意图类型判断规则：**

1. **START_RESEARCH** - 用户想开始新的研究
   - 关键词：分析、研究、挖掘、开始、试试
   - 示例："分析 pv1 数据集"、"研究 USA 市场的 analyst15"、"联合分析 pv1 和 analyst15"
   - 提取：region（区域）、dataset_ids（数据集列表）、goal（研究目标）

2. **MODIFY_DIRECTION** - 用户想修改当前研究的方向或参数（仅在已有研究上下文时）
   - 关键词：换成、改用、调整、修改
   - 示例："换成动量策略"、"改用 analyst15 数据集"
   - 提取：strategy_focus（策略方向）或 dataset_ids（新数据集）

3. **CONFIRM** - 用户确认继续执行
   - 关键词：继续、确认、好的、是的、开始
   - 示例："继续执行"、"确认"、"好的"

4. **QUERY_STATUS** - 查询当前状态
   - 关键词：状态、进度、结果
   - 示例："当前状态"、"进展如何"

5. **QUERY_MEMORY** - 查询历史经验
   - 关键词：之前、历史、经验
   - 示例："之前 USA 的研究结果"

6. **CANCEL** - 取消当前操作
   - 关键词：取消、停止、退出
   - 示例："取消"、"停止"

7. **GENERAL_QUESTION** - 其他问题
   - 无法归类到以上意图的输入

**重要：**
- 如果用户提到具体数据集名称（如 pv1、analyst15），且没有明确说"修改"，应判断为 START_RESEARCH
- 如果当前没有研究上下文（current_phase=init），MODIFY_DIRECTION 不适用，应判断为 START_RESEARCH

请返回 JSON：
{{
    "intent": "START_RESEARCH | MODIFY_DIRECTION | QUERY_STATUS | QUERY_MEMORY | ADJUST_PARAMS | CONFIRM | CANCEL | GENERAL_QUESTION",
    "params": {{
        "region": "区域代码（如有）",
        "dataset_ids": ["数据集ID列表（如有）"],
        "goal": "研究目标描述（如有）",
        "strategy_focus": "策略方向（如有）",
        "question": "用户问题（如果是 GENERAL_QUESTION）"
    }},
    "confidence": 0.0-1.0,
    "reasoning": "解析理由"
}}
"""

# ==================== 计划生成 ====================
PLAN_GENERATION_PROMPT = """你是 BRAIN 量化研究助手的规划器。
根据当前研究状态和历史经验，决定下一步行动。

当前研究状态：
{context_summary}

历史经验：
{memory_context}

可用工具：
{tool_descriptions}

请返回 JSON：
{{
    "next_action": "工具名称",
    "action_params": {{}},
    "reasoning": "为什么选择这个行动",
    "user_message": "向用户展示的说明（中文）"
}}
"""

# ==================== 主动建议 ====================
PROACTIVE_SUGGESTION_PROMPT = """你是 BRAIN 量化研究助手。
当前阶段刚完成，请基于结果给出主动建议。

当前研究状态：
{context_summary}

刚完成的阶段：{completed_phase}
阶段结果摘要：{phase_result_summary}

历史经验：
{memory_context}

请返回 JSON：
{{
    "summary": "结果简要总结（中文，2-3句话）",
    "suggestions": ["建议1", "建议2", "建议3"],
    "recommended_next": "推荐的下一步行动",
    "concerns": ["潜在风险或注意事项"]
}}
"""

# ==================== 研究总结 ====================
RESEARCH_SUMMARY_PROMPT = """你是 BRAIN 量化研究助手。
请对本次研究进行全面总结。

研究目标：{goal}
区域：{region} / {universe} / delay={delay}
数据集：{dataset_ids}
优化轮次：{optimization_rounds}

回测结果摘要：
{backtest_summary}

决策日志：
{decision_log}

请返回 JSON：
{{
    "overall_assessment": "总体评价",
    "best_alphas": ["最佳 Alpha 表达式列表"],
    "key_findings": "关键发现",
    "failed_patterns": "失败模式总结",
    "recommendations": "后续建议"
}}
"""

# ==================== 记忆提取 ====================
MEMORY_EXTRACTION_PROMPT = """你是 BRAIN 量化研究助手的知识提取器。
请从本次研究结果中提取可复用的经验知识。

研究区域：{region}
数据集：{dataset_ids}
回测结果摘要：
{backtest_summary}

请返回 JSON：
{{
    "knowledge_entries": [
        {{
            "category": "effective_combo | operator_trap | field_insight | region_experience",
            "content": "经验描述",
            "region": "{region}",
            "confidence": 0.0-1.0
        }}
    ]
}}
"""

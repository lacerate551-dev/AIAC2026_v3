# -*- coding: utf-8 -*-
"""
Prompt 模板库
存储数据分析、策略构建、闭环优化的 Prompt 模板
"""

# ==================== 数据分析 Prompt ====================
DATA_ANALYSIS_PROMPT = """
你是一位资深量化研究员。请深度分析以下数据集，不仅要筛选字段，更要挖掘字段间的深层关系。

**数据集信息：**
- Region: {region}
- Dataset: {dataset_id}
- 可用字段：
{fields_metadata}

- 可用操作符：
{operators_metadata}

**字段类型说明（重要 - 经实测验证 2026-03-19）：**
- `MATRIX`: 数值时间序列字段，可直接用于 ts_mean/ts_delta/rank/zscore/scale 等操作符
- `VECTOR`: 事件型数据（event data），**需要先使用 vec_sum/vec_avg/vec_count 等操作符转换为 vector 后才能使用 rank/ts_mean 等**
  - 错误用法：`rank(vector_field)` → 报错 "does not support event inputs"
  - 正确用法：`rank(vec_sum(vector_field))` 或 `rank(vec_avg(vector_field))`
- `GROUP`: 分组字段（如 industry, sector），仅用于 group_neutralize 的第二参数
- `SYMBOL`: 标识符字段（如 ticker, cusip），不能用于数值计算

**深度思考要求：**

1. **多维相关性推演**
   - 分析字段组合（如 A/B, A-B, ts_corr(A, B, 10)）能否消除行业偏差或规模偏差
   - 识别互补字段（如价格 + 成交量，基本面 + 情绪）
   - 考虑字段的时序特性（是否需要 ts_delta, ts_rank）

2. **因果逻辑校验**
   - 对每个字段执行"反直觉检查"：如果该字段与预期相反，可能的原因是什么？
   - 是否需要引入 delay（延迟效应）或 diff（差分消除趋势）？
   - 是否存在非线性关系（需要 rank 或 log 变换）？

3. **拒绝平庸**
   - 严禁只推荐简单的 rank(field)
   - 必须包含交叉项（Cross-sectional，如 rank, group_rank）
   - 必须包含时序项（Time-series，如 ts_delta, ts_mean, ts_corr）
   - 鼓励复合操作符（如 rank(ts_delta(...)), ts_corr(rank(...), ...)）

4. **数据覆盖度约束（硬性要求）**
   - 每个字段附带了 coverage（数据覆盖率）和 dateCoverage（时间覆盖率）
   - 严禁推荐 coverage < 0.3 的字段
   - coverage < 0.5 的字段必须说明风险并建议搭配高覆盖率字段"中和"
   - dateCoverage < 0.3 的字段需特别标注"时间跨度不足"

5. **操作符合规性（硬性要求）**
   - 只能使用平台可用操作符列表中的操作符
   - 严禁使用 `correlation`（正确写法：`ts_corr`）
   - 严禁使用 `decay_linear`（正确写法：`ts_decay_linear`）
   - 严禁使用 `std_dev`（正确写法：`ts_std_dev`）
   - 严禁使用 `delay`（正确写法：`ts_delay`）
   - **VECTOR 类型字段必须先使用 vec_sum/vec_avg/vec_count 转换**

**任务要求：**
1. 筛选 5-8 个核心字段（不是固定 5 个）
2. 每个字段必须说明：
   - field_name: 字段名称
   - field_type: 字段的金融语义类型（Price/Volume/Returns/Fundamental/Technical/Sentiment）
   - data_type: 字段的平台数据类型（MATRIX/VECTOR/GROUP 等，直接从输入字段的 type 字段复制）
   - logic: 金融逻辑（为什么这个字段有价值，50 字以内）
   - expected_direction: 预期方向（positive/negative/neutral）
   - suggested_operators: 建议的操作符组合（2-3 个，如果是 VECTOR 类型必须包含 vec_*）
3. 推荐 8-12 个适合这些字段的操作符（从可用操作符中选择）
4. **新增**：推荐 3-5 个字段组合（如 "close/volume", "returns - ts_mean(returns, 20)"）

**输出格式（必须为 JSON）：**
{{
    "core_fields": [
        {{
            "field_name": "close",
            "field_type": "Price",
            "data_type": "MATRIX",
            "logic": "收盘价，适合构建价格动量策略",
            "expected_direction": "positive",
            "suggested_operators": ["rank", "ts_delta", "ts_rank"],
            "coverage": 0.95,
            "dateCoverage": 0.99
        }},
        {{
            "field_name": "anl16_1scermun",
            "field_type": "Analyst",
            "data_type": "VECTOR",
            "logic": "分析师推荐数量，需要 vec_sum 转换后使用",
            "expected_direction": "positive",
            "suggested_operators": ["vec_sum", "rank", "ts_delta"],
            "coverage": 0.57,
            "dateCoverage": 1.0
        }},
        ...（至少 5 个，最多 8 个）
    ],
    "available_operators": ["rank", "ts_delta", "ts_mean", "ts_corr", "ts_rank", "ts_decay_linear", "ts_arg_max", "ts_arg_min", "vec_sum", "vec_avg", "vec_count"],
    "field_combinations": [
        {{
            "combination": "close / volume",
            "logic": "价格/成交量比率，反映单位成交量的价格变化",
            "type": "ratio"
        }},
        {{
            "combination": "returns - ts_mean(returns, 20)",
            "logic": "收益率偏离均值，捕捉短期异常波动",
            "type": "deviation"
        }},
        {{
            "combination": "ts_corr(close, volume, 10)",
            "logic": "价量相关性，识别价量背离信号",
            "type": "correlation"
        }},
        ...（3-5 个）
    ]
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

# ==================== 回测参数推荐 Prompt ====================
BACKTEST_PARAMS_RECOMMENDATION_PROMPT = """
你是一位资深量化回测专家。请根据以下信息，推荐最优的回测参数配置。

**策略信息：**
{strategy_config}

**数据分析结果：**
{analysis_result}

**区域信息：**
- Region: {region}
- Universe: {universe}
- Delay: {delay}

**可用的回测参数：**

1. **decay（衰减系数）**：控制 Alpha 信号的衰减速度
   - 范围：0-10
   - 短期策略（窗口期 < 10 日）：建议 1-3
   - 中期策略（窗口期 10-30 日）：建议 3-7
   - 长期策略（窗口期 > 30 日）：建议 7-10
   - 说明：decay 越小，信号衰减越快，适合高频策略

2. **truncation（截断比例）**：控制极端值的截断
   - 范围：0.01-0.10
   - 常用值：0.08（截断前后 8% 的极端值）
   - 波动率高的策略：建议 0.05-0.08
   - 稳定的策略：建议 0.08-0.10

3. **neutralization（中性化方式）**：控制风险暴露
   - MARKET：市场中性（适合短期、高频策略）
   - INDUSTRY：行业中性（适合中长期、价值策略）
   - SUBINDUSTRY：子行业中性（适合精细化策略）
   - SECTOR：板块中性（适合宏观策略）

4. **pasteurization（巴氏消毒）**：防止未来信息泄露
   - ON：开启（推荐，防止前视偏差）
   - OFF：关闭（仅在确认无前视偏差时使用）

5. **unit_handling（单位处理）**：处理不同单位的数据
   - VERIFY：验证单位一致性（推荐）
   - IGNORE：忽略单位差异（谨慎使用）

6. **nan_handling（缺失值处理）**：处理 NaN 值
   - OFF：不处理（推荐，保持数据真实性）
   - ON：填充缺失值（可能引入偏差）

**任务要求：**

1. **分析策略特征**
   - 策略类型：短期/中期/长期？
   - 窗口期范围：最小和最大窗口期是多少？
   - 操作符类型：时序（TS）还是截面（CS）为主？
   - 复杂度：简单/中等/复杂？

2. **分析数据特征**
   - 数据频率：日频/周频/月频？
   - 数据波动性：高/中/低？
   - 缺失值情况：多/中/少？
   - 行业分布：集中/分散？

3. **推荐回测参数**
   - 为每个参数推荐具体值
   - 说明推荐理由
   - 指出潜在风险
   - 提供备选方案

**输出格式（必须为 JSON）：**
{{
    "strategy_characteristics": {{
        "strategy_period": "短期",
        "window_range": "1-20 日",
        "operator_focus": "时序为主，截面为辅",
        "complexity": "中等"
    }},
    "data_characteristics": {{
        "data_frequency": "日频",
        "volatility": "中等",
        "missing_values": "少",
        "industry_distribution": "分散"
    }},
    "recommended_params": {{
        "decay": {{
            "value": 3,
            "reason": "策略窗口期为 1-20 日，属于短期策略，decay=3 可以快速响应信号变化",
            "alternative": [2, 4],
            "confidence": "高"
        }},
        "truncation": {{
            "value": 0.08,
            "reason": "数据波动性中等，使用标准的 0.08 截断比例可以有效控制极端值",
            "alternative": [0.05, 0.10],
            "confidence": "高"
        }},
        "neutralization": {{
            "value": "MARKET",
            "reason": "短期策略且使用时序操作符为主，市场中性可以降低市场风险暴露",
            "alternative": ["INDUSTRY"],
            "confidence": "中高"
        }},
        "pasteurization": {{
            "value": "ON",
            "reason": "使用时序操作符（ts_delta 等），必须开启巴氏消毒防止前视偏差",
            "alternative": [],
            "confidence": "高"
        }},
        "unit_handling": {{
            "value": "VERIFY",
            "reason": "价量数据单位不同（价格 vs 成交量），需要验证单位一致性",
            "alternative": [],
            "confidence": "高"
        }},
        "nan_handling": {{
            "value": "OFF",
            "reason": "保持数据真实性，不填充缺失值，避免引入偏差",
            "alternative": [],
            "confidence": "高"
        }}
    }},
    "risk_warnings": [
        "短期策略对交易成本敏感，需关注 Turnover 指标",
        "市场中性化可能降低收益，但提高稳定性",
        "建议先抽样回测验证参数有效性"
    ],
    "final_config": {{
        "decay": 3,
        "truncation": 0.08,
        "neutralization": "MARKET",
        "pasteurization": "ON",
        "unit_handling": "VERIFY",
        "nan_handling": "OFF"
    }}
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

# ==================== 策略方向推荐 Prompt ====================
STRATEGY_RECOMMENDATION_PROMPT = """
你是一位资深量化策略专家。请根据以下数据分析结果，推荐最适合的策略方向。

**数据分析结果：**
{analysis_result}

**任务要求：**

1. **深度分析数据特征**
   - 数据集类型：价量数据？基本面数据？情绪数据？技术指标？
   - 字段特性：高频还是低频？截面还是时序？绝对值还是相对值？
   - 数据质量：是否有明显的因果关系？是否适合构建 Alpha？

2. **推荐策略方向**
   - 根据数据特征，推荐 2-3 个最适合的策略方向
   - 每个方向说明：为什么适合？预期效果如何？潜在风险是什么？
   - 按推荐优先级排序（最推荐的放在第一位）

3. **策略方向选项**
   - 动量反转：适合价格、收益率等时序数据，捕捉趋势延续或反转
   - 价值因子：适合基本面数据，寻找被低估的资产
   - 价量背离：适合价格+成交量数据，识别供需失衡
   - 行业轮动：适合行业分类数据，捕捉板块轮动
   - 情绪反转：适合情绪、新闻数据，捕捉市场情绪极端
   - 质量因子：适合财务质量数据，筛选高质量公司
   - 波动率策略：适合波动率数据，捕捉波动率异常
   - 事件驱动：适合事件数据（财报、并购等），捕捉事件影响

**输出格式（必须为 JSON）：**
{{
    "data_characteristics": {{
        "dataset_type": "基本面数据",
        "field_frequency": "季度",
        "data_dimension": "截面为主",
        "causal_strength": "中等",
        "alpha_potential": "较高"
    }},
    "recommended_strategies": [
        {{
            "strategy_name": "价值因子",
            "priority": 1,
            "reason": "数据集包含 PE、PB、ROE 等估值和盈利指标，天然适合构建价值因子策略",
            "expected_effect": "Sharpe 预期 0.8-1.5，适合中长期持有",
            "potential_risk": "价值陷阱风险，需结合质量因子筛选",
            "confidence": "高"
        }},
        {{
            "strategy_name": "质量因子",
            "priority": 2,
            "reason": "ROE、资产负债率等指标可用于筛选高质量公司",
            "expected_effect": "Sharpe 预期 0.6-1.2，稳定性较高",
            "potential_risk": "高质量公司估值可能偏高",
            "confidence": "中"
        }},
        {{
            "strategy_name": "动量反转",
            "priority": 3,
            "reason": "基本面数据变化率可构建基本面动量策略",
            "expected_effect": "Sharpe 预期 0.5-1.0，需较长窗口期",
            "potential_risk": "基本面数据更新频率低，信号延迟",
            "confidence": "中"
        }}
    ],
    "auto_select": "价值因子"
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

# ==================== 策略构建 Prompt ====================
STRATEGY_BUILD_PROMPT = """
你是一位量化策略设计师。基于以下字段分析结果，设计一个可批量生成的 Alpha 策略模板组合。

**输入信息：**
- 核心字段：
{core_fields}

- 字段组合建议：
{field_combinations}

- 可用操作符：
{operators}

- 策略方向：{strategy_focus}

**深度思考要求：**

1. **多样化表达式设计（强制要求）**
   - 不要只生成一个模板，要设计 3-5 个不同类型的模板
   - 类型包括：纯时序型、截面比率型、混合型、衰减型、分组标准化型
   - 必须覆盖至少 3 种不同的结构类型
   - 严禁所有模板使用相同的外层操作符（如全部用 rank(...) 包裹）
   - 每个模板必须包含至少 2 层操作符嵌套

2. **字段组合利用**
   - 充分利用输入的 field_combinations
   - 设计字段交叉项（如 field1 * field2, field1 / field2）
   - 设计时序差值项（如 ts_delta(field1, w1) - ts_delta(field2, w2)）

3. **参数空间设计**
   - 窗口期范围要合理（短期 1-10，中期 10-30，长期 30-60）
   - 参数组合要避免冗余（如 [5, 10, 20] 比 [5, 6, 7, 8, 9, 10] 更好）
   - 预估生成数量应在 20-50 个之间（太少没意义，太多浪费资源）

4. **Self-Correction（自我修正）**
   - 在输出前，自我检查：这些模板是否过于相似？
   - 确保涵盖时序（TS）和截面（CS）两个维度
   - 至少包含 1 个纯时序模板（如 ts_delta, ts_mean）
   - 至少包含 1 个纯截面模板（如 rank, group_rank）
   - 至少包含 1 个混合模板（如 rank(ts_delta(...))）

5. **操作符合规性（硬性要求）**
   - 只能使用以下平台可用操作符：
     - 时序类: ts_delta, ts_mean, ts_median, ts_sum, ts_rank, ts_std_dev, ts_zscore, ts_corr, ts_covariance, ts_decay_linear, ts_decay_exp_window, ts_delay, ts_arg_max, ts_arg_min, ts_product, ts_scale, ts_av_diff
     - 截面类: rank, zscore, normalize, scale, quantile, truncate, winsorize, regression_neut
     - 分组类: group_rank, group_zscore, group_neutralize, group_normalize, group_mean, group_median
     - 算术类: abs, sign, log, sqrt, power, signed_power, inverse, s_log_1p
     - 逻辑类: if_else, is_nan, greater, less
     - 其他: trade_when, inst_tvr, max, min
   - 严禁使用 `correlation`（正确写法：`ts_corr`）
   - 严禁使用 `decay_linear`（正确写法：`ts_decay_linear`）
   - 严禁使用 `std_dev`/`delay`/`delta`/`mean`/`sum` 等缩写（必须加 `ts_` 前缀）

**任务要求：**
1. 设计 3-5 个表达式模板（不是 1 个）
2. 每个模板定义字段填充规则和参数范围
3. 生成一个 Python 脚本，遍历所有模板、字段组合、参数组合
4. 脚本必须输出 JSON 列表，每个元素包含 "expression", "template_type", "settings"

**Python 脚本要求：**
- **重要：不要使用 import 语句**（json、itertools、math 已预先注入，可直接使用）
- 使用 itertools.product 遍历所有组合
- 输出格式：print(json.dumps(alphas, ensure_ascii=False, indent=2))
- 必须为每个 Alpha 添加 template_type 标记（如 "momentum", "ratio", "correlation"）

**输出格式（必须为 JSON）：**
{{
    "strategy_name": "多维价量策略组合",
    "strategy_description": "基于价格、成交量的多维度策略，包含动量、比率、相关性等多种类型",
    "strategy_type": "short_term",
    "templates": [
        {{
            "template": "rank(ts_delta({{field1}}, {{window1}}))",
            "template_type": "momentum_ts",
            "description": "时序动量型：捕捉价格短期变化",
            "field_rules": {{
                "field1": {{"type": "Price", "candidates": ["close", "vwap"]}}
            }},
            "window_ranges": {{
                "window1": [5, 10, 20]
            }}
        }},
        {{
            "template": "rank({{field1}} / {{field2}})",
            "template_type": "ratio_cs",
            "description": "截面比率型：价格与成交量的相对关系",
            "field_rules": {{
                "field1": {{"type": "Price", "candidates": ["close", "vwap"]}},
                "field2": {{"type": "Volume", "candidates": ["volume", "adv20"]}}
            }},
            "window_ranges": {{}}
        }},
        {{
            "template": "rank(ts_corr(-ts_delta({{field1}}, {{window1}}), ts_delta({{field2}}, {{window2}}), {{window3}}))",
            "template_type": "correlation_mixed",
            "description": "混合相关性型：价量背离信号",
            "field_rules": {{
                "field1": {{"type": "Price", "candidates": ["close", "vwap"]}},
                "field2": {{"type": "Volume", "candidates": ["volume", "adv20"]}}
            }},
            "window_ranges": {{
                "window1": [1, 5],
                "window2": [1, 5],
                "window3": [10, 20]
            }}
        }},
        ...（3-5 个模板）
    ],
    "generation_script": "# 注意：不要使用 import，json/itertools/math 已预先注入\\n\\nalphas = []\\n\\n# 模板 1: 时序动量型\\nfor field1, w1 in itertools.product(['close', 'vwap'], [5, 10, 20]):\\n    expr = f'rank(ts_delta({{field1}}, {{w1}}))'\\n    alphas.append({{'expression': expr, 'template_type': 'momentum_ts', 'settings': {{'field1': field1, 'window1': w1}}}})\\n\\n# 模板 2: 截面比率型\\nfor field1, field2 in itertools.product(['close', 'vwap'], ['volume', 'adv20']):\\n    expr = f'rank({{field1}} / {{field2}})'\\n    alphas.append({{'expression': expr, 'template_type': 'ratio_cs', 'settings': {{'field1': field1, 'field2': field2}}}})\\n\\n# 模板 3: 混合相关性型\\nfor field1, field2, w1, w2, w3 in itertools.product(['close', 'vwap'], ['volume', 'adv20'], [1, 5], [1, 5], [10, 20]):\\n    expr = f'rank(ts_corr(-ts_delta({{field1}}, {{w1}}), ts_delta({{field2}}, {{w2}}), {{w3}}))'\\n    alphas.append({{'expression': expr, 'template_type': 'correlation_mixed', 'settings': {{'field1': field1, 'field2': field2, 'window1': w1, 'window2': w2, 'window3': w3}}}})\\n\\nprint(json.dumps(alphas, ensure_ascii=False, indent=2))",
    "backtest_params": {{
        "region": "{region}",
        "universe": "{universe}",
        "delay": {delay}
    }}
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

# ==================== 闭环优化 Prompt ====================
OPTIMIZATION_PROMPT = """
你是一位量化策略优化专家。请分析以下回测结果，找出问题并提出改进建议。

**原始策略：**
{original_strategy}

**回测统计摘要：**
- 总数：{total_count}，成功：{success_count}，失败：{fail_count}，Sharpe 为负：{negative_count}
- 成功率：{success_rate:.1%}
- Sharpe：均值 {sharpe_mean:.3f}，中位数 {sharpe_median:.3f}，最大 {sharpe_max:.3f}，最小 {sharpe_min:.3f}
- Fitness：均值 {fitness_mean:.3f}，中位数 {fitness_median:.3f}
- Turnover：均值 {turnover_mean:.3f}，中位数 {turnover_median:.3f}
- Returns：均值 {returns_mean:.3f}

**失败模式分类：**
{failure_pattern_summary}

**成功案例（Top {top_n}，按 Sharpe 排序）：**
{success_cases}

**失败案例（Bottom {bottom_n}，按 Sharpe 排序）：**
{failure_cases}

**按模板类型分组统计：**
{template_type_stats}

**操作符合规性约束（硬性要求）：**
- 只能使用以下平台可用操作符：
  - 时序类: ts_delta, ts_mean, ts_median, ts_sum, ts_rank, ts_std_dev, ts_zscore, ts_corr, ts_covariance, ts_decay_linear, ts_decay_exp_window, ts_delay, ts_arg_max, ts_arg_min, ts_product, ts_scale, ts_av_diff
  - 截面类: rank, zscore, normalize, scale, quantile, truncate, winsorize, regression_neut
  - 分组类: group_rank, group_zscore, group_neutralize, group_normalize, group_mean, group_median
  - 算术类: abs, sign, log, sqrt, power, signed_power, inverse, s_log_1p
  - 逻辑类: if_else, is_nan, greater, less
  - 其他: trade_when, inst_tvr, max, min
- 严禁使用 `correlation`（正确写法：`ts_corr`）
- 严禁使用 `decay_linear`（正确写法：`ts_decay_linear`）
- 严禁使用 `std_dev`/`delay`/`delta`/`mean`/`sum` 等缩写（必须加 `ts_` 前缀）

**任务要求：**
1. 分析整体表现（是否达到预期，Sharpe > 1.0, Fitness > 0.5）
2. 总结成功案例的共性（哪些字段/参数组合/模板类型效果好）
3. 分析失败原因，按失败模式分类：
   - 操作符错误（表达式语法问题）
   - 字段错误（字段不存在或不适合）
   - Sharpe 为负（策略方向可能反了）
   - Turnover 过高（换手率超标）
   - 其他
4. 提出 3-5 条优化建议，每条标注优先级（高/中/低）和预期改善效果
5. 输出改进后的策略配置，必须包含：
   - 3-5 个不同类型的模板（确保模板多样性，覆盖时序、截面、混合等类型）
   - 可执行的 generation_script（可直接喂给 StrategyGenerator）
   - 调整后的 backtest_params

**Python 脚本要求（generation_script）：**
- 不要使用 import 语句（json、itertools、math 已预先注入，可直接使用）
- 使用 itertools.product 遍历所有组合
- 输出格式：print(json.dumps(alphas, ensure_ascii=False, indent=2))
- 必须为每个 Alpha 添加 template_type 标记
- 预估生成数量应在 20-50 个之间

**输出格式（必须为 JSON）：**
{{
    "overall_analysis": "整体 Sharpe 均值 0.3，低于预期 1.0，主要问题是...",
    "success_cases_summary": "成功案例主要使用 close 和 volume 的 10-20 日窗口...",
    "failure_cases_summary": "失败案例多为长窗口期（> 30 日）或使用了低流动性字段...",
    "optimization_suggestions": [
        {{"suggestion": "缩短窗口期至 10 日以下", "priority": "高", "expected_improvement": "预计 Sharpe 提升 0.2-0.3"}},
        {{"suggestion": "引入 vwap 替代 close", "priority": "中", "expected_improvement": "预计降低 Turnover 5-10%"}},
        {{"suggestion": "增加 rank 操作符以降低 Turnover", "priority": "中", "expected_improvement": "预计 Turnover 降低 10%"}}
    ],
    "updated_strategy": {{
        "strategy_name": "优化后的策略名称",
        "strategy_description": "优化后的策略描述",
        "strategy_type": "short_term",
        "templates": [
            {{
                "template": "rank(ts_delta({{{{field1}}}}, {{{{window1}}}}))",
                "template_type": "momentum_ts",
                "description": "时序动量型",
                "field_rules": {{
                    "field1": {{"type": "Price", "candidates": ["close", "vwap"]}}
                }},
                "window_ranges": {{
                    "window1": [5, 10, 20]
                }}
            }}
        ],
        "generation_script": "alphas = []\\nfor field1, w1 in itertools.product(['close', 'vwap'], [5, 10, 20]):\\n    expr = f'rank(ts_delta({{field1}}, {{w1}}))' \\n    alphas.append({{'expression': expr, 'template_type': 'momentum_ts', 'settings': {{}}}})\\nprint(json.dumps(alphas, ensure_ascii=False, indent=2))",
        "backtest_params": {{
            "decay": 3,
            "truncation": 0.08,
            "neutralization": "MARKET"
        }}
    }}
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

# ==================== 多数据集联合分析 Prompt ====================
MULTI_DATASET_ANALYSIS_PROMPT = """
你是一位资深量化研究员。请深度分析以下多个数据集的字段，重点挖掘跨数据集的字段组合机会。

**区域信息：**
- Region: {region}
- 涉及数据集: {dataset_ids}

**各数据集字段（按来源分组）：**
{grouped_fields_metadata}

**可用操作符：**
{operators_metadata}

**字段类型说明（重要 - 经实测验证 2026-03-19）：**
- `MATRIX`: 数值时间序列字段，可直接用于 ts_mean/ts_delta/rank/zscore/scale 等操作符
- `VECTOR`: 事件型数据（event data），**需要先使用 vec_sum/vec_avg/vec_count 等操作符转换为 vector 后才能使用 rank/ts_mean 等**
  - 错误用法：`rank(vector_field)` → 报错 "does not support event inputs"
  - 正确用法：`rank(vec_sum(vector_field))` 或 `rank(vec_avg(vector_field))`
- `GROUP`: 分组字段（如 industry, sector），仅用于 group_neutralize 的第二参数
- `SYMBOL`: 标识符字段（如 ticker, cusip），不能用于数值计算

**信息维度识别要求：**
请先识别每个数据集的信息维度（价量数据/基本面数据/分析师预期/技术指标/情绪数据/模型预测），然后基于维度互补性设计跨数据集组合。

**深度思考要求：**

1. **跨数据集交叉分析（核心要求）**
   - 字段组合（field_combinations）中至少 50% 必须是跨数据集组合
   - 例如：ts_corr(close, anl15_bps_gr_12_m_1m_chg, 20) 将价量与分析师预期交叉
   - 寻找不同维度间的互补信号（如价量确认基本面变化）

2. **多维相关性推演**
   - 分析跨数据集字段组合能否消除单一维度的噪声
   - 识别互补字段对（如价格动量 + 盈利修正、成交量 + 分析师覆盖度）
   - 考虑字段的时序特性差异（日频价量 vs 季频基本面）

3. **因果逻辑校验**
   - 跨数据集组合是否有合理的金融逻辑支撑？
   - 不同频率数据混合时是否需要时序对齐（ts_mean 平滑低频数据）？

4. **数据覆盖度约束（硬性要求）**
   - 严禁推荐 coverage < 0.3 的字段
   - coverage < 0.5 的字段必须说明风险
   - 跨数据集组合时，两个字段的 coverage 都必须 >= 0.3

5. **操作符合规性（硬性要求）**
   - 只能使用平台可用操作符列表中的操作符
   - 严禁使用 `correlation`（正确写法：`ts_corr`）
   - 严禁使用 `decay_linear`（正确写法：`ts_decay_linear`）
   - 严禁使用 `std_dev`（正确写法：`ts_std_dev`）
   - 严禁使用 `delay`（正确写法：`ts_delay`）
   - **VECTOR 类型字段必须先使用 vec_sum/vec_avg/vec_count 转换**

**任务要求：**
1. 筛选 6-10 个核心字段（必须覆盖所有输入数据集，每个数据集至少 2 个）
2. 每个字段必须标注 source_dataset（来源数据集）
3. 推荐 8-12 个适合这些字段的操作符（如果涉及 VECTOR 类型必须包含 vec_*）
4. 推荐 4-6 个字段组合，其中至少 50% 为跨数据集组合
5. 输出 dataset_dimensions 标注每个数据集的信息维度

**输出格式（必须为 JSON）：**
{{
    "dataset_dimensions": {{
        "pv1": "价量数据",
        "analyst15": "分析师预期数据"
    }},
    "core_fields": [
        {{
            "field_name": "close",
            "field_type": "Price",
            "data_type": "MATRIX",
            "source_dataset": "pv1",
            "logic": "收盘价，适合构建价格动量策略",
            "expected_direction": "positive",
            "suggested_operators": ["rank", "ts_delta", "ts_rank"],
            "coverage": 0.95,
            "dateCoverage": 0.99
        }},
        {{
            "field_name": "anl15_bps_gr_12_m_1m_chg",
            "field_type": "Sentiment",
            "data_type": "MATRIX",
            "source_dataset": "analyst15",
            "logic": "分析师盈利预期变化，捕捉市场情绪转向",
            "expected_direction": "positive",
            "suggested_operators": ["rank", "ts_delta", "ts_corr"],
            "coverage": 0.80,
            "dateCoverage": 0.85
        }}
    ],
    "available_operators": ["rank", "ts_delta", "ts_mean", "ts_corr", "ts_rank", "ts_decay_linear", "ts_std_dev", "ts_zscore", "vec_sum", "vec_avg", "vec_count"],
    "field_combinations": [
        {{
            "combination": "ts_corr(close, anl15_bps_gr_12_m_1m_chg, 20)",
            "logic": "价格与分析师盈利预期变化的相关性，捕捉基本面驱动的价格趋势",
            "type": "cross_dataset_correlation",
            "cross_dataset": true
        }},
        {{
            "combination": "rank(ts_delta(close, 5)) - rank(ts_delta(anl15_bps_gr_12_m_1m_chg, 5))",
            "logic": "价格动量与盈利修正动量的背离，寻找预期差",
            "type": "cross_dataset_divergence",
            "cross_dataset": true
        }},
        {{
            "combination": "close / volume",
            "logic": "价格/成交量比率，反映单位成交量的价格变化",
            "type": "ratio",
            "cross_dataset": false
        }}
    ]
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

# ==================== 错误修复 Prompt ====================
ERROR_FIX_PROMPT = """
你是一位量化策略修复专家。请根据以下错误信息，修复失败的 Alpha 表达式。

**失败的表达式：**
{failed_expression}

**错误信息：**
{error_message}

**错误类型：**
{error_type}

**受影响的实体：**
{affected_entity}

**可用的替代方案：**
{suggested_alternatives}

**任务要求：**

1. **深度分析错误原因**
   - 为什么这个表达式会失败？
   - 是字段不存在？操作符参数错误？还是配置冲突？

2. **选择最佳替代方案**
   - 从可用的替代方案中选择最合适的
   - 确保替代方案在语义上与原字段/操作符相似
   - 如果是字段错误，优先选择相同类型的字段（Price vs Price, Volume vs Volume）

3. **重构表达式**
   - 替换受影响的实体
   - 保持表达式的整体逻辑不变
   - 确保新表达式语法正确

4. **验证修复**
   - 检查新表达式是否符合平台规范
   - 确认所有字段和操作符都有效

**输出格式（必须为 JSON）：**
{{
    "error_analysis": "字段 'fnd13_xxx' 不存在于数据集中，可能是字段名拼写错误或该字段已被平台移除",
    "selected_alternative": "fnd13_yyy",
    "selection_reason": "fnd13_yyy 与 fnd13_xxx 最相似，且都属于基本面数据类型",
    "fixed_expression": "rank(ts_delta(fnd13_yyy, 5))",
    "changes_made": "将字段 'fnd13_xxx' 替换为 'fnd13_yyy'",
    "confidence": 0.85,
    "risk_warning": "替代字段的金融含义可能略有不同，建议验证回测结果"
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""


DATASET_CONFIG_GENERATION_PROMPT = """
你是一位资深量化研究员。请深度分析以下数据集，为其生成研究方向和 Alpha 模板配置。

**数据集信息：**
- Region: {region}
- Dataset ID: {dataset_id}
- Dataset Name: {dataset_name}
- Dataset Description: {dataset_description}

**字段列表（共 {total_fields} 个）：**
{fields_metadata}

**可用操作符：**
{operators_metadata}

**字段类型说明（重要）：**
- `MATRIX`: 数值时间序列字段，可直接用于 ts_mean/ts_delta/rank/zscore/scale 等操作符
- `VECTOR`: 事件型数据（event data），**需要先使用 vec_sum/vec_avg/vec_count 等操作符转换为 vector 后才能使用 rank/ts_mean 等**
  - 错误用法：`rank(vector_field)` → 报错 "does not support event inputs"
  - 正确用法：`rank(vec_sum(vector_field))` 或 `rank(vec_avg(vector_field))`
- `GROUP`: 分组字段（如 industry, sector），仅用于 group_neutralize 的第二参数
- `SYMBOL`: 标识符字段（如 ticker, cusip），不能用于数值计算

**任务要求：**

## 任务 1：数据集特征分析
请分析数据集的整体特征：
- 数据类型：价量数据/基本面数据/分析师预期/技术指标/情绪数据/模型预测/其他
- 更新频率：日频/周频/月频/季度/年度/实时
- 数据维度：截面为主/时序为主/混合
- Alpha 潜力：高/中/低，并说明理由

## 任务 2：研究方向生成（{num_directions} 个）
请为该数据集设计 {num_directions} 个研究方向，每个方向包含：
- name: 方向名称（简洁，如"预期修正信号"）
- description: 方向描述（50 字以内，说明核心逻辑）
- field_patterns: 字段模式列表（如 ["*_flag", "*_value"]，用于匹配字段）
- example_fields: 示例字段（2-3 个该方向的核心字段）
- suggested_operators: 建议的操作符（3-5 个）
- alpha_logic: Alpha 构建逻辑（如何使用这些字段构建 Alpha）

## 任务 3：优先字段推荐（10-15 个）
请推荐最有价值的核心字段，每个字段包含：
- field_id: 字段 ID
- field_type: 金融语义类型（Price/Volume/Returns/Fundamental/Technical/Sentiment/Analyst/Model/Other）
- data_type: 平台数据类型（MATRIX/VECTOR/GROUP/SYMBOL）
- coverage: 覆盖率（如有）
- logic: 金融逻辑（30 字以内）
- priority: 优先级（1-5，1 最高）

## 任务 4：字段配对推荐（5-8 对）
推荐适合配对使用的字段组合：
- field1, field2: 配对字段
- logic: 配对逻辑

## 任务 5：模板生成（{num_templates} 个）
请生成 {num_templates} 个 Alpha 模板，要求：
- 覆盖时序型（time_series）、截面型（cross_section）、配对型（pair）、复杂型（complex）等多种类型
- 每个模板包含：name, expression, description, fields_required, field_types, field_hints, operators, category
- 表达式中使用 {{field}}, {{field1}}, {{field2}}, {{window}} 等占位符
- field_hints 使用通配符模式匹配字段（如 "*_high", "mdl250_eq_*"）

**输出格式（必须为 JSON）：**
{{
    "dataset_characteristics": {{
        "data_category": "分析师预期数据",
        "update_frequency": "季度",
        "data_dimension": "截面为主",
        "alpha_potential": "高",
        "alpha_potential_reason": "分析师预期变化对股价有显著影响"
    }},
    "research_directions": [
        {{
            "name": "预期修正信号",
            "description": "分析师预测的变化趋势，捕捉预期修正方向",
            "field_patterns": ["*_flag", "*_number"],
            "example_fields": ["anl4_epsr_flag", "anl4_netprofit_flag"],
            "suggested_operators": ["ts_delta", "rank", "ts_mean"],
            "alpha_logic": "ts_delta(flag_field, window) 捕捉修正方向"
        }}
    ],
    "priority_fields": [
        {{
            "field_id": "anl4_adjusted_netincome_ft",
            "field_type": "Fundamental",
            "data_type": "MATRIX",
            "coverage": 0.871,
            "logic": "调整后净利润，覆盖率最高",
            "priority": 1
        }}
    ],
    "field_pairs": [
        {{
            "field1": "anl4_capex_high",
            "field2": "anl4_capex_low",
            "logic": "资本支出预测分歧"
        }}
    ],
    "templates": [
        {{
            "name": "estimate_revision_momentum",
            "expression": "rank(ts_delta({{field}}, {{window}}))",
            "description": "预期修正动量：分析师预测变化方向",
            "fields_required": 1,
            "field_types": [["vector"]],
            "field_hints": {{"field": "*_flag"}},
            "operators": ["rank", "ts_delta"],
            "category": "time_series"
        }}
    ],
    "guidance_prompt": "**{dataset_id} 数据集研究方向指引（重要）：**\\n此数据集为..."
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

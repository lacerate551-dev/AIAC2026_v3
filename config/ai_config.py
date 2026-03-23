# -*- coding: utf-8 -*-
"""
AI 模型配置文件
存储模型参数、策略生成配置、回测参数映射规则
"""

# ==================== AI 模型配置 ====================
# 注意：所有模型都使用 OpenAI SDK 调用（兼容格式）
# 具体的 api_key, base_url, model 从 config/api_keys.json 动态加载

AI_MODELS = {
    "claude": {
        "display_name": "Claude 3.5 Sonnet",
        "temperature": 0.7,
        "max_tokens": 8000,
        "description": "Anthropic Claude 模型（通过 codeflow.asia 中转）"
    },
    "codex": {
        "display_name": "GPT-5.3 Codex",
        "temperature": 0.7,
        "max_tokens": 4000,
        "description": "OpenAI Codex 模型（通过 codeflow.asia 中转）"
    },
    "deepseek": {
        "display_name": "DeepSeek Chat",
        "temperature": 0.7,
        "max_tokens":  8192,
        "description": "DeepSeek 模型（通过硅基流动）"
    }
}

# 当前使用的模型提供商（可随时切换）
# 注意：实际使用时会从 api_keys.json 的 current_provider 字段读取
DEFAULT_PROVIDER = "claude"

# ==================== 策略生成配置 ====================
STRATEGY_CONFIG = {
    "max_fields_per_analysis": 5,      # AI 每次分析输出的核心字段数
    "default_gen_count": 500,          # 单次生成的 Alpha 数量（可调整）
    "sample_backtest_ratio": 0.2,      # 抽样回测比例（20%）
    "top_n_success": 3,                # 闭环分析的成功案例数
    "bottom_n_failure": 3,             # 闭环分析的失败案例数
    "max_iterations": 3                # 闭环优化最大迭代次数
}

# ==================== 回测参数映射规则 ====================
# 根据策略类型自动匹配回测参数
STRATEGY_TYPE_PARAMS = {
    "short_term": {
        "decay": 3,
        "truncation": 0.08,
        "neutralization": "MARKET",
        "description": "短期策略（窗口期 < 10 日）"
    },
    "medium_term": {
        "decay": 5,
        "truncation": 0.08,
        "neutralization": "INDUSTRY",
        "description": "中期策略（窗口期 10-30 日）"
    },
    "long_term": {
        "decay": 10,
        "truncation": 0.08,
        "neutralization": "INDUSTRY",
        "description": "长期策略（窗口期 > 30 日）"
    },
    "value": {
        "decay": 7,
        "truncation": 0.08,
        "neutralization": "INDUSTRY",
        "description": "价值因子策略"
    },
    "momentum": {
        "decay": 4,
        "truncation": 0.08,
        "neutralization": "MARKET",
        "description": "动量策略"
    }
}

# ==================== 字段类型定义 ====================
# 用于验证字段填充的合法性
FIELD_TYPES = {
    "Price": ["close", "open", "high", "low", "vwap"],
    "Volume": ["volume", "adv5", "adv10", "adv20", "adv60"],
    "Returns": ["returns", "log_returns"],
    "Fundamental": ["market_cap", "pe_ratio", "pb_ratio"],
    "Technical": ["rsi", "macd", "bollinger"]
}

# ==================== API 调用配置 ====================
API_CONFIG = {
    "max_retries": 3,           # API 调用失败最大重试次数
    "retry_delay": 2,           # 重试延迟（秒）
    "timeout": 60,              # 请求超时时间（秒）
    "enable_logging": True      # 是否记录 API 调用日志
}

# ==================== 生成脚本安全配置 ====================
SCRIPT_EXECUTION_CONFIG = {
    "allowed_modules": ["json", "itertools", "math"],  # 允许导入的模块
    "execution_timeout": 30,                           # 脚本执行超时（秒）
    "max_output_size": 10 * 1024 * 1024               # 最大输出大小（10MB）
}

# ==================== 数据覆盖度阈值 ====================
COVERAGE_THRESHOLDS = {
    "min_coverage": 0.3,           # 低于此值直接剔除
    "low_coverage_warning": 0.5,   # 低于此值标记警告
    "min_date_coverage": 0.3,      # 时间覆盖率最低要求
}

# ==================== 操作符白名单（REGULAR scope） ====================
OPERATOR_WHITELIST = {
    "Arithmetic": ["add", "subtract", "multiply", "divide", "abs", "sign", "log", "sqrt", "power", "signed_power", "inverse", "s_log_1p"],
    "Time Series": ["ts_delta", "ts_mean", "ts_median", "ts_sum", "ts_rank", "ts_std_dev", "ts_zscore", "ts_corr", "ts_covariance", "ts_decay_linear", "ts_decay_exp_window", "ts_delay", "ts_arg_max", "ts_arg_min", "ts_backfill", "ts_count_nans", "ts_delta_limit", "ts_product", "ts_quantile", "ts_regression", "ts_scale", "ts_step", "ts_av_diff", "ts_target_tvr_decay"],
    "Cross Sectional": ["rank", "zscore", "normalize", "scale", "quantile", "truncate", "winsorize", "regression_neut"],
    "Group": ["group_rank", "group_zscore", "group_neutralize", "group_normalize", "group_mean", "group_median", "group_scale", "group_backfill", "group_cartesian_product"],
    "Vector": ["vec_avg", "vec_count", "vec_max", "vec_min", "vec_range", "vec_stddev", "vec_sum"],
    "Logical": ["if_else", "is_nan", "and", "or", "not", "greater", "less", "equal", "greater_equal", "less_equal", "not_equal"],
    "Transformational": ["bucket", "hump", "left_tail", "right_tail"],
    "Other": ["trade_when", "inst_tvr", "days_from_last_change", "last_diff_value", "densify", "reverse", "max", "min", "kth_element"],
}

# 常见错误操作符名 → 正确平台操作符名映射（含模板文件中出现的别名）
OPERATOR_ALIASES = {
    "correlation": "ts_corr",
    "corr": "ts_corr",
    "cov": "ts_covariance",
    "decay_linear": "ts_decay_linear",
    "std_dev": "ts_std_dev",
    "ts_stddev": "ts_std_dev",
    "delay": "ts_delay",
    "delta": "ts_delta",
    "mean": "ts_mean",
    "sum": "ts_sum",
    "ts_argmax": "ts_arg_max",
    "ts_argmin": "ts_arg_min",
}

# ==================== 闭环优化配置 ====================
OPTIMIZATION_CONFIG = {
    "max_iterations": 3,            # 闭环优化最大迭代轮数
    "target_sharpe": 1.0,           # 目标 Sharpe 阈值
    "target_success_rate": 0.3,     # 目标成功率（达标 Alpha 占比）
    "top_n_success": 5,             # 分析 Top N 成功案例
    "bottom_n_failure": 5,          # 分析 Bottom N 失败案例
    "min_results_for_optimization": 5,  # 最少需要多少条结果才能优化
}

# ==================== 多数据集联合研究配置 ====================
MULTI_DATASET_CONFIG = {
    "max_datasets": 4,              # 最多同时分析的数据集数量
    "fields_per_dataset": 30,       # 每个数据集最多保留的字段数
    "total_fields_limit": 80,       # 合并后字段总数上限
    "recommended_combos": {
        "价量+分析师": ["pv1", "analyst15"],
        "价量+基本面": ["pv1", "fundamental13"],
        "分析师+模型": ["analyst15", "model16"],
        "全维度": ["pv1", "analyst15", "fundamental13"],
    }
}

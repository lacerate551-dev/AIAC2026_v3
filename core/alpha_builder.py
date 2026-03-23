"""
Alpha构建模块 - 封装Alpha表达式的生成、验证和模板化
整合了原项目中 batch_alpha_generator.py, build_*.py, step03_build_alphas.py 等脚本的功能
"""
import json
import re
from typing import Optional

import pandas as pd

from . import ace_lib
from config.settings import ALPHA_DEFAULTS, REGION_DEFAULTS


class AlphaBuilder:
    """Alpha 表达式构建器"""

    # ==================== 核心构建方法 ====================

    @staticmethod
    def build_config(
        expression: str,
        region: str,
        universe: str = None,
        delay: int = None,
        decay: int = None,
        truncation: float = None,
        neutralization: str = None,
        pasteurization: str = None,
    ) -> dict:
        """
        构建单个 Alpha 的回测配置。

        Args:
            expression: Alpha 表达式 (如 "rank(ts_delta(close, 5))")
            region: 区域代码
            universe: Universe (默认使用区域默认值)
            delay: 延迟天数 (默认使用区域默认值)
            decay: 衰减系数 (默认 5)
            truncation: 截断阈值 (默认 0.08)
            neutralization: 中性化方法 (默认 "INDUSTRY")
            pasteurization: 去极值 (默认 "ON")

        Returns:
            alpha 配置字典 (可直接用于回测)
        """
        region = region.upper()
        region_config = REGION_DEFAULTS.get(region, REGION_DEFAULTS["USA"])

        config = ace_lib.generate_alpha(
            regular=expression,
            region=region,
            universe=universe or region_config["universe"],
            delay=delay if delay is not None else region_config["delay"],
            decay=decay if decay is not None else ALPHA_DEFAULTS["decay"],
            truncation=truncation or ALPHA_DEFAULTS["truncation"],
            neutralization=neutralization or ALPHA_DEFAULTS["neutralization"],
            pasteurization=pasteurization or ALPHA_DEFAULTS["pasteurization"],
            unit_handling=ALPHA_DEFAULTS["unit_handling"],
            nan_handling=ALPHA_DEFAULTS["nan_handling"],
        )
        return config

    @staticmethod
    def build_batch_configs(
        expressions: list[str],
        region: str,
        **kwargs,
    ) -> list[dict]:
        """
        批量构建 Alpha 回测配置。

        Args:
            expressions: Alpha 表达式列表
            region: 区域代码
            **kwargs: 其他参数传递给 build_config

        Returns:
            alpha 配置字典列表
        """
        configs = []
        for expr in expressions:
            config = AlphaBuilder.build_config(expr, region, **kwargs)
            configs.append(config)
        return configs

    # ==================== 模板生成 ====================

    # 内置基础模板库（保留兼容 generate_from_template 按名称）
    LEGACY_TEMPLATES = [
        {
            "name": "Momentum",
            "pattern": "rank(ts_delta({field}, {window}))",
            "params": {"window": [5, 10, 20]},
            "description": "动量因子：字段在窗口期内的变化排名",
        },
        {
            "name": "Reversal",
            "pattern": "rank(-ts_delta({field}, {window}))",
            "params": {"window": [5, 10, 20]},
            "description": "反转因子：字段在窗口期内的变化反向排名",
        },
        {
            "name": "Z-Score",
            "pattern": "rank(ts_zscore({field}, {window}))",
            "params": {"window": [20, 60, 120]},
            "description": "Z分数因子：字段的标准化偏离度",
        },
        {
            "name": "Mean_Reversion",
            "pattern": "rank({field} - ts_mean({field}, {window}))",
            "params": {"window": [10, 20, 60]},
            "description": "均值回归：当前值与历史均值的偏差",
        },
        {
            "name": "Decay_Linear",
            "pattern": "rank(ts_decay_linear({field}, {window}))",
            "params": {"window": [5, 10, 20]},
            "description": "线性衰减加权：近期数据权重更高",
        },
        {
            "name": "Neutralized_Momentum",
            "pattern": "group_neutralize(rank(ts_delta({field}, 5)), industry)",
            "params": {},
            "description": "行业中性化动量",
        },
        {
            "name": "Volatility_Adjusted",
            "pattern": "rank(ts_delta({field}, {window}) / (ts_std_dev({field}, {window}) + 0.001))",
            "params": {"window": [10, 20]},
            "description": "波动率调整信号",
        },
        {
            "name": "Multi_Field_Combo",
            "pattern": "rank({field1}) * rank({field2})",
            "params": {},
            "description": "双字段乘积组合",
        },
    ]

    # 100 模板库（已验证，仅含合法 BRAIN 操作符）
    TEMPLATES = [
        "rank(ts_mean({field}, {window}))",
        "rank(ts_delta({field}, {window}))",
        "rank(ts_std_dev({field}, {window}))",
        "rank(ts_rank({field}, {window}))",
        "rank(ts_sum({field}, {window}))",
        "rank(ts_arg_max({field}, {window}))",
        "rank(ts_arg_min({field}, {window}))",
        "rank(ts_delay({field}, {window}))",
        "rank(ts_mean(ts_delay({field},1), {window}))",
        "rank(ts_delta(ts_mean({field}, {window}), 1))",
        "rank(ts_std_dev(ts_delta({field},1), {window}))",
        "rank(ts_rank(ts_delta({field},1), {window}))",
        "rank(ts_mean(ts_rank({field}, {window}), {window}))",
        "rank(ts_mean(scale({field}), {window}))",
        "rank(ts_rank(scale({field}), {window}))",
        "rank(ts_std_dev(scale({field}), {window}))",
        "rank(ts_decay_linear({field}, {window}))",
        "rank(ts_mean(ts_decay_linear({field}, {window}), {window}))",
        "rank(ts_delta(ts_decay_linear({field}, {window}),1))",
        "rank(ts_rank(ts_decay_linear({field}, {window}), {window}))",
        "rank(ts_mean(ts_delta({field},1), {window}))",
        "rank(ts_std_dev(ts_mean({field},{window}), {window}))",
        "rank(ts_rank(ts_mean({field},{window}), {window}))",
        "rank(ts_delta(ts_rank({field},{window}),1))",
        "rank(ts_mean(ts_std_dev({field},{window}), {window}))",
        "rank(ts_rank(ts_std_dev({field},{window}), {window}))",
        "rank(ts_mean(ts_mean({field},{window}), {window}))",
        "rank(ts_delta(ts_delta({field},1), {window}))",
        "rank({field})",
        "zscore({field})",
        "scale({field})",
        "rank(zscore({field}))",
        "rank(scale({field}))",
        "zscore(rank({field}))",
        "scale(rank({field}))",
        "rank(abs({field}))",
        "rank(log({field}))",
        "rank({field} / ts_mean({field}, {window}))",
        "rank(ts_mean({field},{window}) / ts_std_dev({field},{window}))",
        "rank({field} - ts_mean({field},{window}))",
        "rank(ts_rank({field},{window}) - 0.5)",
        "rank(ts_mean({field},{window}) - ts_mean({field},{window2}))",
        "rank(ts_std_dev({field},{window}) / ts_mean({field},{window}))",
        "rank({field1} - {field2})",
        "rank({field1} / {field2})",
        "rank(ts_mean({field1},{window}) - ts_mean({field2},{window}))",
        "rank(ts_delta({field1},{window}) - ts_delta({field2},{window}))",
        "rank(ts_std_dev({field1},{window}) - ts_std_dev({field2},{window}))",
        "rank(ts_rank({field1},{window}) - ts_rank({field2},{window}))",
        "rank(ts_corr({field1}, {field2}, {window}))",
        "rank(ts_covariance({field1}, {field2}, {window}))",
        "rank(ts_mean({field1},{window1}) / ts_mean({field2},{window2}))",
        "rank(ts_delta({field1},1) / ts_delta({field2},1))",
        "rank(scale({field1}) - scale({field2}))",
        "rank(zscore({field1}) - zscore({field2}))",
        "rank(ts_rank({field1},{window}) / ts_rank({field2},{window}))",
        "rank(ts_delay({field1},{window}) - ts_delay({field2},{window}))",
        "rank(ts_mean({field1},{window}) - {field2})",
        "rank({field1} - ts_mean({field2},{window}))",
        "rank(ts_std_dev({field1},{window}) / ts_std_dev({field2},{window}))",
        "rank(ts_mean({field1},{window}) * ts_mean({field2},{window}))",
        "rank(ts_delta({field1},{window}) * ts_delta({field2},{window}))",
        "rank(ts_rank({field1},{window}) * ts_rank({field2},{window}))",
        "rank(ts_corr(ts_delta({field1},1), ts_delta({field2},1), {window}))",
        "rank(ts_corr(ts_mean({field1},{window}), ts_mean({field2},{window}), {window}))",
        "rank(ts_rank({field1},{window1}) - ts_rank({field2},{window2}))",
        "rank(ts_mean({field1},{window}) / ts_std_dev({field2},{window}))",
        "rank(ts_std_dev({field1},{window}) / ts_mean({field2},{window}))",
        "rank(ts_rank(ts_mean({field},{window}), {window}))",
        "rank(ts_rank(ts_std_dev({field},{window}), {window}))",
        "rank(ts_mean(ts_rank({field},{window}), {window}))",
        "rank(ts_mean(ts_std_dev({field},{window}), {window}))",
        "rank(ts_delta(ts_mean({field},{window}),1))",
        "rank(ts_delta(ts_std_dev({field},{window}),1))",
        "rank(ts_rank(ts_delta({field},1), {window}))",
        "rank(ts_mean(ts_delta({field},1), {window}))",
        "rank(ts_std_dev(ts_delta({field},1), {window}))",
        "rank(ts_mean(ts_decay_linear({field},{window}), {window}))",
        "rank(ts_rank(ts_decay_linear({field},{window}), {window}))",
        "rank(ts_delta(ts_decay_linear({field},{window}),1))",
        "rank(ts_std_dev(ts_decay_linear({field},{window}), {window}))",
        "rank(ts_mean(rank({field}), {window}))",
        "rank(ts_std_dev(rank({field}), {window}))",
        "rank(ts_delta(rank({field}),1))",
        "rank(ts_rank(rank({field}), {window}))",
        "rank(ts_mean(zscore({field}), {window}))",
        "rank(ts_rank(zscore({field}), {window}))",
        "rank(ts_std_dev(zscore({field}), {window}))",
        "rank(ts_mean(scale({field}), {window}))",
        "rank(ts_rank(scale({field}), {window}))",
        "rank(ts_std_dev(scale({field}), {window}))",
        "rank(ts_delta(scale({field}),1))",
        "rank(ts_rank(ts_rank({field},{window}), {window}))",
        "rank(ts_mean(ts_mean({field},{window}), {window}))",
        "rank(ts_std_dev(ts_std_dev({field},{window}), {window}))",
        "rank(ts_delta(ts_delta({field},1), {window}))",
        "rank(ts_rank(ts_std_dev({field},{window}), {window}))",
        "rank(ts_mean(ts_rank({field},{window}), {window}))",
    ]




    @classmethod
    def generate_from_template(
        cls,
        template_name: str,
        fields: list[str],
        region: str,
        max_count: int = 50,
        **kwargs,
    ) -> list[dict]:
        """
        基于模板和字段列表自动生成 Alpha 表达式及配置。

        Args:
            template_name: 模板名称 (如 "Momentum", "Reversal")
            fields: 数据字段列表
            region: 区域代码
            max_count: 最大生成数量
            **kwargs: 其他参数传递给 build_config

        Returns:
            alpha 配置字典列表
        """
        # 查找模板（从 LEGACY_TEMPLATES 按名称）
        template = None
        for t in cls.LEGACY_TEMPLATES:
            if t["name"].lower() == template_name.lower():
                template = t
                break
        if template is None:
            print(f"❌ 未找到模板: {template_name}")
            print(f"   可用模板: {', '.join(t['name'] for t in cls.LEGACY_TEMPLATES)}")
            return []

        pattern = template["pattern"]
        params = template.get("params", {})

        # 生成表达式
        expressions = []
        window_values = params.get("window", [None])
        if window_values == [None]:
            window_values = [None]

        for field in fields:
            for window in window_values:
                expr = pattern.replace("{field}", field)
                if window is not None:
                    expr = expr.replace("{window}", str(window))
                expressions.append(expr)
                if len(expressions) >= max_count:
                    break
            if len(expressions) >= max_count:
                break

        # 构建配置
        configs = cls.build_batch_configs(expressions, region, **kwargs)
        print(f"✅ 基于模板 [{template_name}] 生成了 {len(configs)} 个 Alpha 配置")
        return configs

    @classmethod
    def list_templates(cls) -> None:
        """打印所有可用的 Alpha 模板（含 LEGACY 按名称 + TEMPLATES 表达式库）"""
        print("\n📋 内置模板（按名称）:")
        print(f"{'编号':<4} {'名称':<25} {'说明'}")
        print("-" * 65)
        for i, t in enumerate(cls.LEGACY_TEMPLATES, 1):
            print(f"{i:<4} {t['name']:<25} {t['description']}")
        if getattr(cls, "TEMPLATES", None):
            print(f"\n📋 表达式模板库（共 {len(cls.TEMPLATES)} 条，按索引使用）:")
            for i, expr in enumerate(cls.TEMPLATES[:20], 0):
                print(f"  [{i}] {expr[:60]}{'...' if len(expr) > 60 else ''}")
            if len(cls.TEMPLATES) > 20:
                print(f"  ... 还有 {len(cls.TEMPLATES) - 20} 条")
        print()

    # ==================== 表达式验证 ====================

    @staticmethod
    def validate_expression(
        expression: str,
        operators_df: pd.DataFrame = None,
        fields_df: pd.DataFrame = None,
    ) -> dict:
        """
        验证 Alpha 表达式的基本合法性。

        检查内容:
        1. 括号是否匹配
        2. 操作符是否存在 (如果提供 operators_df)
        3. 字段是否存在 (如果提供 fields_df)
        4. 向量字段是否正确使用 vec_* 操作符

        Args:
            expression: Alpha 表达式
            operators_df: 操作符 DataFrame (可选)
            fields_df: 字段 DataFrame (可选)

        Returns:
            验证结果字典 {"valid": bool, "errors": list, "warnings": list}
        """
        errors = []
        warnings = []

        # 1. 括号匹配检查
        if expression.count("(") != expression.count(")"):
            errors.append("括号不匹配")

        # 2. 空表达式检查
        if not expression or expression.strip() == "":
            errors.append("表达式为空")
            return {"valid": False, "errors": errors, "warnings": warnings}

        # 3. 操作符检查（含自动修复）
        fixed_expression = expression
        if operators_df is not None and len(operators_df) > 0:
            from config.ai_config import OPERATOR_ALIASES
            # 提取表达式中的操作符名
            used_ops = set(re.findall(r'([a-z_]+)\s*\(', expression))
            # 获取已有操作符集合
            name_col = "name" if "name" in operators_df.columns else operators_df.columns[0]
            valid_ops = set(operators_df[name_col].str.lower().unique())
            invalid_ops = used_ops - valid_ops - {"industry", "subindustry", "sector"}

            for invalid_op in list(invalid_ops):
                if invalid_op in OPERATOR_ALIASES:
                    correct_op = OPERATOR_ALIASES[invalid_op]
                    fixed_expression = re.sub(rf'\b{re.escape(invalid_op)}\s*\(', f'{correct_op}(', fixed_expression)
                    warnings.append(f"操作符已自动修复: {invalid_op} → {correct_op}")
                    invalid_ops.discard(invalid_op)

            if invalid_ops:
                errors.append(f"未知操作符: {', '.join(invalid_ops)}")

        # 4. 字段检查
        if fields_df is not None and len(fields_df) > 0:
            id_col = "id" if "id" in fields_df.columns else fields_df.columns[0]
            valid_fields = set(fields_df[id_col].unique())
            # 提取可能是字段名的标识符 (非操作符、非数字、非关键字)
            all_ids = set(re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', expression))
            ops_in_expr = set(re.findall(r'([a-z_]+)\s*\(', expression))
            keywords = {"rank", "group_neutralize", "industry", "subindustry", "sector",
                        "sign", "abs", "log", "max", "min", "if", "else", "returns",
                        "close", "open", "high", "low", "volume", "vwap", "cap",
                        "sharesout", "trade_when"}
            potential_fields = all_ids - ops_in_expr - keywords
            # 简单检查：仅当字段看起来像数据集前缀时才验证
            for field in potential_fields:
                if "_" in field and not field.startswith("ts_") and not field.startswith("vec_"):
                    if field not in valid_fields:
                        warnings.append(f"字段可能不存在: {field}")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "fixed_expression": fixed_expression if fixed_expression != expression else None,
        }

    @staticmethod
    def print_validation(result: dict) -> None:
        """打印验证结果"""
        if result["valid"]:
            print("✅ 表达式验证通过")
        else:
            print("❌ 表达式验证失败:")
            for err in result["errors"]:
                print(f"   错误: {err}")
        for warn in result.get("warnings", []):
            print(f"   ⚠️ 警告: {warn}")

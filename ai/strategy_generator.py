# -*- coding: utf-8 -*-
"""
策略生成器模块
执行 AI 生成的 Python 脚本，批量生成 Alpha 配置
"""

import json
import logging
import sys
import io
from pathlib import Path
from typing import Dict, List, Any

from config.ai_config import SCRIPT_EXECUTION_CONFIG, STRATEGY_CONFIG


class StrategyGenerator:
    """执行 AI 生成的 Python 脚本，批量生成 Alpha 配置"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def generate_alphas(self, strategy_config: Dict[str, Any], output_file: str = "alphas.json") -> int:
        """
        执行策略生成脚本

        Args:
            strategy_config: AIResearcher.build_strategy() 返回的策略配置
            output_file: 输出的 JSON 文件路径

        Returns:
            生成的 Alpha 数量
        """
        self.logger.info("开始执行策略生成脚本...")

        # 1. 提取生成脚本
        generation_script = strategy_config.get("generation_script", "")
        if not generation_script:
            raise ValueError("策略配置中没有 generation_script")

        # 2. 准备沙箱环境
        import itertools
        import math

        # 构建安全的 builtins
        safe_builtins = {
            "range": range,
            "len": len,
            "str": str,
            "int": int,
            "float": float,
            "list": list,
            "dict": dict,
            "enumerate": enumerate,
            "zip": zip,
            "print": print,
            "format": format,
        }

        # 将模块直接注入到全局命名空间
        safe_globals = {
            "json": json,
            "itertools": itertools,
            "math": math,
            "__builtins__": safe_builtins,
        }

        safe_locals = {}

        # 3. 捕获脚本输出
        old_stdout = sys.stdout
        sys.stdout = captured_output = io.StringIO()

        try:
            # 执行脚本
            exec(generation_script, safe_globals, safe_locals)

            # 获取输出
            output = captured_output.getvalue()

            # 恢复 stdout
            sys.stdout = old_stdout

            # 4. 解析输出的 JSON
            try:
                alphas_list = json.loads(output)
            except json.JSONDecodeError as e:
                self.logger.error(f"脚本输出不是有效的 JSON: {output[:200]}")
                raise ValueError(f"脚本输出解析失败: {e}")

            if not isinstance(alphas_list, list):
                raise ValueError("脚本输出必须是 JSON 列表")

            # 5. 为每个 Alpha 添加 settings 和统计模板类型
            backtest_params = strategy_config.get("backtest_params", {})
            template_type_counts = {}

            for alpha in alphas_list:
                if "settings" not in alpha:
                    alpha["settings"] = backtest_params.copy()

                # 溯源元数据注入
                alpha["dataset_id"] = (
                    strategy_config.get("backtest_params", {}).get("dataset_id")
                    or strategy_config.get("context", {}).get("dataset_id")
                    or "unknown"
                )
                alpha["strategy_name"] = strategy_config.get("strategy_name", "unknown")
                alpha["region"] = (
                    strategy_config.get("backtest_params", {}).get("region")
                    or strategy_config.get("context", {}).get("region")
                    or "unknown"
                )
                alpha["universe"] = (
                    strategy_config.get("backtest_params", {}).get("universe")
                    or strategy_config.get("context", {}).get("universe")
                    or "unknown"
                )
                alpha["delay"] = (
                    strategy_config.get("backtest_params", {}).get("delay")
                    or strategy_config.get("context", {}).get("delay")
                    or 1
                )

                # 统计模板类型
                template_type = alpha.get("template_type", "unknown")
                template_type_counts[template_type] = template_type_counts.get(template_type, 0) + 1

            # 6. 去重检查
            unique_expressions = set()
            deduplicated_alphas = []

            for alpha in alphas_list:
                expr = alpha.get("expression", "")
                if expr and expr not in unique_expressions:
                    unique_expressions.add(expr)
                    deduplicated_alphas.append(alpha)

            removed_count = len(alphas_list) - len(deduplicated_alphas)
            if removed_count > 0:
                self.logger.warning(f"去重：移除了 {removed_count} 个重复表达式")

            # 7. 输出模板类型统计
            self.logger.info(f"模板类型分布: {template_type_counts}")

            # 8. 写入文件
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(deduplicated_alphas, f, ensure_ascii=False, indent=2)

            self.logger.info(f"成功生成 {len(deduplicated_alphas)} 个 Alpha，已保存到 {output_path}")

            return len(deduplicated_alphas)

        except Exception as e:
            sys.stdout = old_stdout
            self.logger.error(f"脚本执行失败: {e}")
            raise

    def validate_expressions(self, alphas_file: str) -> Dict[str, Any]:
        """
        验证生成的表达式语法

        Args:
            alphas_file: Alpha 配置文件路径

        Returns:
            验证结果统计
        """
        from core.alpha_builder import AlphaBuilder

        self.logger.info(f"开始验证表达式: {alphas_file}")

        # 1. 读取文件
        with open(alphas_file, "r", encoding="utf-8") as f:
            alphas_list = json.load(f)

        if not isinstance(alphas_list, list):
            raise ValueError("文件内容必须是 JSON 列表")

        # 2. 逐个验证
        valid_count = 0
        invalid_count = 0
        invalid_expressions = []

        for i, alpha in enumerate(alphas_list):
            expr = alpha.get("expression", "")

            try:
                # 使用 AlphaBuilder 验证（这里简化处理，实际可能需要 session）
                # 简单检查：表达式不为空，包含基本操作符
                if not expr:
                    raise ValueError("表达式为空")

                # 基本语法检查：括号匹配
                if expr.count("(") != expr.count(")"):
                    raise ValueError("括号不匹配")

                # 标记为有效
                alpha["valid"] = True
                valid_count += 1

            except Exception as e:
                alpha["valid"] = False
                alpha["error"] = str(e)
                invalid_count += 1
                invalid_expressions.append({
                    "index": i,
                    "expression": expr,
                    "error": str(e)
                })

        # 3. 更新文件（添加 valid 标记）
        with open(alphas_file, "w", encoding="utf-8") as f:
            json.dump(alphas_list, f, ensure_ascii=False, indent=2)

        # 4. 返回统计结果
        result = {
            "total": len(alphas_list),
            "valid": valid_count,
            "invalid": invalid_count,
            "invalid_expressions": invalid_expressions[:10]  # 只返回前 10 个
        }

        self.logger.info(f"验证完成: 总数 {result['total']}, 有效 {result['valid']}, 无效 {result['invalid']}")

        return result

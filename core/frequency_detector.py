# -*- coding: utf-8 -*-
"""
[备用工具] 字段更新频率检测器 - 基于回测的自动化频率推断

状态: 备用（当前 Pipeline 使用 ai/frequency_inference.py 基于规则的快速推断）

用途: 当需要精确检测字段更新频率时使用，通过构建特定 alpha 表达式并回测来分析。

注意: 此方法较慢（需要实际回测），但结果更精确。日常使用推荐 frequency_inference.py。
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import pandas as pd

from core.alpha_builder import AlphaBuilder
from core.backtest_runner import BacktestRunner
from config.settings import CACHE_DIR

logger = logging.getLogger(__name__)


class FrequencyDetector:
    """字段更新频率检测器"""

    def __init__(self, session, region: str = "USA"):
        """
        初始化频率检测器

        Args:
            session: 已认证的 BRAIN 会话
            region: 区域代码
        """
        self.session = session
        self.region = region.upper()
        self.alpha_builder = AlphaBuilder()

    def build_frequency_test_alphas(
        self, field_name: str, dataset_id: str = None
    ) -> List[Dict]:
        """
        为指定字段构建频率检测 alpha 表达式

        Args:
            field_name: 字段名称
            dataset_id: 数据集 ID（可选，用于命名）

        Returns:
            alpha 配置列表
        """
        prefix = f"{dataset_id}_" if dataset_id else ""

        # 构建多个测试 alpha
        test_alphas = []

        # 1. days_from_last_change - 最直接的频率指标
        test_alphas.append({
            "name": f"{prefix}{field_name}_freq_test_days_change",
            "expression": f"days_from_last_change({field_name})",
            "description": f"检测 {field_name} 的变化间隔天数"
        })

        # 2. ts_count_nans - 缺失值模式分析
        for window in [20, 60, 90]:
            test_alphas.append({
                "name": f"{prefix}{field_name}_freq_test_nans_{window}d",
                "expression": f"ts_count_nans({field_name}, {window})",
                "description": f"检测 {field_name} 在 {window} 天内的缺失值数量"
            })

        # 3. ts_delta - 变化检测
        for lag in [1, 7, 30]:
            test_alphas.append({
                "name": f"{prefix}{field_name}_freq_test_delta_{lag}d",
                "expression": f"abs(ts_delta({field_name}, {lag}))",
                "description": f"检测 {field_name} 的 {lag} 天变化幅度"
            })

        # 4. 组合指标 - 综合判断
        test_alphas.append({
            "name": f"{prefix}{field_name}_freq_test_combined",
            "expression": f"rank(divide(ts_count_nans({field_name}, 20), add(days_from_last_change({field_name}), 1)))",
            "description": f"综合频率指标: 缺失值比例 / 变化间隔"
        })

        return test_alphas

    def run_frequency_detection(
        self,
        field_name: str,
        dataset_id: str = None,
        backtest_days: int = 252,  # 至少 1 年
    ) -> Dict:
        """
        运行频率检测回测

        Args:
            field_name: 字段名称
            dataset_id: 数据集 ID
            backtest_days: 回测天数（默认 252 个交易日 = 1年）

        Returns:
            检测结果字典
        """
        logger.info(f"开始检测字段 {field_name} 的更新频率...")

        # 构建测试 alpha
        test_alphas = self.build_frequency_test_alphas(field_name, dataset_id)

        # 批量回测：使用已有操作符构建表达式，回测后从结果推断更新频率
        results = []
        for alpha_config in test_alphas:
            try:
                # 使用 AlphaBuilder.build_config 生成回测配置（与主流程一致）
                config = self.alpha_builder.build_config(
                    alpha_config["expression"],
                    self.region,
                    decay=0,
                    truncation=0.01,
                    neutralization="NONE",
                )

                # 运行单次回测（BacktestRunner.run_single 返回扁平结构：turnover, coverage, sharpe, fitness 等）
                result = BacktestRunner.run_single(self.session, config)

                if result and result.get("success"):
                    results.append({
                        "test_type": alpha_config["name"],
                        "description": alpha_config["description"],
                        "turnover": result.get("turnover", 0),
                        "coverage": result.get("coverage", 0),
                        "fitness": result.get("fitness", 0),
                        "sharpe": result.get("sharpe", 0),
                    })
                    logger.info(f"  ✓ {alpha_config['name']}: turnover={result.get('turnover', 0):.4f}, coverage={result.get('coverage', 0):.4f}")

            except Exception as e:
                logger.warning(f"  ✗ {alpha_config['name']} 回测失败: {e}")
                continue

        if not results:
            logger.error(f"字段 {field_name} 的所有频率检测均失败")
            return {"field_name": field_name, "frequency": "unknown", "confidence": 0.0}

        # 分析结果并推断频率
        frequency_result = self._analyze_frequency_results(field_name, results)
        return frequency_result

    def _analyze_frequency_results(
        self, field_name: str, results: List[Dict]
    ) -> Dict:
        """
        分析回测结果并推断频率

        Args:
            field_name: 字段名称
            results: 回测结果列表

        Returns:
            频率推断结果
        """
        # 提取关键指标
        days_change_result = next(
            (r for r in results if "days_change" in r["test_type"]), None
        )
        nans_20d_result = next(
            (r for r in results if "nans_20d" in r["test_type"]), None
        )
        nans_60d_result = next(
            (r for r in results if "nans_60d" in r["test_type"]), None
        )
        nans_90d_result = next(
            (r for r in results if "nans_90d" in r["test_type"]), None
        )
        delta_1d_result = next(
            (r for r in results if "delta_1d" in r["test_type"]), None
        )
        delta_7d_result = next(
            (r for r in results if "delta_7d" in r["test_type"]), None
        )

        # 推断逻辑
        frequency = "unknown"
        confidence = 0.0
        reasoning = []

        # 1. 基于 coverage 判断
        avg_coverage = sum(r["coverage"] for r in results) / len(results)
        if avg_coverage < 0.1:
            frequency = "irregular"
            confidence = 0.8
            reasoning.append(f"平均覆盖率极低 ({avg_coverage:.2%})")

        # 2. 基于 turnover 判断变化频率
        if days_change_result:
            # 注意: days_from_last_change 的 turnover 反映变化频率
            # 高 turnover = 频繁变化 = 高频数据
            turnover = days_change_result["turnover"]

            if turnover > 0.5:
                frequency = "daily"
                confidence = 0.9
                reasoning.append(f"变化间隔 turnover 高 ({turnover:.4f}), 表明日频更新")
            elif turnover > 0.2:
                frequency = "weekly"
                confidence = 0.7
                reasoning.append(f"变化间隔 turnover 中等 ({turnover:.4f}), 表明周频更新")
            elif turnover > 0.05:
                frequency = "monthly"
                confidence = 0.7
                reasoning.append(f"变化间隔 turnover 较低 ({turnover:.4f}), 表明月频更新")
            else:
                frequency = "quarterly"
                confidence = 0.6
                reasoning.append(f"变化间隔 turnover 很低 ({turnover:.4f}), 表明季频或更低")

        # 3. 基于缺失值模式验证
        if nans_20d_result and nans_60d_result:
            nans_20 = nans_20d_result["coverage"]
            nans_60 = nans_60d_result["coverage"]

            # 如果 20 天内缺失值很多，说明不是日频
            if nans_20 < 0.3 and frequency == "daily":
                confidence += 0.1
                reasoning.append(f"20天缺失值少 ({nans_20:.2%}), 验证日频判断")
            elif nans_20 > 0.7 and frequency in ["weekly", "monthly"]:
                confidence += 0.1
                reasoning.append(f"20天缺失值多 ({nans_20:.2%}), 验证低频判断")

        # 4. 基于 delta 变化幅度验证
        if delta_1d_result and delta_7d_result:
            delta_1 = delta_1d_result["turnover"]
            delta_7 = delta_7d_result["turnover"]

            # 如果 1 天 delta 有明显变化，说明是高频数据
            if delta_1 > 0.3 and frequency == "daily":
                confidence += 0.1
                reasoning.append(f"1天变化明显 (turnover={delta_1:.4f}), 验证日频")

        # 限制置信度范围
        confidence = min(confidence, 1.0)

        return {
            "field_name": field_name,
            "frequency": frequency,
            "confidence": confidence,
            "reasoning": reasoning,
            "metrics": {
                "avg_coverage": avg_coverage,
                "test_results": results,
            }
        }

    def batch_detect_fields(
        self,
        fields: List[str],
        dataset_id: str = None,
        max_concurrent: int = 3,
    ) -> Dict[str, Dict]:
        """
        批量检测多个字段的更新频率

        Args:
            fields: 字段名称列表
            dataset_id: 数据集 ID
            max_concurrent: 最大并发数

        Returns:
            {field_name: frequency_result} 字典
        """
        logger.info(f"开始批量检测 {len(fields)} 个字段的更新频率...")

        results = {}
        for i, field in enumerate(fields, 1):
            logger.info(f"[{i}/{len(fields)}] 检测字段: {field}")
            try:
                result = self.run_frequency_detection(field, dataset_id)
                results[field] = result

                # 输出结果
                logger.info(
                    f"  结果: {result['frequency']} "
                    f"(置信度: {result['confidence']:.2f})"
                )
            except Exception as e:
                logger.error(f"  检测失败: {e}")
                results[field] = {
                    "field_name": field,
                    "frequency": "unknown",
                    "confidence": 0.0,
                    "error": str(e)
                }

        return results

    def save_detection_results(
        self, results: Dict[str, Dict], output_path: Path = None
    ):
        """
        保存检测结果到文件

        Args:
            results: 检测结果字典
            output_path: 输出路径（默认保存到 cache/frequency_detection/）
        """
        if output_path is None:
            output_dir = CACHE_DIR / "frequency_detection"
            output_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = output_dir / f"detection_results_{timestamp}.json"

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        logger.info(f"检测结果已保存到: {output_path}")

    def load_detection_results(self, input_path: Path) -> Dict[str, Dict]:
        """
        从文件加载检测结果

        Args:
            input_path: 输入路径

        Returns:
            检测结果字典
        """
        with open(input_path, "r", encoding="utf-8") as f:
            results = json.load(f)

        logger.info(f"已加载检测结果: {input_path}")
        return results

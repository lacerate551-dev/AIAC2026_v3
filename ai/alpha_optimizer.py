# -*- coding: utf-8 -*-
"""
Alpha 优化器模块
支持通过 Alpha ID 多轮迭代优化，目标是让 Alpha 通过所有 BRAIN 平台检查项。

优化策略：
1. 参数优化：调整 decay, truncation, neutralization（低成本）
2. 表达式平滑：添加 ts_decay_linear, ts_mean 等平滑操作符
3. 组合优化：添加 group_neutralize 等
4. AI 深度优化：AI 分析失败原因，自主查询字段，生成新表达式
"""
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# 默认优化配置
DEFAULT_CONFIG = {
    "max_rounds": 5,
    "target_sharpe": 1.0,
    "target_fitness": 0.5,
    "max_turnover": 0.70,  # 70%
    # 参数优化搜索空间
    "decay_candidates": [2, 5, 10, 15, 20],
    "truncation_candidates": [0.01, 0.05, 0.08, 0.10],
    "neutralization_candidates": ["MARKET", "INDUSTRY", "SUBINDUSTRY", "SECTOR"],
    # 固定中性化（用于 Power Pool Theme 匹配，设为 "FAST" 等可锁定）
    "fixed_neutralization": None,
    # 并行回测数量
    "parallel_count": 6,
}


@dataclass
class OptimizationRecord:
    """单次优化记录"""
    round_num: int
    strategy: str  # "param_tune", "smoothing", "combination", "ai_deep"
    expression: str
    settings: Dict[str, Any]
    result: Optional[Dict[str, Any]] = None
    success: bool = False
    improvement: str = ""


@dataclass
class OptimizationHistory:
    """优化历史"""
    original_alpha_id: str
    original_expression: str
    original_settings: Dict[str, Any]
    original_metrics: Dict[str, float]
    records: List[OptimizationRecord] = field(default_factory=list)
    best_alpha: Optional[Dict[str, Any]] = None
    best_metrics: Optional[Dict[str, float]] = None
    final_status: str = "running"  # "success", "failed", "max_rounds"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_alpha_id": self.original_alpha_id,
            "original_expression": self.original_expression,
            "original_settings": self.original_settings,
            "original_metrics": self.original_metrics,
            "records": [
                {
                    "round": r.round_num,
                    "strategy": r.strategy,
                    "expression": r.expression,
                    "settings": r.settings,
                    "metrics": {
                        "sharpe": r.result.get("sharpe") if r.result else None,
                        "fitness": r.result.get("fitness") if r.result else None,
                        "turnover": r.result.get("turnover") if r.result else None,
                    } if r.result else None,
                    "success": r.success,
                    "improvement": r.improvement,
                }
                for r in self.records
            ],
            "best_alpha": self.best_alpha,
            "best_metrics": self.best_metrics,
            "final_status": self.final_status,
            "timestamp": datetime.now().isoformat(),
        }


class AlphaOptimizer:
    """Alpha 优化器"""

    def __init__(self, config: Dict[str, Any] = None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.logger = logging.getLogger(__name__)

    def optimize(
        self,
        session,
        alpha_id: str,
        region: str,
        max_rounds: int = None,
        output_dir: Optional[Path] = None,
    ) -> OptimizationHistory:
        """
        优化指定 Alpha

        Args:
            session: BRAIN 会话
            alpha_id: Alpha ID
            region: 区域
            max_rounds: 最大优化轮数
            output_dir: 输出目录（保存优化历史）

        Returns:
            OptimizationHistory: 优化历史记录
        """
        from core import ace_lib
        from core.backtest_runner import BacktestRunner

        max_rounds = max_rounds or self.config["max_rounds"]

        # 1. 获取原始 Alpha 详情
        self.logger.info(f"[优化] 获取 Alpha {alpha_id} 详情...")
        original_result = ace_lib.get_simulation_result_json(session, alpha_id)

        if not original_result:
            self.logger.error(f"无法获取 Alpha {alpha_id} 详情")
            return OptimizationHistory(
                original_alpha_id=alpha_id,
                original_expression="",
                original_settings={},
                original_metrics={},
                final_status="failed",
            )

        # 提取原始信息
        original_expr = original_result.get("regular", "")
        if isinstance(original_expr, dict):
            original_expr = original_expr.get("code", "")

        original_settings = original_result.get("settings", {})
        original_is = original_result.get("is", {})

        history = OptimizationHistory(
            original_alpha_id=alpha_id,
            original_expression=original_expr,
            original_settings=original_settings,
            original_metrics={
                "sharpe": original_is.get("sharpe", 0),
                "fitness": original_is.get("fitness", 0),
                "turnover": original_is.get("turnover", 0),
                "returns": original_is.get("returns", 0),
                "drawdown": original_is.get("drawdown", 0),
            },
        )

        # 检查是否已经达标
        if self._check_qualified(original_is):
            self.logger.info("[优化] 原 Alpha 已达标，无需优化")
            history.best_alpha = {
                "alpha_id": alpha_id,
                "expression": original_expr,
                "settings": original_settings,
            }
            history.best_metrics = history.original_metrics
            history.final_status = "success"
            return history

        # 分析失败原因
        failed_checks = self._analyze_failures(original_is)
        self.logger.info(f"[优化] 失败检查项: {failed_checks}")

        # 2. 多轮优化
        for round_num in range(1, max_rounds + 1):
            self.logger.info(f"\n{'='*50}")
            self.logger.info(f"[优化] 第 {round_num}/{max_rounds} 轮")

            # 选择优化策略
            strategy = self._select_strategy(round_num, failed_checks, history)
            self.logger.info(f"[优化] 策略: {strategy}")

            # 生成候选 Alpha
            candidates = self._generate_candidates(
                original_expr, original_settings, strategy, round_num, failed_checks
            )

            if not candidates:
                self.logger.warning(f"[优化] 无法生成候选 Alpha，跳过本轮")
                continue

            # 并行回测
            self.logger.info(f"[优化] 回测 {len(candidates)} 个候选...")
            results = self._run_batch_backtest(session, candidates, region, original_settings)

            # 评估结果
            best_result, best_candidate = self._evaluate_results(results, candidates)

            if best_result:
                record = OptimizationRecord(
                    round_num=round_num,
                    strategy=strategy,
                    expression=best_candidate.get("expression", ""),
                    settings=best_candidate.get("settings", {}),
                    result=best_result,
                    success=best_result.get("success", False),
                    improvement=self._describe_improvement(history.best_metrics, best_result),
                )
                history.records.append(record)

                # 更新最佳 Alpha
                if self._is_better(history.best_metrics, best_result):
                    history.best_alpha = best_candidate
                    history.best_metrics = {
                        "sharpe": best_result.get("sharpe", 0),
                        "fitness": best_result.get("fitness", 0),
                        "turnover": best_result.get("turnover", 0),
                    }
                    self.logger.info(
                        f"[优化] 新最佳: Sharpe={best_result.get('sharpe', 0):.2f}, "
                        f"Turnover={best_result.get('turnover', 0):.2%}"
                    )

                # 检查是否达标
                if self._check_qualified(best_result):
                    self.logger.info("[优化] ✅ 达标！优化成功")
                    history.final_status = "success"
                    break
            else:
                history.records.append(OptimizationRecord(
                    round_num=round_num,
                    strategy=strategy,
                    expression="",
                    settings={},
                    success=False,
                    improvement="本轮无有效结果",
                ))

        # 3. 最终状态
        if history.final_status == "running":
            history.final_status = "max_rounds" if history.records else "failed"

        # 4. 保存历史
        if output_dir:
            self._save_history(history, output_dir)

        return history

    def _check_qualified(self, metrics: Dict[str, Any]) -> bool:
        """检查是否达标"""
        if not metrics:
            return False

        sharpe = metrics.get("sharpe", 0) or 0
        fitness = metrics.get("fitness", 0) or 0
        turnover = metrics.get("turnover", 1) or 1

        return (
            sharpe >= self.config["target_sharpe"]
            and fitness >= self.config["target_fitness"]
            and turnover <= self.config["max_turnover"]
        )

    def _analyze_failures(self, is_data: Dict[str, Any]) -> List[str]:
        """分析失败的检查项"""
        failures = []
        checks = is_data.get("checks", [])

        for c in checks:
            if c.get("result") == "FAIL":
                failures.append(c.get("name", ""))

        # 同时检查核心指标
        sharpe = is_data.get("sharpe", 0) or 0
        fitness = is_data.get("fitness", 0) or 0
        turnover = is_data.get("turnover", 1) or 1

        if turnover > self.config["max_turnover"]:
            failures.append("HIGH_TURNOVER")
        if sharpe < self.config["target_sharpe"]:
            failures.append("LOW_SHARPE")
        if fitness < self.config["target_fitness"]:
            failures.append("LOW_FITNESS")

        return list(set(failures))

    def _select_strategy(
        self,
        round_num: int,
        failed_checks: List[str],
        history: OptimizationHistory,
    ) -> str:
        """选择优化策略"""
        # 如果设置了强制策略，直接使用
        force_strategy = self.config.get("force_strategy")
        if force_strategy:
            self.logger.info(f"[优化] 使用强制策略: {force_strategy}")
            return force_strategy

        # 根据轮次和失败原因选择策略
        if round_num == 1:
            # 第一轮：参数优化（成本最低）
            return "param_tune"
        elif round_num == 2:
            # 第二轮：表达式平滑
            if "HIGH_TURNOVER" in failed_checks:
                return "smoothing"
            return "param_tune"
        elif round_num == 3:
            # 第三轮：组合优化
            if "CONCENTRATED_WEIGHT" in failed_checks:
                return "combination"
            return "smoothing"
        else:
            # 第四轮及以后：AI 深度优化
            return "ai_deep"

    def _generate_candidates(
        self,
        original_expr: str,
        original_settings: Dict[str, Any],
        strategy: str,
        round_num: int,
        failed_checks: List[str],
    ) -> List[Dict[str, Any]]:
        """生成候选 Alpha"""
        candidates = []

        if strategy == "param_tune":
            candidates = self._generate_param_candidates(original_expr, original_settings, failed_checks)
        elif strategy == "smoothing":
            candidates = self._generate_smoothing_candidates(original_expr, original_settings, failed_checks)
        elif strategy == "combination":
            candidates = self._generate_combination_candidates(original_expr, original_settings, failed_checks)
        elif strategy == "ai_deep":
            candidates = self._generate_ai_candidates(original_expr, original_settings, failed_checks)

        return candidates[:self.config["parallel_count"]]

    def _generate_param_candidates(
        self,
        expression: str,
        settings: Dict[str, Any],
        failed_checks: List[str],
    ) -> List[Dict[str, Any]]:
        """参数优化：生成不同参数组合"""
        candidates = []

        # 原始参数
        orig_decay = settings.get("decay", 5)
        orig_trunc = settings.get("truncation", 0.08)
        orig_neut = settings.get("neutralization", "INDUSTRY")

        # 针对 HIGH_TURNOVER，优先尝试更大的 decay
        if "HIGH_TURNOVER" in failed_checks:
            decay_candidates = [d for d in self.config["decay_candidates"] if d > orig_decay]
            decay_candidates = decay_candidates[:3]  # 最多 3 个
        else:
            decay_candidates = self.config["decay_candidates"][:3]

        # 中性化候选：如果设置了固定中性化，则只使用该值
        fixed_neut = self.config.get("fixed_neutralization")
        if fixed_neut:
            neut_candidates = [fixed_neut]
        else:
            neut_candidates = self.config["neutralization_candidates"][:2]

        # 生成组合
        for decay in decay_candidates:
            for neut in neut_candidates:
                candidates.append({
                    "expression": expression,
                    "settings": {
                        **settings,
                        "decay": decay,
                        "neutralization": neut,
                    },
                })

        return candidates

    def _generate_smoothing_candidates(
        self,
        expression: str,
        settings: Dict[str, Any],
        failed_checks: List[str],
    ) -> List[Dict[str, Any]]:
        """表达式平滑：添加平滑操作符"""
        candidates = []

        # 固定中性化时使用该值
        fixed_neut = self.config.get("fixed_neutralization")
        neut = fixed_neut if fixed_neut else settings.get("neutralization", "INDUSTRY")

        # 针对 HIGH_TURNOVER，添加平滑
        if "HIGH_TURNOVER" in failed_checks:
            # ts_decay_linear
            for window in [5, 10, 20]:
                candidates.append({
                    "expression": f"ts_decay_linear({expression}, {window})",
                    "settings": {**settings, "decay": max(settings.get("decay", 5), 10), "neutralization": neut},
                })

            # ts_mean
            for window in [5, 10]:
                candidates.append({
                    "expression": f"ts_mean({expression}, {window})",
                    "settings": {**settings, "decay": max(settings.get("decay", 5), 10), "neutralization": neut},
                })

        return candidates

    def _generate_combination_candidates(
        self,
        expression: str,
        settings: Dict[str, Any],
        failed_checks: List[str],
    ) -> List[Dict[str, Any]]:
        """组合优化：添加中性化等"""
        candidates = []

        # 固定中性化时使用该值，否则默认 MARKET
        fixed_neut = self.config.get("fixed_neutralization")
        neut = fixed_neut if fixed_neut else "MARKET"

        # 针对 CONCENTRATED_WEIGHT，添加 group_neutralize
        if "CONCENTRATED_WEIGHT" in failed_checks:
            # 尝试不同的分组中性化
            for group in ["industry", "sector", "subindustry"]:
                candidates.append({
                    "expression": f"group_neutralize({expression}, {group})",
                    "settings": {**settings, "neutralization": neut},
                })

        # 同时结合平滑
        if "HIGH_TURNOVER" in failed_checks:
            candidates.append({
                "expression": f"ts_decay_linear({expression}, 10)",
                "settings": {**settings, "decay": 15, "neutralization": neut},
            })

        return candidates

    def _generate_ai_candidates(
        self,
        expression: str,
        settings: Dict[str, Any],
        failed_checks: List[str],
    ) -> List[Dict[str, Any]]:
        """AI 深度优化：调用 AI 生成新表达式"""
        candidates = []

        # 固定中性化时使用该值
        fixed_neut = self.config.get("fixed_neutralization")
        neut = fixed_neut if fixed_neut else "SUBINDUSTRY"

        try:
            from ai.researcher_brain import AIResearcher
            ai = AIResearcher()

            # 读取本地可用操作符文档
            operators_doc = self._load_operators_documentation()

            prompt = f"""你是一个量化因子优化专家。请分析以下 Alpha 表达式的问题并生成优化版本。

原始表达式: {expression}
当前设置: decay={settings.get('decay')}, truncation={settings.get('truncation')}, neutralization={settings.get('neutralization')}
失败检查项: {failed_checks}

**BRAIN 平台可用操作符文档：**
{operators_doc}

**重要规则：**
1. 只能使用上述文档中列出的操作符
2. 分组中性化只能使用: industry, sector, subindustry
3. 条件判断使用 if_else(condition, true_val, false_val)，不是 if...then...else
4. 时间序列标准差使用 ts_std_dev，不是 ts_std

问题分析:
- HIGH_TURNOVER: 换手率过高，需要增加平滑
- CONCENTRATED_WEIGHT: 持仓过于集中，需要分散
- LOW_2Y_SHARPE: 2年夏普率低，稳定性不足

优化建议:
1. 增加时间序列平滑: ts_decay_linear, ts_mean
2. 增大 decay 参数
3. 添加 group_neutralize 分散持仓
4. 使用更长的窗口参数

请生成 3 个优化后的表达式，返回 JSON 格式:
{{"expressions": ["expr1", "expr2", "expr3"], "reasoning": "优化理由"}}
"""

            result = ai._call_ai(prompt, json_mode=True)
            expressions = result.get("expressions", [])

            # 验证并过滤表达式
            for expr in expressions[:3]:
                if expr and expr != expression:
                    if self._validate_expression(expr):
                        candidates.append({
                            "expression": expr,
                            "settings": {**settings, "decay": 15, "neutralization": neut},
                        })
                    else:
                        self.logger.warning(f"AI 生成了无效表达式，已跳过: {expr[:50]}...")

        except Exception as e:
            self.logger.warning(f"AI 优化失败: {e}")

        return candidates

    def _load_operators_documentation(self) -> str:
        """加载本地可用操作符文档"""
        try:
            from pathlib import Path
            operators_path = Path(__file__).parent.parent / "config" / "operators_metadata.json"

            if not operators_path.exists():
                return "请参考 BRAIN 平台官方文档"

            import json
            with open(operators_path, "r", encoding="utf-8") as f:
                operators = json.load(f)

            # 格式化为文档
            doc_lines = []
            for op in operators[:30]:  # 限制数量避免 prompt 过长
                name = op.get("name", "")
                desc = op.get("description", "")
                example = op.get("example", "")
                doc_lines.append(f"- {name}: {desc}")
                if example:
                    doc_lines.append(f"  示例: {example}")

            return "\n".join(doc_lines)

        except Exception as e:
            self.logger.warning(f"加载操作符文档失败: {e}")
            return "请参考 BRAIN 平台官方文档"

    def _validate_expression(self, expr: str) -> bool:
        """验证表达式是否只使用有效操作符"""
        import re

        # 已知的无效模式
        INVALID_PATTERNS = [
            r'\bts_std\b',           # 应该是 ts_std_dev
            r'\bif\s+',              # if ... then ... else 语法
            r'\bthen\b',             # then 关键字
            r'\belse\b',             # else 单独使用
            r'\bindclass\b',         # 无效变量
            r'\bgics_sector\b',      # 无效变量
            r'\bgics_industry\b',    # 无效变量
        ]

        for pattern in INVALID_PATTERNS:
            if re.search(pattern, expr, re.IGNORECASE):
                return False

        return True

    def _run_batch_backtest(
        self,
        session,
        candidates: List[Dict[str, Any]],
        region: str,
        base_settings: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """批量回测候选 Alpha"""
        from core.alpha_builder import AlphaBuilder
        from core import ace_lib

        results = []
        configs = []

        for c in candidates:
            config = AlphaBuilder.build_config(
                c["expression"],
                region,
                universe=base_settings.get("universe"),
                delay=base_settings.get("delay"),
                decay=c["settings"].get("decay"),
                truncation=c["settings"].get("truncation"),
                neutralization=c["settings"].get("neutralization"),
            )
            configs.append(config)

        # 批量回测
        batch_results = ace_lib.simulate_multi_alpha(session, configs)

        for i, result in enumerate(batch_results):
            alpha_id = result.get("alpha_id")
            candidate = candidates[i] if i < len(candidates) else {}

            if alpha_id:
                detailed = ace_lib.get_simulation_result_json(session, alpha_id)
                is_data = detailed.get("is", {})
                results.append({
                    "alpha_id": alpha_id,
                    "expression": candidate.get("expression", ""),
                    "settings": candidate.get("settings", {}),
                    "sharpe": is_data.get("sharpe", 0),
                    "fitness": is_data.get("fitness", 0),
                    "turnover": is_data.get("turnover", 0),
                    "returns": is_data.get("returns", 0),
                    "success": True,
                })
            else:
                results.append({
                    "alpha_id": None,
                    "expression": candidate.get("expression", ""),
                    "settings": candidate.get("settings", {}),
                    "sharpe": 0,
                    "fitness": 0,
                    "turnover": 1,
                    "success": False,
                })

        return results

    def _evaluate_results(
        self,
        results: List[Dict[str, Any]],
        candidates: List[Dict[str, Any]],
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """评估回测结果，返回最佳结果"""
        if not results:
            return None, None

        # 按综合得分排序
        def score(r):
            if not r.get("success"):
                return -1000
            sharpe = r.get("sharpe", 0) or 0
            fitness = r.get("fitness", 0) or 0
            turnover = r.get("turnover", 1) or 1

            # 惩罚高换手率
            turnover_penalty = max(0, turnover - self.config["max_turnover"]) * 10

            return sharpe + fitness - turnover_penalty

        sorted_results = sorted(results, key=score, reverse=True)
        best = sorted_results[0]

        if not best.get("success"):
            return None, None

        # 找到对应的 candidate
        best_candidate = None
        for c in candidates:
            if c.get("expression") == best.get("expression"):
                best_candidate = c
                break

        return best, best_candidate

    def _is_better(
        self,
        current_best: Optional[Dict[str, float]],
        new_result: Dict[str, Any],
    ) -> bool:
        """判断新结果是否更好"""
        if not current_best:
            return True

        def score(m):
            if not m:
                return -1000
            sharpe = m.get("sharpe", 0) or 0
            fitness = m.get("fitness", 0) or 0
            turnover = m.get("turnover", 1) or 1
            turnover_penalty = max(0, turnover - self.config["max_turnover"]) * 10
            return sharpe + fitness - turnover_penalty

        return score(new_result) > score(current_best)

    def _describe_improvement(
        self,
        old_metrics: Optional[Dict[str, float]],
        new_result: Dict[str, Any],
    ) -> str:
        """描述改进情况"""
        if not old_metrics:
            return "首次优化"

        old_sharpe = old_metrics.get("sharpe", 0) or 0
        old_turnover = old_metrics.get("turnover", 1) or 1
        new_sharpe = new_result.get("sharpe", 0) or 0
        new_turnover = new_result.get("turnover", 1) or 1

        parts = []
        if new_sharpe > old_sharpe:
            parts.append(f"Sharpe {old_sharpe:.2f}→{new_sharpe:.2f}")
        if new_turnover < old_turnover:
            parts.append(f"Turnover {old_turnover:.1%}→{new_turnover:.1%}")

        return ", ".join(parts) if parts else "无改进"

    def _save_history(self, history: OptimizationHistory, output_dir: Path):
        """保存优化历史"""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"optimization_{history.original_alpha_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(history.to_dict(), f, ensure_ascii=False, indent=2)

        self.logger.info(f"[优化] 历史已保存: {filepath}")


def optimize_alpha(
    session,
    alpha_id: str,
    region: str,
    max_rounds: int = 5,
    output_dir: Optional[Path] = None,
    config: Optional[Dict[str, Any]] = None,
) -> OptimizationHistory:
    """
    优化指定 Alpha 的便捷函数

    Args:
        session: BRAIN 会话
        alpha_id: Alpha ID
        region: 区域
        max_rounds: 最大优化轮数
        output_dir: 输出目录
        config: 优化配置

    Returns:
        OptimizationHistory: 优化历史
    """
    optimizer = AlphaOptimizer(config)
    return optimizer.optimize(session, alpha_id, region, max_rounds, output_dir)


def batch_optimize(
    session,
    alpha_ids: List[str],
    region: str,
    max_rounds: int = 5,
    output_dir: Optional[Path] = None,
    stop_on_success: bool = True,
    config: Optional[Dict[str, Any]] = None,
) -> List[OptimizationHistory]:
    """
    批量优化多个 Alpha

    Args:
        session: BRAIN 会话
        alpha_ids: Alpha ID 列表
        region: 区域
        max_rounds: 最大优化轮数
        output_dir: 输出目录
        stop_on_success: 是否在找到达标 Alpha 后停止
        config: 优化配置

    Returns:
        List[OptimizationHistory]: 优化历史列表
    """
    optimizer = AlphaOptimizer(config)
    histories = []

    for i, alpha_id in enumerate(alpha_ids):
        logger.info(f"\n{'#'*60}")
        logger.info(f"[批量优化] {i+1}/{len(alpha_ids)}: {alpha_id}")

        history = optimizer.optimize(session, alpha_id, region, max_rounds, output_dir)
        histories.append(history)

        if stop_on_success and history.final_status == "success":
            logger.info("[批量优化] 找到达标 Alpha，停止优化")
            break

    return histories


# ==================== 批量组合优化 ====================

@dataclass
class ExpressionGroup:
    """表达式分组"""
    expression: str
    alphas: List[Dict[str, Any]]  # 同一表达式的不同参数变体
    avg_sharpe: float = 0.0
    avg_turnover: float = 0.0
    best_sharpe: float = 0.0
    best_alpha_id: str = ""


@dataclass
class CombinationResult:
    """组合优化结果"""
    source_groups: List[ExpressionGroup]
    generated_expressions: List[str]
    backtest_results: List[Dict[str, Any]]
    qualified_alphas: List[Dict[str, Any]]
    analysis: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_groups": [
                {
                    "expression": g.expression,
                    "count": len(g.alphas),
                    "avg_sharpe": g.avg_sharpe,
                    "avg_turnover": g.avg_turnover,
                }
                for g in self.source_groups
            ],
            "generated_expressions": self.generated_expressions,
            "qualified_count": len(self.qualified_alphas),
            "qualified_alphas": [
                {
                    "alpha_id": a.get("alpha_id"),
                    "expression": a.get("expression"),
                    "sharpe": a.get("sharpe"),
                    "turnover": a.get("turnover"),
                }
                for a in self.qualified_alphas
            ],
            "analysis": self.analysis,
            "timestamp": datetime.now().isoformat(),
        }


class BatchCombinationOptimizer:
    """
    批量组合优化器

    分析多个 Alpha 的表达式结构，自动分组，
    通过 AI 分析生成组合创新方案。
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.logger = logging.getLogger(__name__)

    def analyze_and_combine(
        self,
        session,
        alpha_ids: List[str],
        region: str,
        output_dir: Optional[Path] = None,
        max_combinations: int = 10,
    ) -> CombinationResult:
        """
        分析并组合优化多个 Alpha

        Args:
            session: BRAIN 会话
            alpha_ids: Alpha ID 列表
            region: 区域
            output_dir: 输出目录
            max_combinations: 最大生成组合数

        Returns:
            CombinationResult: 组合优化结果
        """
        from core import ace_lib

        self.logger.info(f"[组合优化] 分析 {len(alpha_ids)} 个 Alpha...")

        # Step 1: 获取所有 Alpha 详情并分组
        groups = self._fetch_and_group_alphas(session, alpha_ids)

        if not groups:
            self.logger.error("[组合优化] 无法获取 Alpha 详情")
            return CombinationResult(
                source_groups=[],
                generated_expressions=[],
                backtest_results=[],
                qualified_alphas=[],
                analysis="获取 Alpha 详情失败",
            )

        self.logger.info(f"[组合优化] 共 {len(groups)} 个表达式组")
        for i, g in enumerate(groups, 1):
            self.logger.info(
                f"  组{i}: {len(g.alphas)} 个变体, "
                f"Sharpe={g.avg_sharpe:.2f}, Turnover={g.avg_turnover:.1%}"
            )

        # Step 2: AI 分析并生成组合方案
        self.logger.info("[组合优化] AI 分析表达式结构...")
        combination_plans = self._generate_combination_plans(groups, max_combinations)

        if not combination_plans:
            self.logger.warning("[组合优化] AI 未生成有效组合方案")
            return CombinationResult(
                source_groups=groups,
                generated_expressions=[],
                backtest_results=[],
                qualified_alphas=[],
                analysis="AI 未生成有效组合方案",
            )

        self.logger.info(f"[组合优化] 生成 {len(combination_plans)} 个组合方案")

        # Step 3: 构建候选 Alpha
        candidates = self._build_candidates(combination_plans, groups)

        # Step 4: 批量回测
        self.logger.info(f"[组合优化] 回测 {len(candidates)} 个候选 Alpha...")
        base_settings = groups[0].alphas[0].get("settings", {}) if groups[0].alphas else {}
        results = self._run_batch_backtest(session, candidates, region, base_settings)

        # Step 5: 筛选达标 Alpha
        qualified = [
            r for r in results
            if r.get("success")
            and (r.get("sharpe", 0) or 0) >= self.config["target_sharpe"]
            and (r.get("fitness", 0) or 0) >= self.config["target_fitness"]
            and (r.get("turnover", 1) or 1) <= self.config["max_turnover"]
        ]

        # Step 6: 生成分析报告
        analysis = self._generate_analysis_report(groups, results, qualified)

        # Step 7: 保存结果
        result = CombinationResult(
            source_groups=groups,
            generated_expressions=[c.get("expression", "") for c in candidates],
            backtest_results=results,
            qualified_alphas=qualified,
            analysis=analysis,
        )

        if output_dir:
            self._save_result(result, output_dir)

        return result

    def _fetch_and_group_alphas(
        self,
        session,
        alpha_ids: List[str],
    ) -> List[ExpressionGroup]:
        """获取 Alpha 详情并按表达式分组"""
        from core import ace_lib

        expression_map: Dict[str, List[Dict[str, Any]]] = {}

        for aid in alpha_ids:
            try:
                result = ace_lib.get_simulation_result_json(session, aid)

                regular = result.get("regular", "")
                if isinstance(regular, dict):
                    regular = regular.get("code", "")

                settings = result.get("settings", {})
                is_data = result.get("is", {})

                alpha_info = {
                    "alpha_id": aid,
                    "expression": regular,
                    "settings": settings,
                    "sharpe": is_data.get("sharpe", 0) or 0,
                    "fitness": is_data.get("fitness", 0) or 0,
                    "turnover": is_data.get("turnover", 1) or 1,
                    "returns": is_data.get("returns", 0) or 0,
                }

                if regular not in expression_map:
                    expression_map[regular] = []
                expression_map[regular].append(alpha_info)

            except Exception as e:
                self.logger.warning(f"获取 Alpha {aid} 失败: {e}")

        # 构建分组
        groups = []
        for expr, alphas in expression_map.items():
            sharpe_list = [a["sharpe"] for a in alphas]
            turnover_list = [a["turnover"] for a in alphas]

            group = ExpressionGroup(
                expression=expr,
                alphas=alphas,
                avg_sharpe=sum(sharpe_list) / len(sharpe_list) if sharpe_list else 0,
                avg_turnover=sum(turnover_list) / len(turnover_list) if turnover_list else 0,
                best_sharpe=max(sharpe_list) if sharpe_list else 0,
                best_alpha_id=alphas[0]["alpha_id"] if alphas else "",
            )
            groups.append(group)

        # 按 avg_sharpe 降序排序
        groups.sort(key=lambda g: g.avg_sharpe, reverse=True)

        return groups

    def _generate_combination_plans(
        self,
        groups: List[ExpressionGroup],
        max_plans: int,
    ) -> List[Dict[str, Any]]:
        """AI 生成组合方案"""
        try:
            from ai.researcher_brain import AIResearcher
            ai = AIResearcher()
        except Exception as e:
            self.logger.error(f"AI 初始化失败: {e}")
            return []

        # 构建分析输入
        groups_info = []
        for i, g in enumerate(groups, 1):
            groups_info.append({
                "group_id": i,
                "expression": g.expression,
                "count": len(g.alphas),
                "avg_sharpe": round(g.avg_sharpe, 2),
                "avg_turnover": round(g.avg_turnover, 3),
                "best_sharpe": round(g.best_sharpe, 2),
            })

        prompt = f"""你是一个量化因子组合优化专家。请分析以下 Alpha 表达式组，生成组合优化方案。

## 输入数据

表达式分组（按 Sharpe 降序）:
```json
{json.dumps(groups_info, ensure_ascii=False, indent=2)}
```

## 问题分析

所有 Alpha 的共同问题:
- Turnover 过高（目标 ≤ 70%，当前普遍 120%+）
- 需要降低换手率同时保持 Sharpe

## 可用操作符列表（必须严格使用这些操作符，不要创造新语法）

**时间序列操作符**:
- ts_mean(expr, window) - 滑窗均值
- ts_decay_linear(expr, window) - 线性衰减加权（降低 turnover 最有效）
- ts_sum(expr, window) - 滑窗求和
- ts_std_dev(expr, window) - 滑窗标准差
- ts_rank(expr, window) - 滑窗排名
- ts_delta(expr, n) - 差分
- ts_zscore(expr, window) - 时序标准化

**截面操作符**:
- rank(expr) - 截面排名
- zscore(expr) - 截面标准化
- scale(expr) - 截面缩放

**分组操作符**:
- group_neutralize(expr, industry) - 行业中性化
- group_neutralize(expr, sector) - 板块中性化
- group_neutralize(expr, subindustry) - 子行业中性化
- group_rank(expr, group) - 组内排名

**条件操作符**:
- if_else(condition, true_value, false_value) - 条件选择（注意：不是 if...then...else）

**数学操作符**:
- abs(expr), log(expr), sign(expr), sqrt(expr)

## 组合策略建议

1. **平滑包装**（推荐，最有效降低 turnover）:
   - `ts_decay_linear(expr, 20)` 或 `ts_mean(expr, 15)`

2. **信号融合**:
   - `rank(expr1 + expr2)` 或 `rank(0.5 * expr1 + 0.5 * expr2)`

3. **中性化增强**:
   - `group_neutralize(expr, subindustry)`

4. **条件过滤**（使用 if_else，不是 if）:
   - `if_else(abs(expr) > 0.1, expr, 0)`

## 输出要求

请生成 {min(max_plans, 6)} 个组合方案，返回 JSON 格式:
{{
  "analysis": "对各组表达式的分析（中文）",
  "plans": [
    {{
      "type": "smoothing|fusion|neutralize|filter|hybrid",
      "expression": "生成的表达式",
      "reasoning": "设计理由（中文）",
      "expected_effect": "预期效果"
    }}
  ]
}}

**重要提醒**:
- 表达式必须只使用上面列出的操作符
- 条件判断用 if_else(cond, true_val, false_val)，不要用 if...then...else
- 窗口参数建议 15-30
- 优先使用 ts_decay_linear 降低 turnover
"""

        try:
            result = ai._call_ai(prompt, json_mode=True)
            plans = result.get("plans", [])

            # 过滤有效方案（检查不支持的操作符）
            INVALID_PATTERNS = [
                r'\bif\s+',           # if ... then ... else (错误语法)
                r'\bthen\b',          # then 关键字
                r'\belse\b',          # else 关键字（单独使用）
                r'\bfor\b',           # for 循环
                r'\bwhile\b',         # while 循环
            ]
            VALID_OPERATORS = [
                'rank', 'zscore', 'scale', 'normalize', 'quantile',
                'ts_mean', 'ts_delta', 'ts_delay', 'ts_std_dev', 'ts_rank',
                'ts_sum', 'ts_corr', 'ts_covariance', 'ts_arg_max', 'ts_arg_min',
                'ts_decay_linear', 'ts_zscore', 'ts_product', 'ts_max', 'ts_min',
                'ts_median', 'ts_scale', 'ts_skewness', 'ts_kurtosis', 'ts_regression',
                'group_neutralize', 'group_rank', 'group_mean', 'group_zscore', 'group_scale',
                'abs', 'log', 'sign', 'power', 'sqrt', 'inverse', 'winsorize', 'truncate',
                'if_else', 'trade_when', 'regression_neut',
                'vec_avg', 'vec_sum', 'vec_count', 'vec_max', 'vec_min',
                'days_from_last_change', 'ts_count_nans',
            ]

            valid_plans = []
            for p in plans:
                expr = p.get("expression", "")
                if not expr:
                    continue

                # 检查是否包含不支持的模式
                has_invalid = False
                for pattern in INVALID_PATTERNS:
                    if re.search(pattern, expr, re.IGNORECASE):
                        self.logger.warning(f"表达式包含不支持语法: {expr[:50]}...")
                        has_invalid = True
                        break

                if has_invalid:
                    continue

                # 检查是否以 rank 开头（必要条件）
                if not expr.strip().startswith("rank("):
                    self.logger.warning(f"表达式不以 rank 开头: {expr[:50]}...")
                    continue

                valid_plans.append(p)

            return valid_plans[:max_plans]

        except Exception as e:
            self.logger.error(f"AI 生成组合方案失败: {e}")
            return []

    def _build_candidates(
        self,
        plans: List[Dict[str, Any]],
        groups: List[ExpressionGroup],
    ) -> List[Dict[str, Any]]:
        """根据组合方案构建候选 Alpha"""
        candidates = []

        # 获取基础设置
        base_settings = {}
        if groups and groups[0].alphas:
            base_settings = groups[0].alphas[0].get("settings", {})

        for plan in plans:
            expr = plan.get("expression", "")
            if not expr:
                continue

            # 为每个表达式生成多个参数变体
            # decay 较大值有助于降低 turnover
            for decay in [10, 15, 20]:
                for neut in ["MARKET", "INDUSTRY", "SUBINDUSTRY"]:
                    candidates.append({
                        "expression": expr,
                        "settings": {
                            **base_settings,
                            "decay": decay,
                            "neutralization": neut,
                        },
                        "plan_type": plan.get("type", "unknown"),
                        "reasoning": plan.get("reasoning", ""),
                    })

        return candidates

    def _run_batch_backtest(
        self,
        session,
        candidates: List[Dict[str, Any]],
        region: str,
        base_settings: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """批量回测候选 Alpha（每批最多 10 个）"""
        from core.alpha_builder import AlphaBuilder
        from core import ace_lib

        results = []
        configs = []

        for c in candidates:
            config = AlphaBuilder.build_config(
                c["expression"],
                region,
                universe=base_settings.get("universe"),
                delay=base_settings.get("delay"),
                decay=c["settings"].get("decay"),
                truncation=c["settings"].get("truncation", 0.08),
                neutralization=c["settings"].get("neutralization"),
            )
            configs.append(config)

        # 分批回测（每批最多 10 个）
        batch_size = 10
        total_batches = (len(configs) + batch_size - 1) // batch_size

        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(configs))
            batch_configs = configs[start_idx:end_idx]
            batch_candidates = candidates[start_idx:end_idx]

            self.logger.info(f"[回测] 批次 {batch_idx + 1}/{total_batches} ({len(batch_configs)} 个)")

            batch_results = ace_lib.simulate_multi_alpha(session, batch_configs)

            for i, result in enumerate(batch_results):
                alpha_id = result.get("alpha_id")
                candidate = batch_candidates[i] if i < len(batch_candidates) else {}

                if alpha_id:
                    detailed = ace_lib.get_simulation_result_json(session, alpha_id)
                    is_data = detailed.get("is", {})
                    checks = is_data.get("checks", [])

                    # 检查失败项
                    failed_checks = [c.get("name") for c in checks if c.get("result") == "FAIL"]

                    results.append({
                        "alpha_id": alpha_id,
                        "expression": candidate.get("expression", ""),
                        "settings": candidate.get("settings", {}),
                        "plan_type": candidate.get("plan_type", ""),
                        "reasoning": candidate.get("reasoning", ""),
                        "sharpe": is_data.get("sharpe", 0) or 0,
                        "fitness": is_data.get("fitness", 0) or 0,
                        "turnover": is_data.get("turnover", 1) or 1,
                        "returns": is_data.get("returns", 0) or 0,
                        "failed_checks": failed_checks,
                        "success": True,
                    })
                else:
                    results.append({
                        "alpha_id": None,
                        "expression": candidate.get("expression", ""),
                        "settings": candidate.get("settings", {}),
                        "sharpe": 0,
                        "fitness": 0,
                        "turnover": 1,
                        "success": False,
                    })

        return results

    def _generate_analysis_report(
        self,
        groups: List[ExpressionGroup],
        results: List[Dict[str, Any]],
        qualified: List[Dict[str, Any]],
    ) -> str:
        """生成分析报告"""
        lines = []

        lines.append("## 组合优化分析报告\n")

        # 源 Alpha 分析
        lines.append("### 源 Alpha 分析\n")
        lines.append(f"- 输入 Alpha 数量: {sum(len(g.alphas) for g in groups)}")
        lines.append(f"- 表达式分组数: {len(groups)}\n")

        for i, g in enumerate(groups, 1):
            lines.append(f"**组 {i}**: `{g.expression[:60]}...`")
            lines.append(f"  - 变体数: {len(g.alphas)}")
            lines.append(f"  - 平均 Sharpe: {g.avg_sharpe:.2f}")
            lines.append(f"  - 平均 Turnover: {g.avg_turnover:.1%}\n")

        # 回测结果
        lines.append("### 回测结果\n")
        success_count = sum(1 for r in results if r.get("success"))
        lines.append(f"- 回测成功: {success_count}/{len(results)}")
        lines.append(f"- 达标 Alpha: {len(qualified)}\n")

        if qualified:
            lines.append("### 达标 Alpha 列表\n")
            for i, a in enumerate(qualified, 1):
                lines.append(f"{i}. `{a.get('alpha_id', 'N/A')}`")
                lines.append(f"   - Sharpe: {a.get('sharpe', 0):.2f}")
                lines.append(f"   - Turnover: {a.get('turnover', 0):.1%}")
                lines.append(f"   - 表达式: `{a.get('expression', '')[:50]}...`\n")

        return "\n".join(lines)

    def _save_result(self, result: CombinationResult, output_dir: Path):
        """保存组合优化结果"""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"combination_optimization_{timestamp}.json"
        filepath = output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)

        self.logger.info(f"[组合优化] 结果已保存: {filepath}")


def batch_combine_optimize(
    session,
    alpha_ids: List[str],
    region: str,
    output_dir: Optional[Path] = None,
    max_combinations: int = 10,
) -> CombinationResult:
    """
    批量组合优化的便捷函数

    Args:
        session: BRAIN 会话
        alpha_ids: Alpha ID 列表
        region: 区域
        output_dir: 输出目录
        max_combinations: 最大生成组合数

    Returns:
        CombinationResult: 组合优化结果
    """
    optimizer = BatchCombinationOptimizer()
    return optimizer.analyze_and_combine(
        session, alpha_ids, region, output_dir, max_combinations
    )
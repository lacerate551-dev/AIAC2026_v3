"""
回测执行与报告解析模块 - 统一管理Alpha回测的提交、进度跟踪和结果解析
整合了原项目中 step6_standalone_optimizer.py, optimize_*.py, fetch_alpha_report.py 等脚本的功能
"""
import json
import re
import random
import signal
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd

from . import ace_lib
from .session_manager import SessionManager
from config.settings import (
    RESEARCH_DIR,
    BATCH_SIZE,
    MAX_CONCURRENT_SIMS,
    MIN_SHARPE,
    MIN_FITNESS,
    MAX_TURNOVER,
)


# ==================== 错误分析引擎 ====================

class ErrorAnalyzer:
    """错误分析引擎 - 深度解析回测错误并分类"""

    # 错误类型定义
    ERROR_TYPE_FIELD = "A"      # 字段无效/缺失
    ERROR_TYPE_OPERATOR = "B"   # 操作符语法错误
    ERROR_TYPE_CONFIG = "C"     # 配置冲突
    ERROR_TYPE_LIMIT = "D"      # 平台限制
    ERROR_TYPE_SYNTAX = "E"     # 表达式语法错误
    ERROR_TYPE_UNKNOWN = "Z"    # 未知错误

    @staticmethod
    def analyze_error(error_message: str, raw_response: dict = None) -> Dict[str, Any]:
        """
        深度分析错误信息，返回错误类型、受影响实体、建议操作

        Args:
            error_message: 错误信息字符串
            raw_response: 原始响应字典（可选）

        Returns:
            {
                "error_type": "A/B/C/D/E/Z",
                "error_category": "field/operator/config/limit/syntax/unknown",
                "affected_entity": "close" or "ts_mean" or "delay=0",
                "suggested_action": "refresh_fields/refresh_operators/adjust_config/none",
                "confidence": 0.0-1.0,
                "error_message": "原始错误信息"
            }
        """
        if not error_message:
            error_message = ""

        error_lower = error_message.lower()

        # 类型 A: 字段错误
        # 匹配模式: "field 'xxx' not found", "invalid field", "unknown field"
        field_patterns = [
            r"field\s+['\"]?(\w+)['\"]?\s+(?:not found|invalid|unknown|does not exist)",
            r"(?:invalid|unknown)\s+field\s+['\"]?(\w+)['\"]?",
            r"field\s+['\"]?(\w+)['\"]?\s+is\s+(?:invalid|not available)",
        ]

        for pattern in field_patterns:
            match = re.search(pattern, error_lower)
            if match:
                field_name = match.group(1)
                return {
                    "error_type": ErrorAnalyzer.ERROR_TYPE_FIELD,
                    "error_category": "field",
                    "affected_entity": field_name,
                    "suggested_action": "refresh_fields",
                    "confidence": 0.9,
                    "error_message": error_message
                }

        # 通用字段关键词匹配
        if any(keyword in error_lower for keyword in ["field", "column", "attribute"]):
            if any(keyword in error_lower for keyword in ["not found", "invalid", "unknown", "missing"]):
                # 尝试提取字段名（简单提取引号内容）
                field_match = re.search(r"['\"](\w+)['\"]", error_message)
                field_name = field_match.group(1) if field_match else "unknown"
                return {
                    "error_type": ErrorAnalyzer.ERROR_TYPE_FIELD,
                    "error_category": "field",
                    "affected_entity": field_name,
                    "suggested_action": "refresh_fields",
                    "confidence": 0.7,
                    "error_message": error_message
                }

        # 类型 B: 操作符错误
        # 匹配模式: "operator 'xxx' requires", "invalid operator", "ts_mean expects"
        operator_patterns = [
            r"operator\s+['\"]?(\w+)['\"]?\s+(?:requires|expects|needs)",
            r"(?:invalid|unknown)\s+operator\s+['\"]?(\w+)['\"]?",
            r"(\w+)\s+(?:requires|expects)\s+(?:window|parameter)",
        ]

        for pattern in operator_patterns:
            match = re.search(pattern, error_lower)
            if match:
                operator_name = match.group(1)
                return {
                    "error_type": ErrorAnalyzer.ERROR_TYPE_OPERATOR,
                    "error_category": "operator",
                    "affected_entity": operator_name,
                    "suggested_action": "refresh_operators",
                    "confidence": 0.85,
                    "error_message": error_message
                }

        # 通用操作符关键词匹配
        if any(keyword in error_lower for keyword in ["operator", "function"]):
            if any(keyword in error_lower for keyword in ["invalid", "unknown", "requires", "expects"]):
                # 尝试提取操作符名
                op_match = re.search(r"['\"]?(\w+)['\"]?", error_message)
                operator_name = op_match.group(1) if op_match else "unknown"
                return {
                    "error_type": ErrorAnalyzer.ERROR_TYPE_OPERATOR,
                    "error_category": "operator",
                    "affected_entity": operator_name,
                    "suggested_action": "refresh_operators",
                    "confidence": 0.6,
                    "error_message": error_message
                }

        # 类型 C: 配置错误
        # 匹配模式: "delay 0 is not allowed", "invalid universe", "region not supported"
        config_patterns = [
            r"(delay|universe|region|neutralization)\s+['\"]?(\w+)['\"]?\s+(?:is not allowed|invalid|not supported)",
            r"(?:invalid|unsupported)\s+(delay|universe|region|neutralization)",
        ]

        for pattern in config_patterns:
            match = re.search(pattern, error_lower)
            if match:
                config_key = match.group(1)
                config_value = match.group(2) if len(match.groups()) > 1 else "unknown"
                return {
                    "error_type": ErrorAnalyzer.ERROR_TYPE_CONFIG,
                    "error_category": "config",
                    "affected_entity": f"{config_key}={config_value}",
                    "suggested_action": "adjust_config",
                    "confidence": 0.8,
                    "error_message": error_message
                }

        # 类型 D: 平台限制
        # 匹配模式: "maximum of 10 simulations", "rate limit", "quota exceeded"
        if any(keyword in error_lower for keyword in ["maximum", "limit", "quota", "exceeded", "too many"]):
            return {
                "error_type": ErrorAnalyzer.ERROR_TYPE_LIMIT,
                "error_category": "limit",
                "affected_entity": "platform_limit",
                "suggested_action": "reduce_batch_size",
                "confidence": 0.9,
                "error_message": error_message
            }

        # 类型 E: 语法错误
        # 匹配模式: "syntax error", "parse error", "invalid expression"
        if any(keyword in error_lower for keyword in ["syntax", "parse", "parsing", "malformed"]):
            return {
                "error_type": ErrorAnalyzer.ERROR_TYPE_SYNTAX,
                "error_category": "syntax",
                "affected_entity": "expression",
                "suggested_action": "fix_syntax",
                "confidence": 0.85,
                "error_message": error_message
            }

        # 未知错误
        return {
            "error_type": ErrorAnalyzer.ERROR_TYPE_UNKNOWN,
            "error_category": "unknown",
            "affected_entity": "unknown",
            "suggested_action": "none",
            "confidence": 0.3,
            "error_message": error_message
        }


class BacktestRunner:
    """回测执行与报告解析器"""

    def __init__(self):
        """初始化回测运行器"""
        self.logger = logging.getLogger(__name__)
        self._interrupted = False  # 中断标志

    # ==================== 进度管理 ====================

    @staticmethod
    def _save_progress(
        progress_file: Path,
        total: int,
        completed_indices: set,
        sample_indices: List[int] = None,
        start_time: str = None,
    ):
        """
        原子化保存进度文件

        Args:
            progress_file: 进度文件路径
            total: 总数
            completed_indices: 已完成的索引集合
            sample_indices: 抽样的索引列表（用于 random_n 模式）
            start_time: 开始时间
        """
        progress_data = {
            "total": total,
            "completed": len(completed_indices),
            "completed_indices": sorted(list(completed_indices)),
            "start_time": start_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # 如果是随机抽样模式，持久化抽样索引
        if sample_indices is not None:
            progress_data["sample_indices"] = sample_indices
            progress_data["sample_mode"] = "random_n"

        # 原子化写入：先写临时文件，再重命名
        temp_file = progress_file.with_suffix(".json.tmp")
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)
            # 原子化重命名
            temp_file.replace(progress_file)
        except Exception as e:
            logging.error(f"保存进度文件失败: {e}")
            if temp_file.exists():
                temp_file.unlink()

    @staticmethod
    def _load_progress(progress_file: Path) -> Dict[str, Any]:
        """
        加载进度文件

        Args:
            progress_file: 进度文件路径

        Returns:
            进度数据字典
        """
        try:
            with open(progress_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"加载进度文件失败: {e}")
            return {}

    def _setup_signal_handler(self, progress_file: Path, total: int, completed_indices: set, sample_indices: List[int] = None, start_time: str = None):
        """
        设置信号处理器，捕获 Ctrl+C 实现优雅退出

        Args:
            progress_file: 进度文件路径
            total: 总数
            completed_indices: 已完成的索引集合
            sample_indices: 抽样索引列表
            start_time: 开始时间
        """
        def signal_handler(signum, frame):
            print("\n\n[!] 检测到中断信号 (Ctrl+C)，正在保存进度...")
            self._interrupted = True
            # 保存进度
            self._save_progress(progress_file, total, completed_indices, sample_indices, start_time)
            print(f"[OK] 进度已保存到: {progress_file}")
            print(f"[STATS] 已完成: {len(completed_indices)}/{total}")
            print("[TIP] 下次运行时选择'续传'可继续回测")
            raise KeyboardInterrupt()

        signal.signal(signal.SIGINT, signal_handler)

    # ==================== 回测执行 ====================

    @staticmethod
    def run_single(session, alpha_config: dict) -> dict:
        """
        回测单个 Alpha 并返回完整结果。

        Args:
            session: 已认证的 Session 对象
            alpha_config: 由 AlphaBuilder.build_config() 生成的配置

        Returns:
            完整的回测结果字典，包含:
            - alpha_id: Alpha ID
            - expression: 表达式
            - sharpe: Sharpe Ratio
            - fitness: Fitness
            - turnover: Turnover
            - checks: 检查项结果
            - raw: 原始 JSON 结果
        """
        print(f"[TEST] 回测中: {alpha_config.get('regular', 'N/A')[:60]}...")

        # 执行回测
        result = ace_lib.simulate_single_alpha(session, alpha_config)
        alpha_id = result.get("alpha_id")

        if alpha_id is None:
            expr = alpha_config.get("regular", "unknown")
            print(f"[FAIL] 回测失败: {expr[:60]}")
            return {
                "alpha_id": None,
                "expression": expr,
                "sharpe": 0,
                "fitness": 0,
                "turnover": 0,
                "checks": [],
                "raw": result,
                "success": False,
            }

        # 获取详细结果
        detailed = ace_lib.get_simulation_result_json(session, alpha_id)
        return BacktestRunner.parse_report(detailed)

    def run_batch(
        self,
        session,
        alpha_configs: List[Dict],
        output_dir: str = None,
        batch_size: int = None,
        sample_mode: str = "all",
        sample_count: int = None,
        resume: bool = False,
        alpha_metadata: List[Dict] = None,
    ) -> List[Dict]:
        """
        批量回测多个 Alpha（支持断点续传和抽样）

        Args:
            session: 已认证的 Session 对象
            alpha_configs: 配置列表
            output_dir: 输出目录（用于保存进度文件）
            batch_size: 每批数量 (默认 BATCH_SIZE)
            sample_mode: 抽样模式 ("all" / "first_n" / "random_n")
            sample_count: 抽样数量（仅在 first_n 和 random_n 模式下使用）
            resume: 是否续传
            alpha_metadata: Alpha 溯源元数据列表（可选）

        Returns:
            回测结果列表
        """
        batch_size = batch_size or BATCH_SIZE
        total = len(alpha_configs)

        # 初始化进度管理
        progress_file = None
        completed_indices = set()
        sample_indices = None
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            progress_file = output_path / "progress.json"

            # 加载进度（如果续传）
            if resume and progress_file.exists():
                progress_data = self._load_progress(progress_file)
                completed_indices = set(progress_data.get("completed_indices", []))
                sample_indices = progress_data.get("sample_indices")
                start_time = progress_data.get("start_time", start_time)

                print(f"\n[FILE] 加载进度: 已完成 {len(completed_indices)}/{progress_data.get('total', total)}")

                # 如果是随机抽样模式的续传，使用保存的索引
                if sample_indices:
                    sample_mode = "random_n"
                    sample_count = len(sample_indices)
                    print(f"[RAND] 续传随机抽样模式: {sample_count} 个 Alpha")

        # 处理抽样逻辑
        if sample_mode == "first_n" and sample_count:
            indices_to_test = list(range(min(sample_count, total)))
            print(f"\n[STATS] 抽样模式: 前 {len(indices_to_test)} 个")
        elif sample_mode == "random_n" and sample_count:
            if sample_indices is None:
                # 首次随机抽样
                sample_indices = sorted(random.sample(range(total), min(sample_count, total)))
                print(f"\n[RAND] 抽样模式: 随机 {len(sample_indices)} 个")
            indices_to_test = sample_indices
        else:
            # 全部回测
            indices_to_test = list(range(total))
            print(f"\n[STATS] 回测模式: 全部 {total} 个")

        # 过滤已完成的索引
        if resume and completed_indices:
            indices_to_test = [i for i in indices_to_test if i not in completed_indices]
            print(f"[SKIP]  跳过已完成: {len(completed_indices)} 个，剩余: {len(indices_to_test)} 个")

        # 设置信号处理器（优雅退出）
        if progress_file:
            self._setup_signal_handler(progress_file, len(indices_to_test), completed_indices, sample_indices, start_time)

        # 准备结果列表（保持原始索引顺序）
        all_results = [None] * total

        print(f"\n[RUN] 开始批量回测 (每批 {batch_size} 个)")
        print("=" * 60)

        try:
            # 批量回测
            for batch_start in range(0, len(indices_to_test), batch_size):
                if self._interrupted:
                    break

                batch_indices = indices_to_test[batch_start : batch_start + batch_size]
                batch_configs = [alpha_configs[i] for i in batch_indices]
                batch_num = batch_start // batch_size + 1
                total_batches = (len(indices_to_test) + batch_size - 1) // batch_size

                print(f"\n[Batch] 批次 {batch_num}/{total_batches} ({len(batch_configs)} 个 Alpha)")

                # 使用多重模拟
                batch_results = ace_lib.simulate_multi_alpha(session, batch_configs)

                for j, result in enumerate(batch_results):
                    if self._interrupted:
                        break

                    original_index = batch_indices[j]
                    alpha_id = result.get("alpha_id")

                    if alpha_id:
                        detailed = ace_lib.get_simulation_result_json(session, alpha_id)
                        parsed = BacktestRunner.parse_report(detailed)

                        # 合并溯源元数据
                        if alpha_metadata and original_index < len(alpha_metadata):
                            meta = alpha_metadata[original_index]
                            for key in ("dataset_id", "strategy_name", "template_type", "region", "universe", "delay"):
                                if key in meta:
                                    parsed[key] = meta[key]

                        all_results[original_index] = parsed
                        status = "[OK]" if parsed["sharpe"] >= MIN_SHARPE else "  "
                        print(
                            f"  {status} [{original_index+1}/{total}] "
                            f"Sharpe={parsed['sharpe']:.3f} "
                            f"Fitness={parsed['fitness']:.3f} "
                            f"Turnover={parsed['turnover']:.2%} "
                            f"ID={alpha_id}"
                        )
                    else:
                        all_results[original_index] = {
                            "alpha_id": None,
                            "expression": result.get("simulate_data", {}).get("regular", "N/A"),
                            "sharpe": 0, "fitness": 0, "turnover": 0,
                            "checks": [], "raw": result, "success": False,
                        }
                        print(f"  [FAIL] [{original_index+1}/{total}] 回测失败")

                    # 更新进度
                    completed_indices.add(original_index)
                    if progress_file:
                        self._save_progress(progress_file, total, completed_indices, sample_indices, start_time)

        except KeyboardInterrupt:
            print("\n\n[WARN]  回测已中断")
            # 信号处理器已保存进度

        finally:
            # 清理信号处理器
            signal.signal(signal.SIGINT, signal.SIG_DFL)

        # 过滤 None 结果（未回测的）
        final_results = [r for r in all_results if r is not None]

        # 汇总
        successful = [r for r in final_results if r.get("success", False)]
        # 仅按 Sharpe 筛选（用于终端显示）
        high_sharpe = [r for r in successful if r["sharpe"] >= MIN_SHARPE]
        # 完整筛选（包含 Fitness 和 Turnover）
        fully_qualified = [
            r for r in successful
            if r["sharpe"] >= MIN_SHARPE
            and r["fitness"] >= MIN_FITNESS
            and r["turnover"] <= MAX_TURNOVER
        ]

        print(f"\n{'=' * 60}")
        print(f"[STATS] 回测完成: {len(successful)}/{len(final_results)} 成功")
        print(f"[STATS] 高Sharpe(≥{MIN_SHARPE}): {len(high_sharpe)} 个")
        print(f"[STATS] 完全达标(Sharpe≥{MIN_SHARPE}, Fitness≥{MIN_FITNESS}, Turnover≤{MAX_TURNOVER:.0%}): {len(fully_qualified)} 个")

        # 如果全部完成，删除进度文件
        if progress_file and len(completed_indices) >= len(indices_to_test):
            try:
                progress_file.unlink()
                print(f"[OK] 回测全部完成，已清理进度文件")
            except:
                pass

        return final_results

    # ==================== 报告解析 ====================

    @staticmethod
    def _extract_error_message(report_json: dict) -> str:
        """
        从回测响应中提取错误信息

        Args:
            report_json: 原始响应字典

        Returns:
            错误信息字符串
        """
        # 尝试多种可能的错误字段
        if "errors" in report_json:
            errors = report_json["errors"]
            if isinstance(errors, list) and errors:
                return str(errors[0])
            elif isinstance(errors, str):
                return errors
            else:
                return str(errors)

        if "error" in report_json:
            error = report_json["error"]
            if isinstance(error, dict):
                return error.get("message", str(error))
            else:
                return str(error)

        if "message" in report_json:
            return str(report_json["message"])

        # 如果没有明确的错误字段，返回整个响应的字符串表示
        return json.dumps(report_json, ensure_ascii=False)[:200]

    @staticmethod
    def parse_report(report_json: dict) -> dict:
        """
        解析单个 Alpha 的回测报告 JSON（增强鲁棒性 + 错误分析）

        Args:
            report_json: 由 ace_lib.get_simulation_result_json() 返回的原始 JSON

        Returns:
            解析后的结构化结果字典（包含错误分析）
        """
        # 默认返回结构
        default_result = {
            "alpha_id": None,
            "expression": "",
            "sharpe": 0.0,
            "fitness": 0.0,
            "turnover": 0.0,
            "coverage": 0.0,
            "returns": 0.0,
            "drawdown": 0.0,
            "margin": 0.0,
            "long_count": 0,
            "short_count": 0,
            "pnl": 0.0,
            "checks": [],
            "failed_checks": [],
            "passed_checks": [],
            "n_failed": 0,
            "n_passed": 0,
            "settings": {},
            "raw": report_json,
            "success": False,
            "error_message": None,
            "error_analysis": None,
        }

        try:
            # 检查基本结构
            if not report_json or not isinstance(report_json, dict):
                logging.warning("回测报告为空或格式错误")
                return default_result

            # 检查是否有错误信息（回测失败）
            if "errors" in report_json or "error" in report_json:
                error_msg = BacktestRunner._extract_error_message(report_json)
                error_analysis = ErrorAnalyzer.analyze_error(error_msg, report_json)

                default_result["error_message"] = error_msg
                default_result["error_analysis"] = error_analysis
                default_result["alpha_id"] = report_json.get("id")

                # 尝试提取表达式
                if "regular" in report_json:
                    regular = report_json["regular"]
                    if isinstance(regular, dict):
                        default_result["expression"] = regular.get("code", "")
                    else:
                        default_result["expression"] = str(regular)

                logging.warning(f"回测失败: {error_msg[:100]}")
                return default_result

            if "is" not in report_json:
                logging.warning(f"回测报告缺少 'is' 字段: {report_json.get('id', 'unknown')}")
                default_result["alpha_id"] = report_json.get("id")
                return default_result

            is_data = report_json.get("is", {})

            # 安全提取表达式
            regular = report_json.get("regular", "")
            if isinstance(regular, dict):
                expression = regular.get("code", "")
            else:
                expression = str(regular) if regular else ""

            # 安全提取指标（使用默认值）
            def safe_get(data, key, default=0.0):
                """安全获取数值，失败返回默认值"""
                try:
                    value = data.get(key, default)
                    return float(value) if value is not None else default
                except (ValueError, TypeError):
                    logging.warning(f"无法解析字段 {key}: {data.get(key)}")
                    return default

            # 解析 coverage（供频率检测等使用）
            coverage = safe_get(is_data, "coverage", 0.0)

            # 解析检查项
            checks = is_data.get("checks", [])
            if not isinstance(checks, list):
                checks = []

            failed_checks = [c for c in checks if isinstance(c, dict) and c.get("result") == "FAIL"]
            passed_checks = [c for c in checks if isinstance(c, dict) and c.get("result") == "PASS"]

            # 构建结果
            result = {
                "alpha_id": report_json.get("id", ""),
                "expression": expression,
                "sharpe": safe_get(is_data, "sharpe", 0.0),
                "fitness": safe_get(is_data, "fitness", 0.0),
                "turnover": safe_get(is_data, "turnover", 0.0),
                "coverage": coverage,
                "returns": safe_get(is_data, "returns", 0.0),
                "drawdown": safe_get(is_data, "drawdown", 0.0),
                "margin": safe_get(is_data, "margin", 0.0),
                "long_count": int(safe_get(is_data, "longCount", 0)),
                "short_count": int(safe_get(is_data, "shortCount", 0)),
                "pnl": safe_get(is_data, "pnl", 0.0),
                "checks": checks,
                "failed_checks": [c.get("name", "unknown") for c in failed_checks],
                "passed_checks": [c.get("name", "unknown") for c in passed_checks],
                "n_failed": len(failed_checks),
                "n_passed": len(passed_checks),
                "settings": report_json.get("settings", {}),
                "raw": report_json,
                "success": True,
                "error_message": None,
                "error_analysis": None,
            }

            return result

        except Exception as e:
            logging.error(f"解析回测报告时发生异常: {e}")
            default_result["error_message"] = str(e)
            default_result["error_analysis"] = ErrorAnalyzer.analyze_error(str(e))
            return default_result

    @staticmethod
    def get_report(session, alpha_id: str) -> dict:
        """
        获取指定 Alpha 的完整回测报告。

        Args:
            session: 已认证的 Session 对象
            alpha_id: Alpha ID

        Returns:
            解析后的结果字典
        """
        raw = ace_lib.get_simulation_result_json(session, alpha_id)
        return BacktestRunner.parse_report(raw)

    # ==================== 结果筛选与排序 ====================

    @staticmethod
    def filter_results(
        results: list[dict],
        min_sharpe: float = None,
        min_fitness: float = None,
        max_turnover: float = None,
    ) -> list[dict]:
        """
        筛选高价值 Alpha 结果。

        Args:
            results: 回测结果列表
            min_sharpe: 最低 Sharpe Ratio
            min_fitness: 最低 Fitness
            max_turnover: 最高 Turnover

        Returns:
            筛选后的结果列表 (按 Sharpe 降序排列)
        """
        min_sharpe = min_sharpe if min_sharpe is not None else MIN_SHARPE
        min_fitness = min_fitness if min_fitness is not None else MIN_FITNESS
        max_turnover = max_turnover if max_turnover is not None else MAX_TURNOVER

        filtered = [
            r for r in results
            if r.get("success", False)
            and r["sharpe"] >= min_sharpe
            and r["fitness"] >= min_fitness
            and r["turnover"] <= max_turnover
        ]
        filtered.sort(key=lambda x: x["sharpe"], reverse=True)
        return filtered

    # ==================== 报告格式化 ====================

    @staticmethod
    def format_report_table(results: list[dict]) -> str:
        """
        将回测结果格式化为表格字符串。

        Args:
            results: 回测结果列表

        Returns:
            Markdown 格式的表格字符串
        """
        if not results:
            return "无回测结果"

        lines = []
        lines.append(f"{'#':<3} {'Alpha ID':<12} {'Sharpe':>7} {'Fitness':>8} {'Turnover':>9} {'失败':>4} 表达式")
        lines.append("-" * 90)

        for i, r in enumerate(results, 1):
            expr = r.get("expression", "N/A")
            if len(expr) > 40:
                expr = expr[:37] + "..."
            lines.append(
                f"{i:<3} {r.get('alpha_id', 'N/A'):<12} "
                f"{r['sharpe']:>7.3f} {r['fitness']:>8.3f} "
                f"{r['turnover']:>8.2%} {r.get('n_failed', '?'):>4} "
                f"{expr}"
            )
        return "\n".join(lines)

    @staticmethod
    def format_report_markdown(results: list[dict], title: str = "回测报告") -> str:
        """
        将回测结果格式化为 Markdown 文档。

        Args:
            results: 回测结果列表
            title: 报告标题

        Returns:
            Markdown 格式的报告字符串
        """
        successful = [r for r in results if r.get("success", False)]
        failed = [r for r in results if not r.get("success", False)]

        lines = [
            f"# {title}",
            f"",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**总计**: {len(results)} 个 Alpha | 成功: {len(successful)} | 失败: {len(failed)}",
            f"",
        ]

        if successful:
            # 排序
            successful.sort(key=lambda x: x["sharpe"], reverse=True)

            # 等级统计
            s_level = [r for r in successful if r["sharpe"] >= 1.5]
            a_plus = [r for r in successful if 1.0 <= r["sharpe"] < 1.5]
            a_level = [r for r in successful if 0.8 <= r["sharpe"] < 1.0]
            b_level = [r for r in successful if 0.5 <= r["sharpe"] < 0.8]

            lines.append("## [STATS] 等级分布")
            lines.append(f"- **S级** (Sharpe≥1.5): {len(s_level)}")
            lines.append(f"- **A+级** (1.0≤Sharpe<1.5): {len(a_plus)}")
            lines.append(f"- **A级** (0.8≤Sharpe<1.0): {len(a_level)}")
            lines.append(f"- **B级** (0.5≤Sharpe<0.8): {len(b_level)}")
            lines.append("")

            # 详细表格
            lines.append("## 📋 回测详情")
            lines.append("")
            lines.append("| # | Alpha ID | Sharpe | Fitness | Turnover | 表达式 |")
            lines.append("|---|----------|--------|---------|----------|--------|")
            for i, r in enumerate(successful, 1):
                expr = r.get("expression", "N/A")
                if len(expr) > 50:
                    expr = expr[:47] + "..."
                # 转义管道符
                expr = expr.replace("|", "\\|")
                lines.append(
                    f"| {i} | {r.get('alpha_id', 'N/A')} | "
                    f"{r['sharpe']:.3f} | {r['fitness']:.3f} | "
                    f"{r['turnover']:.2%} | `{expr}` |"
                )

        lines.append("")
        return "\n".join(lines)

    # ==================== 研究归档 ====================

    @staticmethod
    def save_research(
        results: list[dict],
        region: str,
        dataset: str = "mixed",
        notes: str = "",
    ) -> Path:
        """
        将回测结果保存为研究归档。

        Args:
            results: 回测结果列表
            region: 区域代码
            dataset: 数据集名称
            notes: 研究备注

        Returns:
            归档目录路径
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        research_dir = RESEARCH_DIR / f"{region}_{dataset}_{timestamp}"
        research_dir.mkdir(parents=True, exist_ok=True)

        # 保存配置
        config = {
            "region": region,
            "dataset": dataset,
            "timestamp": timestamp,
            "total_alphas": len(results),
            "successful": len([r for r in results if r.get("success")]),
            "notes": notes,
        }
        with open(research_dir / "config.json", "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)

        # 保存结果 (不含 raw 数据以节省空间)
        results_clean = []
        for r in results:
            clean = {k: v for k, v in r.items() if k != "raw"}
            results_clean.append(clean)
        with open(research_dir / "results.json", "w", encoding="utf-8") as f:
            json.dump(results_clean, f, ensure_ascii=False, indent=2)

        # 保存 Markdown 报告
        report_md = BacktestRunner.format_report_markdown(
            results, title=f"{region}/{dataset} 回测报告"
        )
        with open(research_dir / "report.md", "w", encoding="utf-8") as f:
            f.write(report_md)

        print(f"\n💾 研究已归档到: {research_dir}")
        return research_dir

    # ==================== 结果过滤工具 ====================

    @staticmethod
    def filter_results_by_criteria(
        results: list[dict],
        min_sharpe: float | None = None,
        min_fitness: float | None = None,
        max_turnover: float | None = None,
    ) -> list[dict]:
        """
        按自定义阈值过滤回测结果。

        仅保留 success=True 的结果，并根据 Sharpe / Fitness / Turnover 做筛选。

        Args:
            results: 回测结果列表（通常来自 save_research 写入的 results.json）
            min_sharpe: 最小 Sharpe，None 表示不限制
            min_fitness: 最小 Fitness，None 表示不限制
            max_turnover: 最大换手率（同 results 中的单位，若为字符串百分比则会自动解析）

        Returns:
            过滤后的结果列表
        """

        def _parse_turnover(value: object) -> float:
            """
            将 turnover 字段统一转为浮点数（百分比数值）。

            支持:
                - float/int: 直接返回
                - 字符串: "79.89%" 或 "79.89" → 79.89
            """
            if value is None:
                return 0.0
            if isinstance(value, (int, float)):
                return float(value)
            s = str(value).strip()
            if not s:
                return 0.0
            if s.endswith("%"):
                s = s[:-1]
            try:
                return float(s)
            except ValueError:
                return 0.0

        out: list[dict] = []
        for r in results:
            if not r.get("success"):
                continue
            sharpe = float(r.get("sharpe", 0.0) or 0.0)
            fitness = float(r.get("fitness", 0.0) or 0.0)
            turnover = _parse_turnover(r.get("turnover"))

            if min_sharpe is not None and sharpe < min_sharpe:
                continue
            if min_fitness is not None and fitness < min_fitness:
                continue
            if max_turnover is not None and turnover > max_turnover:
                continue
            out.append(r)

        return out

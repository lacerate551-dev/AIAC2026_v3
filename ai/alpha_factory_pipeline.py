# -*- coding: utf-8 -*-
"""
Alpha Factory Pipeline
整合：AI 数据分析 → 模板调度 → Alpha 批量生成 → 去重 → 聚类 → 批量回测 → 筛选 → 错误自愈 → 保存高质量 Alpha。
各步骤可独立调用；流水线结束后输出 research_report.json。
"""
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# 默认报告输出目录（可被 run_pipeline 覆盖）
DEFAULT_RESEARCH_DIR = Path(__file__).resolve().parent.parent / "research"


def _compute_coverage_stats(meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """从 metadata 计算 dataset_statistics（含 coverage 统计）。"""
    if not meta:
        return {"dataset_count": 0, "field_count": 0, "average_coverage": 0.0, "min_coverage": 0.0, "max_coverage": 0.0}
    dm = meta.get("dataset_metadata") or []
    fm = meta.get("field_metadata") or []
    coverages = []
    for d in dm:
        c = d.get("coverage")
        if c is not None and isinstance(c, (int, float)):
            coverages.append(float(c))
    for f in fm:
        c = f.get("coverage")
        if c is not None and isinstance(c, (int, float)):
            coverages.append(float(c))
    if not coverages:
        return {
            "dataset_count": len(dm),
            "field_count": len(fm),
            "average_coverage": 0.0,
            "min_coverage": 0.0,
            "max_coverage": 0.0,
        }
    return {
        "dataset_count": len(dm),
        "field_count": len(fm),
        "average_coverage": sum(coverages) / len(coverages),
        "min_coverage": min(coverages),
        "max_coverage": max(coverages),
    }


def _is_coverage_low(meta: Optional[Dict[str, Any]], threshold: float) -> bool:
    """判断当前 metadata 的 coverage 是否低于阈值（平均或最小）。"""
    stats = _compute_coverage_stats(meta)
    avg = stats.get("average_coverage", 0) or 0
    mn = stats.get("min_coverage", 0) or 0
    return avg < threshold or mn < threshold


def _trim_recommended_fields(
    recommended: List[Dict[str, Any]],
    max_fields: int,
) -> Tuple[List[Dict[str, Any]], int, int]:
    """按 priority 升序排序后保留前 max_fields 个。Returns (trimmed_list, total, used)."""
    total = len(recommended)
    if total <= max_fields or max_fields <= 0:
        return (recommended, total, len(recommended))
    sorted_list = sorted(recommended, key=lambda x: int(x.get("priority", 3)))
    return (sorted_list[:max_fields], total, max_fields)


def _build_field_type_index(meta: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """从 metadata 构建 field_id -> normalized_type 映射。"""
    index: Dict[str, str] = {}
    if not meta:
        return index
    for row in meta.get("field_metadata", []):
        fid = str(row.get("field_id") or "").strip()
        fname = str(row.get("field_name") or "").strip()
        ftype = str(row.get("normalized_type") or "").strip().lower()
        if not ftype:
            # 兼容旧 metadata（未包含 normalized_type）
            ftype = str(row.get("type") or "").strip().lower()
        if fid:
            index[fid] = ftype
        if fname:
            index[fname] = ftype
    return index


def _fallback_vector_fields_from_meta(
    meta: Optional[Dict[str, Any]],
    limit: int,
) -> List[Dict[str, Any]]:
    """
    当 AI 推荐字段为空/被过滤为空时，从 metadata 中回退挑选一批 vector/event 字段用于生成 Alpha，
    以保证 pv1 等核心量价数据集以及 analyst16 等 event 类型数据集不因类型机制回退而无法生成 Alpha。
    """
    if not meta or limit <= 0:
        return []
    candidates = []
    for row in meta.get("field_metadata", []) or []:
        nt = str(row.get("normalized_type") or "").strip().lower()
        # 支持 vector 和 event 类型字段
        if nt not in ("vector", "event"):
            continue
        fid = str(row.get("field_id") or row.get("field_name") or "").strip()
        if not fid:
            continue
        cov = row.get("coverage", 0.0) or 0.0
        candidates.append((float(cov), fid, nt))
    if not candidates:
        return []
    # coverage 高的优先；同 coverage 下按名称稳定排序
    candidates.sort(key=lambda x: (-x[0], x[1]))
    picked = []
    for _, fid, nt in candidates[:limit]:
        picked.append({
            "dataset_id": "",
            "field_id": fid,
            "reason": f"fallback: 从 metadata 中自动挑选的 {nt} 字段（AI 推荐为空/被过滤）",
            "priority": 3,
            "normalized_type": nt,
        })
    return picked


def _filter_recommended_by_type(
    recommended: List[Dict[str, Any]],
    meta: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    根据 field_metadata 中的 normalized_type 过滤推荐字段。

    过滤规则：
    - vector 类型：保留，可直接用于数值计算（BRAIN 平台的 MATRIX 类型）
    - event 类型：保留，可用 vec_* 操作符转换为 vector 后使用（BRAIN 平台的 VECTOR 类型）
    - group 类型：保留，可用于 group_neutralize 等分组操作
    - symbol 类型：过滤掉，不能用于数值计算
    - 未知类型：保留（由下游类型检查器处理）
    """
    if not recommended or not meta:
        return recommended
    type_index = _build_field_type_index(meta)
    # 只过滤 symbol 类型：标识符字段不能用于数值计算
    # event 类型保留：可用 vec_* 模板处理
    disallow_types = ("symbol",)

    def _is_allowed(field_id: str) -> bool:
        t = type_index.get(field_id, "").lower()
        if not t:
            # 未标明类型的字段默认允许（由下游类型检查器处理）
            return True
        return t not in disallow_types

    filtered: List[Dict[str, Any]] = []
    for item in recommended:
        fid = str(item.get("field_id") or item.get("field_name") or "").strip()
        if not fid:
            continue
        if _is_allowed(fid):
            filtered.append(item)
    return filtered


# ==================== 步骤 1：AI 数据分析 ====================
def step_ai_analysis(
    session,
    region: str,
    dataset_ids: List[str],
    data_manager,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
    ai_researcher=None,
    return_metadata: bool = False,
) -> Tuple[Optional[Dict], Optional[List[Dict]], Optional[Dict]]:
    """
    选择有效 fields（AI 数据分析）。可独立调用。

    Returns:
        (analysis_result, recommended_fields[, meta])；
        若 return_metadata=True 则第三项为构建的 metadata（dataset_metadata + field_metadata），否则为 None；
        失败返回 (None, None, None)。
    """
    try:
        from ai.metadata_builder import build_metadata_for_region_datasets
        from ai.data_analysis import analyze_metadata
        meta = build_metadata_for_region_datasets(
            session, region, dataset_ids, data_manager,
            universe=universe, delay=delay, force_refresh=False, use_backtest_frequency=False,
        )
        if not meta.get("field_metadata"):
            return (None, None, meta if return_metadata else None)
        if ai_researcher is None:
            from ai.researcher_brain import AIResearcher
            ai_researcher = AIResearcher()
        analysis = analyze_metadata(
            meta["dataset_metadata"], meta["field_metadata"],
            region, ai_researcher,
        )
        if analysis.get("error"):
            return (analysis, None, meta if return_metadata else None)
        recommended = analysis.get("recommended_fields") or []
        return (analysis, recommended, meta if return_metadata else None)
    except Exception as e:
        logger.exception("AI 数据分析失败: %s", e)
        return (None, None, None)


# ==================== 步骤 2：模板调度 ====================
def step_template_schedule(
    templates_per_round: Optional[int] = None,
    distribution: Optional[Dict[str, int]] = None,
    template_mode: str = "default",
    dataset_id: Optional[str] = None,
    templates_path: Optional[str] = None,
    recommended_fields: Optional[List[Dict[str, Any]]] = None,
    return_metadata: bool = True,
) -> Tuple[List, Dict[str, int]]:
    """
    随机选择 20~30 个模板，覆盖不同类别。可独立调用。

    Args:
        templates_per_round: 每轮模板数
        distribution: 各类别分布
        template_mode: 模板模式 ("default" 或 "specialized")
        dataset_id: 数据集 ID（用于 specialized 模式）
        templates_path: 自定义模板路径
        recommended_fields: 推荐字段列表（用于智能选择 VECTOR 模板）
        return_metadata: 是否返回完整模板对象（含 field_hints），默认 True

    Returns:
        (scheduled_templates, actual_distribution)
        - 当 return_metadata=True 时，返回完整模板对象列表
        - 当 return_metadata=False 时，返回表达式字符串列表（向后兼容）
    """
    from ai.template_scheduler import schedule_templates, schedule_templates_with_metadata
    from ai.template_loader import load_mixed_templates

    # 检查是否有 VECTOR 类型字段
    has_event_fields = False
    if recommended_fields:
        for f in recommended_fields:
            ft = (f.get("normalized_type") or "").lower()
            if ft == "event":
                has_event_fields = True
                break

    # 加载模板（自动包含 VECTOR 模板如果需要）
    templates, source = load_mixed_templates(
        template_mode=template_mode,
        dataset_id=dataset_id,
        templates_path=templates_path,
        include_vector=has_event_fields,
    )

    if not templates:
        logger.warning(f"模板加载失败或为空，来源: {source}")
        return [], {}

    logger.info(f"模板调度: 加载 {len(templates)} 个模板，来源: {source}, VECTOR支持={has_event_fields}")

    # 根据返回类型选择调度函数
    if return_metadata:
        return schedule_templates_with_metadata(
            templates,
            templates_per_round=templates_per_round,
            distribution=distribution,
        )
    else:
        # 向后兼容：只返回表达式字符串
        template_expressions = [t.get("expression") for t in templates if t.get("expression")]
        return schedule_templates(
            template_expressions,
            templates_per_round=templates_per_round,
            distribution=distribution,
        )


# ==================== 步骤 3：Alpha 批量生成 ====================
def step_alpha_generation(
    recommended_fields: List[Dict[str, Any]],
    template_expressions_or_templates: List,
    template_params: Optional[Dict[str, List]] = None,
    max_two_field_pairs: int = 30,
) -> List[Dict[str, Any]]:
    """
    从推荐字段 + 模板生成 Alpha 列表（未去重）。可独立调用。

    支持两种输入格式：
    - 表达式字符串列表（向后兼容）
    - 带 field_hints 的模板对象列表（优先）

    Args:
        recommended_fields: 推荐字段列表
        template_expressions_or_templates: 模板表达式列表或模板对象列表
        template_params: 模板参数
        max_two_field_pairs: 双字段组合上限

    Returns:
        Alpha 列表
    """
    if not template_expressions_or_templates:
        return []

    # 检测输入类型
    first_item = template_expressions_or_templates[0]
    if isinstance(first_item, dict) and "expression" in first_item:
        # 新格式：带 field_hints 的模板对象列表
        from ai.alpha_generator import generate_alphas_from_templates_with_hints
        return generate_alphas_from_templates_with_hints(
            template_expressions_or_templates,
            recommended_fields,
            template_params=template_params,
            max_two_field_pairs=max_two_field_pairs,
        )
    else:
        # 旧格式：表达式字符串列表（向后兼容）
        from ai.alpha_generator import generate_alphas_from_expressions
        return generate_alphas_from_expressions(
            template_expressions_or_templates,
            recommended_fields,
            template_params=template_params,
            max_two_field_pairs=max_two_field_pairs,
        )


# ==================== 步骤 4：Alpha 去重 ====================
def step_dedup(
    alpha_items: List[Dict[str, Any]],
    max_per_structure: int = 3,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """按结构去重。可独立调用。Returns (deduped_list, stats)."""
    from ai.alpha_deduplicator import deduplicate
    return deduplicate(alpha_items, max_per_structure=max_per_structure)


# ==================== 步骤 5：Alpha 聚类 ====================
def step_cluster(
    alpha_items: List[Dict[str, Any]],
    report_path: Optional[Path] = None,
) -> Dict[str, int]:
    """聚类并可选写报告。可独立调用。Returns cluster counts."""
    from ai.alpha_cluster import cluster_alphas, run_cluster_and_report
    counts, _ = cluster_alphas(alpha_items)
    if report_path is not None:
        run_cluster_and_report(alpha_items, report_path=report_path)
    return counts


# ==================== 步骤 6：批量回测 ====================
def step_backtest(
    session,
    alpha_items: List[Dict[str, Any]],
    region: str,
    output_dir: Optional[str] = None,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
    resume: bool = False,
) -> List[Dict[str, Any]]:
    """批量回测。可独立调用。Returns 回测结果列表。"""
    from ai.backtest_loop import run_batch_backtest
    return run_batch_backtest(
        session, alpha_items, region,
        output_dir=output_dir, universe=universe, delay=delay,
        resume=resume,
    )


# ==================== 步骤 7：Alpha 筛选 ====================
def step_filter(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """筛选高价值 Alpha（MIN_SHARPE / MIN_FITNESS / MAX_TURNOVER）。可独立调用。"""
    from ai.backtest_loop import filter_high_value
    return filter_high_value(results)


# ==================== 步骤 8：错误自愈 ====================
def step_self_heal(
    failed_results: List[Dict],
    session,
    region: str,
    dataset_id: str,
    ai_researcher=None,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """对失败结果做 AI 自愈并补考。可独立调用。Returns 补考成功结果列表。"""
    if not failed_results:
        return []
    if ai_researcher is None:
        try:
            from ai.researcher_brain import AIResearcher
            ai_researcher = AIResearcher()
        except Exception:
            return []
    from ai.backtest_loop import run_self_heal
    return run_self_heal(
        failed_results, session, region, dataset_id, ai_researcher,
        universe=universe, delay=delay,
    )


# ==================== 步骤 9：保存高质量 Alpha ====================
def step_save_high_quality(
    high_value: List[Dict[str, Any]],
    region: str,
    output_dir: Path,
    tag: str = "pipeline",
) -> Path:
    # region/tag 保留供调用方区分多轮或不同区域归档
    """将高质量 Alpha 结果写入 output_dir 并返回路径。可独立调用。"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "high_quality_alphas.json"
    # 只保存必要字段，不含 raw
    save_list = []
    for r in high_value:
        save_list.append({
            "alpha_id": r.get("alpha_id"),
            "expression": r.get("expression"),
            "sharpe": r.get("sharpe"),
            "fitness": r.get("fitness"),
            "turnover": r.get("turnover"),
        })
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(save_list, f, ensure_ascii=False, indent=2)
    logger.info("高质量 Alpha 已保存: %s", out_file)
    return out_file


# ==================== 流水线入口与报告 ====================
def run_pipeline(
    session,
    region: str,
    dataset_ids: List[str],
    data_manager,
    output_dir: Optional[Path] = None,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
    templates_per_round: Optional[int] = None,
    run_self_heal_flag: bool = True,
    steps: Optional[List[str]] = None,
    template_mode: str = "default",
    templates_path: Optional[str] = None,
    resume: bool = False,
    max_recommended_fields: Optional[int] = None,
) -> Dict[str, Any]:
    """
    运行完整或部分 Alpha Factory 流水线；各步可独立调用。

    Args:
        session: BRAIN 会话
        region: 区域
        dataset_ids: 数据集 ID 列表
        data_manager: DataManager 类或实例
        output_dir: 本轮输出目录，默认 research/alpha_factory_<timestamp>
        universe: 可选
        delay: 可选
        templates_per_round: 每轮模板数，默认 config
        run_self_heal_flag: 是否执行错误自愈
        steps: 要执行的步骤名列表，如 ["ai_analysis","template_schedule",...]；None 表示全部
        template_mode: 模板模式 ("default" 使用通用模板，"specialized" 使用针对性模板)
        templates_path: 自定义模板文件路径（优先级最高）
        resume: 是否从已有输出目录恢复回测进度
        max_recommended_fields: 最大推荐字段数，默认 config

    Returns:
        state 字典，含各步输出及 report 路径；并写入 research_report.json
    """
    try:
        from config.settings import RESEARCH_DIR
        default_out = Path(RESEARCH_DIR)
    except Exception:
        default_out = DEFAULT_RESEARCH_DIR
    out = Path(output_dir) if output_dir is not None else default_out / f"alpha_factory_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    out.mkdir(parents=True, exist_ok=True)

    all_step_names = [
        "ai_analysis", "template_schedule", "alpha_generation", "dedup", "cluster",
        "backtest", "filter", "self_heal", "save_high_quality",
    ]
    run_steps = steps if steps is not None else all_step_names

    state = {
        "region": region,
        "dataset_ids": dataset_ids,
        "output_dir": str(out),
        "recommended_fields": None,
        "scheduled_templates": None,
        "alpha_items_raw": None,
        "alpha_items_deduped": None,
        "cluster_counts": None,
        "backtest_results": None,
        "high_value": None,
        "retry_results": None,
    }
    report = {
        "templates_used": 0,
        "alpha_generated": 0,
        "after_dedup": 0,
        "clusters": 0,
        "backtest_success": 0,
    }

    ai_researcher = None
    pipeline_log_entries: List[Dict[str, Any]] = []
    try:
        from config.alpha_config import (
            COVERAGE_THRESHOLD,
            AUTO_DATASET_EXPANSION,
            MAX_EXPANSION_ROUNDS,
            MAX_RECOMMENDED_FIELDS as DEFAULT_MAX_FIELDS,
            get_dynamic_limits,
        )
    except Exception:
        COVERAGE_THRESHOLD = 0.6
        AUTO_DATASET_EXPANSION = False
        MAX_EXPANSION_ROUNDS = 2
        DEFAULT_MAX_FIELDS = 15
        # 如果导入失败，提供默认的动态计算函数
        def get_dynamic_limits(field_count):
            return {"max_fields": DEFAULT_MAX_FIELDS, "templates_per_round": 25, "field_count": field_count}

    # 动态限制（初始使用参数或默认值，后续根据字段数更新）
    _max_fields = max_recommended_fields if max_recommended_fields is not None else DEFAULT_MAX_FIELDS
    _dynamic_limits = None  # 存储动态计算的配置

    if "ai_analysis" in run_steps:
        current_datasets = list(dataset_ids)
        _analysis = None
        recommended = None
        _meta = None
        expansion_round = 0
        while True:
            _analysis, recommended, _meta = step_ai_analysis(
                session, region, current_datasets, data_manager,
                universe=universe, delay=delay,
                return_metadata=True,
            )
            if not recommended and _analysis is None:
                logger.warning("Pipeline: AI 分析失败或无推荐字段，中止")
                _write_report(out, report, state)
                return state
            if not recommended:
                logger.warning("Pipeline: 无推荐字段，中止")
                _write_report(out, report, state)
                return state
            coverage_low = _meta and _is_coverage_low(_meta, COVERAGE_THRESHOLD)
            neutralization = (_analysis or {}).get("neutralization_datasets") or []
            new_dataset_ids = [str(n.get("dataset_id", "")).strip() for n in neutralization if n.get("dataset_id")]
            should_expand = (
                AUTO_DATASET_EXPANSION
                and coverage_low
                and len(neutralization) > 0
                and expansion_round < MAX_EXPANSION_ROUNDS
                and len(new_dataset_ids) > 0
            )
            if should_expand:
                expanded = list(dict.fromkeys(current_datasets + new_dataset_ids))
                pipeline_log_entries.append({
                    "round": expansion_round + 1,
                    "datasets": list(current_datasets),
                    "coverage_low": coverage_low,
                    "expanded_to": expanded,
                })
                logger.info("Pipeline: 自动扩展数据集 %s -> %s (round %s)", current_datasets, expanded, expansion_round + 1)
                current_datasets = expanded
                expansion_round += 1
            else:
                break
        state["dataset_ids"] = current_datasets
        # 先基于字段类型过滤掉不适合作为模板输入的字段
        # 先基于字段类型过滤掉不适合作为模板输入的字段，并给剩余字段补充 normalized_type（供下游生成）
        recommended = _filter_recommended_by_type(recommended or [], _meta)
        # 将 meta 中的 normalized_type 回填到 recommended_fields，便于 alpha_generator 做 vector 过滤
        if _meta is not None:
            type_index = _build_field_type_index(_meta)
            for item in recommended:
                fid = str(item.get("field_id") or item.get("field_name") or "").strip()
                if fid and "normalized_type" not in item:
                    item["normalized_type"] = type_index.get(fid, "")
        state["recommended_fields"] = recommended
        state["_meta"] = _meta
        state["_analysis"] = _analysis

        # 根据数据集字段数量动态计算推荐字段上限和模板数
        field_metadata = _meta.get("field_metadata", []) if _meta else []
        field_count = len(field_metadata)
        _dynamic_limits = get_dynamic_limits(field_count)

        # 如果用户没有通过参数指定，使用动态计算值
        if max_recommended_fields is None:
            _max_fields = _dynamic_limits["max_fields"]
        else:
            _max_fields = max_recommended_fields

        logger.info(
            "Pipeline: 数据集字段数=%s, 动态限制: max_fields=%s, templates_per_round=%s",
            field_count, _max_fields, _dynamic_limits.get("templates_per_round", 25)
        )

        recommended_trimmed, total_fields, used_fields = _trim_recommended_fields(recommended, _max_fields)
        # 若过滤/截断后为空，则从 metadata 回退挑选 vector 字段，避免 pv1 等核心数据集功能回退
        if not recommended_trimmed:
            fallback = _fallback_vector_fields_from_meta(_meta, _max_fields)
            if fallback:
                logger.warning("AI 推荐字段为空/被过滤为空，已从 metadata 回退选取 %s 个 vector 字段用于生成 Alpha", len(fallback))
                recommended_trimmed = fallback
                # total_fields/used_fields 统计仍保留 AI 原始值；在 ai_analysis_result 中额外标记 fallback_used
                fallback_used = True
            else:
                fallback_used = False
        else:
            fallback_used = False

        state["recommended_fields"] = recommended_trimmed
        out.mkdir(parents=True, exist_ok=True)
        if _analysis is not None:
            analysis_to_save = dict(_analysis)
            analysis_to_save["recommended_fields_total"] = total_fields
            analysis_to_save["recommended_fields_used"] = used_fields
            analysis_to_save["recommended_fields_fallback_used"] = fallback_used
            analysis_to_save["recommended_fields"] = recommended_trimmed
            _save_json(out / "ai_analysis_result.json", analysis_to_save)
        if pipeline_log_entries:
            _save_json(out / "pipeline_log.json", {"expansion_rounds": pipeline_log_entries, "final_datasets": state["dataset_ids"]})
        if _meta is not None:
            _save_json(out / "metadata_snapshot.json", {
                "run_config": {
                    "datasets": state["dataset_ids"],
                    "coverage_threshold": COVERAGE_THRESHOLD,
                    "templates_used": 0,
                    "fields_selected": len(state["recommended_fields"] or []),
                },
                "dataset_statistics": _compute_coverage_stats(_meta),
                "dataset_metadata": _meta.get("dataset_metadata", []),
                "field_metadata": _meta.get("field_metadata", []),
            })
        try:
            from ai.researcher_brain import AIResearcher
            ai_researcher = AIResearcher()
        except Exception:
            pass

    if "template_schedule" in run_steps:
        # 优先使用用户指定的模板数，其次使用动态计算值
        if templates_per_round is not None:
            _tpr = templates_per_round
        elif _dynamic_limits is not None:
            _tpr = _dynamic_limits.get("templates_per_round", 25)
        else:
            try:
                from config.alpha_config import templates_per_round as cfg_tpr
                _tpr = cfg_tpr
            except Exception:
                _tpr = 25

        logger.info("Pipeline: 使用模板数=%s (用户指定=%s, 动态计算=%s)",
                    _tpr, templates_per_round, _dynamic_limits.get("templates_per_round") if _dynamic_limits else None)

        scheduled, _dist = step_template_schedule(
            templates_per_round=_tpr,
            template_mode=template_mode,
            dataset_id=dataset_ids[0] if dataset_ids else None,
            templates_path=templates_path,
            recommended_fields=state.get("recommended_fields"),
        )
        state["scheduled_templates"] = scheduled
        report["templates_used"] = len(scheduled)
        # 记录动态配置信息
        if _dynamic_limits:
            report["dynamic_limits"] = _dynamic_limits
        if not scheduled:
            logger.warning("Pipeline: 无调度模板，中止")
            _write_report(out, report, state)
            return state
        if state.get("_meta") is not None:
            _meta = state["_meta"]
            run_config = {
                "datasets": state.get("dataset_ids", []),
                "coverage_threshold": COVERAGE_THRESHOLD,
                "templates_used": len(scheduled),
                "fields_selected": len(state.get("recommended_fields") or []),
            }
            dataset_statistics = _compute_coverage_stats(_meta)
            _save_json(out / "metadata_snapshot.json", {
                "run_config": run_config,
                "dataset_statistics": dataset_statistics,
                "dataset_metadata": _meta.get("dataset_metadata", []),
                "field_metadata": _meta.get("field_metadata", []),
            })

    if "alpha_generation" in run_steps:
        if not (state["recommended_fields"] or []):
            print("❌ 无可用于生成的 vector 字段（AI 推荐为空且 metadata 回退也未找到）。请更换数据集或检查字段类型映射。")
            _write_report(out, report, state)
            return state
        # 尝试加载数据集特定的回测参数
        dataset_backtest_params = None
        try:
            from ai.template_loader import load_backtest_params
            primary_dataset = dataset_ids[0] if dataset_ids else None
            if primary_dataset:
                dataset_backtest_params = load_backtest_params(primary_dataset)
                if dataset_backtest_params:
                    logger.info("使用数据集 %s 的专用回测参数: %s", primary_dataset, dataset_backtest_params)
        except Exception as e:
            logger.debug("加载数据集回测参数失败: %s", e)

        # 获取双字段组合限制
        try:
            from config.alpha_config import MAX_TWO_FIELD_PAIRS
        except Exception:
            MAX_TWO_FIELD_PAIRS = 30

        raw = step_alpha_generation(
            state["recommended_fields"] or [],
            state["scheduled_templates"] or [],
            template_params=dataset_backtest_params,
            max_two_field_pairs=MAX_TWO_FIELD_PAIRS,
        )
        state["alpha_items_raw"] = raw
        report["alpha_generated"] = len(raw)
        if dataset_backtest_params:
            report["backtest_params_source"] = f"specialized:{dataset_ids[0]}"
        else:
            report["backtest_params_source"] = "default"
        if not raw:
            _write_report(out, report, state)
            return state

    if "dedup" in run_steps:
        # 使用配置中的 MAX_PER_STRUCTURE
        try:
            from config.alpha_config import MAX_PER_STRUCTURE
        except Exception:
            MAX_PER_STRUCTURE = 3
        deduped, _ = step_dedup(state["alpha_items_raw"] or [], max_per_structure=MAX_PER_STRUCTURE)
        state["alpha_items_deduped"] = deduped
        report["after_dedup"] = len(deduped)
        if not deduped:
            _write_report(out, report, state)
            return state

    if "cluster" in run_steps:
        counts = step_cluster(state["alpha_items_deduped"], report_path=out / "alpha_cluster_report.json")
        state["cluster_counts"] = counts
        report["clusters"] = len(counts)

    if "backtest" in run_steps:
        # 过滤掉包含未替换占位符的表达式（如 {field3}）
        placeholder_pattern = re.compile(r"\{[a-zA-Z_][a-zA-Z0-9_]*\}")
        placeholder_filtered = []
        placeholder_errors = []
        for item in state["alpha_items_deduped"] or []:
            expr = item.get("expression") or ""
            if placeholder_pattern.search(expr):
                placeholder_errors.append({"expression": expr, "error": "包含未替换的占位符"})
                logger.warning("跳过包含未替换占位符的表达式: %s", expr[:100])
            else:
                placeholder_filtered.append(item)
        if placeholder_errors:
            logger.warning("占位符检查：跳过 %s 条包含未替换占位符的表达式", len(placeholder_errors))
            state["alpha_items_deduped"] = placeholder_filtered

        # 回测前类型检查：用本轮 metadata 的 field_metadata + operators_metadata.json 静态校验表达式
        try:
            from core.type_checker import load_operator_metadata, build_field_type_index, check_expression_types
            op_meta = load_operator_metadata()
            field_index = build_field_type_index((state.get("_meta") or {}).get("field_metadata", []))
            kept = []
            type_errors = []
            for item in state["alpha_items_deduped"] or []:
                expr = item.get("expression") or ""
                ok, err = check_expression_types(expr, field_index, operator_metadata=op_meta)
                if ok:
                    kept.append(item)
                else:
                    type_errors.append({"expression": expr, "error": err})
                    logger.warning("类型检查失败，跳过表达式: %s | reason=%s", expr[:160], err)
            if type_errors:
                _save_json(out / "type_check_report.json", {
                    "total": len(state["alpha_items_deduped"] or []),
                    "kept": len(kept),
                    "skipped": len(type_errors),
                    "errors": type_errors[:200],  # 防止文件过大
                })
                logger.warning("类型检查：跳过 %s 条表达式（已写入 type_check_report.json）", len(type_errors))
            state["alpha_items_deduped"] = kept
        except Exception as e:
            logger.warning("类型检查器跳过（宽松模式继续回测）: %s", e)

        if not (state["alpha_items_deduped"] or []):
            print("❌ 类型检查后无可回测的 Alpha 表达式，本轮跳过回测。")
            _write_report(out, report, state)
            return state

        # 保存 alpha_items 以便恢复
        _save_json(out / "alpha_items.json", state["alpha_items_deduped"])
        logger.info(f"已保存 {len(state['alpha_items_deduped'])} 个 Alpha 到 alpha_items.json")

        results = step_backtest(
            session,
            state["alpha_items_deduped"],
            region,
            output_dir=str(out),
            universe=universe,
            delay=delay,
            resume=resume,
        )
        state["backtest_results"] = results
        report["backtest_success"] = len([r for r in results if r.get("success")])

    if "filter" in run_steps:
        try:
            from config.settings import MIN_SHARPE, MIN_FITNESS, MAX_TURNOVER
        except ImportError:
            MIN_SHARPE, MIN_FITNESS, MAX_TURNOVER = 1.0, 0.5, 0.70
        high = step_filter(state["backtest_results"] or [])
        state["high_value"] = high
        # 打印筛选详情
        total_success = len([r for r in state["backtest_results"] or [] if r.get("success")])
        print(f"\n[STATS] 筛选结果: {len(high)}/{total_success} 个 Alpha 符合条件")
        print(f"        条件: Sharpe≥{MIN_SHARPE}, Fitness≥{MIN_FITNESS}, Turnover≤{MAX_TURNOVER:.0%}")
        if len(high) == 0 and total_success > 0:
            print(f"        提示: 虽有 {total_success} 个回测成功，但均未达到完整筛选标准")
            # 分析原因
            success_results = [r for r in state["backtest_results"] or [] if r.get("success")]
            low_fitness = len([r for r in success_results if r.get("fitness", 0) < MIN_FITNESS])
            high_turnover = len([r for r in success_results if r.get("turnover", 1) > MAX_TURNOVER])
            if low_fitness > 0:
                print(f"        - {low_fitness} 个 Fitness < {MIN_FITNESS}")
            if high_turnover > 0:
                print(f"        - {high_turnover} 个 Turnover > {MAX_TURNOVER:.0%}")

    if "self_heal" in run_steps and run_self_heal_flag and state.get("backtest_results"):
        failed = [r for r in state["backtest_results"] if not r.get("success")]
        final_dataset_ids = state.get("dataset_ids") or dataset_ids
        if failed and final_dataset_ids and ai_researcher:
            retry = step_self_heal(
                failed, session, region, final_dataset_ids[0],
                ai_researcher=ai_researcher, universe=universe, delay=delay,
            )
            state["retry_results"] = retry
            if retry:
                state["backtest_results"] = (state["backtest_results"] or []) + retry
                report["backtest_success"] = len([r for r in state["backtest_results"] if r.get("success")])
                state["high_value"] = step_filter(state["backtest_results"])

    if "save_high_quality" in run_steps and state.get("high_value") is not None:
        step_save_high_quality(state["high_value"], region, out, tag="pipeline")

    _write_report(out, report, state)
    return state


def _save_json(path: Path, data: Dict[str, Any]) -> None:
    """将字典写入 JSON 文件。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _write_report(output_dir: Path, report: Dict[str, Any], state: Dict[str, Any]) -> None:
    """写入 research_report.json（仅报告字段，不含大列表）+ 完整回测结果。"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 保存完整回测结果（包含 alpha_id, sharpe, fitness, turnover 等）
    backtest_results = state.get("backtest_results") or []
    if backtest_results:
        backtest_path = output_dir / "backtest_results.json"
        # 精简保存：只保留关键字段
        simplified_results = []
        for r in backtest_results:
            if r and r.get("alpha_id"):
                simplified_results.append({
                    "alpha_id": r.get("alpha_id"),
                    "expression": r.get("expression"),
                    "sharpe": r.get("sharpe"),
                    "fitness": r.get("fitness"),
                    "turnover": r.get("turnover"),
                    "decay": r.get("decay"),
                    "truncation": r.get("truncation"),
                    "neutralization": r.get("neutralization"),
                    "success": r.get("success"),
                    "checks": r.get("checks", []),
                })
        with open(backtest_path, "w", encoding="utf-8") as f:
            json.dump(simplified_results, f, ensure_ascii=False, indent=2)
        logger.info(f"回测结果已保存: {backtest_path} ({len(simplified_results)} 条)")

    # 保存报告摘要
    path = output_dir / "research_report.json"
    payload = {
        "templates_used": report.get("templates_used", 0),
        "alpha_generated": report.get("alpha_generated", 0),
        "after_dedup": report.get("after_dedup", 0),
        "clusters": report.get("clusters", 0),
        "backtest_success": report.get("backtest_success", 0),
        "region": state.get("region"),
        "dataset_ids": state.get("dataset_ids"),
        "output_dir": state.get("output_dir"),
        "timestamp": datetime.now().isoformat(),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("研究报告已写入: %s", path)

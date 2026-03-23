# -*- coding: utf-8 -*-
"""
AI Data Analysis Module
- Input: dataset metadata + field metadata (built by metadata_builder)
- Output: valid field list (BRAIN field id) + neutralization dataset recommendations
- Does not generate Alpha expressions, signal_id, window - only recommends fields to reduce token usage

v2 增强：针对特定数据集注入研究方向引导 prompt，指导 AI 按特定策略推荐字段组合。
v3 增强：研究方向引导从配置文件加载，支持动态扩展。
"""
import json
import logging
from typing import Dict, Any, List, Optional

from config.alpha_config import COVERAGE_THRESHOLD
from ai.template_loader import get_guidance_prompt

logger = logging.getLogger(__name__)


def _build_research_guidance(dataset_ids: set) -> str:
    """
    构建数据集特定的研究方向引导 prompt。
    优先从配置文件加载，如果不存在则使用内置的 analyst10 引导（向后兼容）。

    Args:
        dataset_ids: 数据集 ID 集合（小写）

    Returns:
        研究方向引导 prompt 字符串
    """
    # 按数据集顺序尝试加载引导
    for did in sorted(dataset_ids):
        prompt = get_guidance_prompt(did)
        if prompt:
            logger.info(f"从配置加载研究方向引导: {did}")
            return prompt

    # 如果没有配置文件，检查是否有 analyst10（向后兼容）
    if "analyst10" in dataset_ids:
        return _build_analyst10_research_prompt_fallback()

    return ""


def _build_analyst10_research_prompt_fallback() -> str:
    """
    analyst10 数据集的研究方向引导 prompt（内置回退，向后兼容）。
    优先使用配置文件 config/dataset_templates/analyst10_guidance.json。
    """
    return """
**Analyst10 数据集研究方向指引（重要）：**
此数据集为 Performance-Weighted Analyst Estimates，包含分析师预测数据。请按以下三大研究方向推荐字段组合：

**研究方向 1：Smart Estimates vs Consensus 差异信号**
- 选择配对字段：`*_smart_ests_*` 与对应的 `*_consensus_*`
- 示例配对：`anl10_epsfq1_smart_ests_v0_*` 与 `anl10_epsfq1_consensus_*`
- Alpha 逻辑：智能预测与市场共识的差异可能蕴含信息优势
- 推荐操作：同时推荐同一指标（如 eps、revenue）的 smart_ests 和 consensus 字段，便于生成差值/比率类 alpha

**研究方向 2：Predicted Surprises 预测偏差**
- 选择 `*_pred_surps_*` 系列字段（已计算为 (smart_ests - consensus)/consensus）
- Alpha 逻辑：预测偏差直接捕捉市场预期差，是最直接的 alpha 信号
- 推荐操作：优先推荐不同版本（v0/v1/v2）和不同周期（fq1-fq4, fy1-fy2）的 pred_surps 字段

**研究方向 3：周期对比（季度 vs 年度）**
- 配对季度字段 (fq1/fq2/fq3/fq4) 与年度字段 (fy1/fy2)
- Alpha 逻辑：短期预期与长期预期的分歧可能预示趋势变化
- 推荐操作：推荐同一指标的季度和年度字段组合

**字段选择优先级：**
1. 覆盖率 > 60% 且 userCount > 10 的字段优先
2. 每个研究方向至少推荐 3-5 个高价值字段
3. 对于研究方向 1 和 3，确保推荐的字段可以配对使用（双字段模板）
"""


def analyze_metadata(
    dataset_metadata: List[Dict[str, Any]],
    field_metadata: List[Dict[str, Any]],
    region: str,
    ai_researcher,
    coverage_threshold: float = None,
) -> Dict[str, Any]:
    """
    AI analysis based on dataset + field metadata, output recommended fields and neutralization datasets.

    Args:
        dataset_metadata: dataset level list, each item contains dataset_id, dataset_name, category, coverage, region, frequency
        field_metadata: field level list, each item contains dataset_id, field_id, field_name, description, coverage, type
        region: region code
        ai_researcher: AI researcher instance with _call_ai(prompt, json_mode=True) method
        coverage_threshold: coverage threshold, defaults to config.alpha_config.COVERAGE_THRESHOLD

    Returns:
        {
            "recommended_fields": [
                {"dataset_id": "...", "field_id": "...", "reason": "...", "priority": 1-5},
                ...
            ],
            "neutralization_datasets": [
                {"dataset_id": "...", "reason": "..."},
                ...
            ]
        }
        Deduplicated by field_id within single analysis (first occurrence kept).
    """
    if coverage_threshold is None:
        coverage_threshold = COVERAGE_THRESHOLD

    # Control volume sent to AI: all datasets, truncated fields to avoid token explosion
    # Field type priority reordering:
    # - VECTOR/MATRIX types first (numeric time series, can be used directly for factor calculation)
    # - GROUP types next (can be used for grouping neutralization)
    # - SYMBOL/EVENT types last (not suitable for direct numeric factor use)
    max_fields_for_prompt = 200

    def _field_priority(row: Dict[str, Any]) -> int:
        """Field sorting priority: numeric types before grouping/identifier types"""
        t = str(row.get("type") or row.get("normalized_type") or "").upper()
        # VECTOR/MATRIX are numeric time series, best for factor calculation
        if t in ("VECTOR", "MATRIX"):
            return 1
        # GROUP can be used for grouping operations
        if t == "GROUP":
            return 2
        # Other types go last
        return 3

    sorted_fields = sorted(field_metadata, key=_field_priority) if field_metadata else []
    fields_for_prompt = sorted_fields[:max_fields_for_prompt]
    if len(field_metadata) > max_fields_for_prompt:
        logger.info(
            f"field_metadata has {len(field_metadata)} items, only sending top {max_fields_for_prompt} "
            f"to AI after priority sorting (VECTOR/MATRIX numeric types first)"
        )

    # 检测数据集，加载研究方向引导
    dataset_ids = set()
    for d in dataset_metadata:
        did = str(d.get("dataset_id") or d.get("id") or "").strip().lower()
        if did:
            dataset_ids.add(did)

    # 从配置文件加载研究方向引导（支持动态扩展）
    research_guidance = _build_research_guidance(dataset_ids)
    if research_guidance:
        logger.info(f"已注入研究方向引导 prompt，数据集: {dataset_ids}")

    prompt = f"""You are a quantitative researcher. Based on the dataset and field metadata below, recommend fields suitable for Alpha factor mining (BRAIN field id).
{research_guidance}
**Field Type Descriptions (Important - Verified 2026-03-19):**
- `MATRIX`: Numeric time series fields, can be used directly with ts_mean/ts_delta/rank/zscore/scale operators
- `VECTOR`: Event-type data, **requires vec_* operators (vec_sum/vec_avg/vec_count) to convert to vector before using rank/ts_mean/etc.**
  - Wrong: `rank(vector_field)` → Error "does not support event inputs"
  - Correct: `rank(vec_sum(vector_field))` or `rank(vec_avg(vector_field))`
- `GROUP`: Grouping fields (e.g., industry, sector), only for second argument of group_neutralize
- `SYMBOL`: Identifier fields (e.g., ticker, cusip), cannot be used for numeric calculations

**Operator Type Requirements Examples:**
- `rank(signal)`: signal must be MATRIX type (or vec_* converted VECTOR)
- `ts_mean(field, window)`: field must be MATRIX type
- `vec_sum(vector_field)`: converts VECTOR to vector, then can use rank/ts_mean
- `ts_corr(field1, field2, window)`: field1, field2 must both be MATRIX type
- `group_neutralize(signal, group)`: signal must be numeric type, group must be GROUP type

**Recommendation Rules:**
1. **Prioritize fields with type MATRIX**, these can be used directly in most Alpha templates
2. **For VECTOR type fields**, note that they require vec_* conversion in Alpha expressions
3. Carefully recommend GROUP type fields, only when grouping neutralization is needed
4. **Do NOT recommend SYMBOL type fields for factor calculation**
5. Consider field coverage, prioritize high-coverage fields
6. If any dataset or field coverage < {coverage_threshold}, recommend other datasets in neutralization_datasets

**Region:** {region}

**dataset_metadata (dataset level):**
{json.dumps(dataset_metadata, ensure_ascii=False, indent=2)}

**field_metadata (field level, top {len(fields_for_prompt)} items, sorted by type priority):**
{json.dumps(fields_for_prompt, ensure_ascii=False, indent=2)}

**Output Format (strict JSON):**
{{
  "recommended_fields": [
    {{ "dataset_id": "dataset_id", "field_id": "field_id", "field_type": "vector/group/symbol", "reason": "brief reason", "priority": 1 }}
  ],
  "neutralization_datasets": [
    {{ "dataset_id": "dataset_id", "reason": "brief reason" }}
  ]
}}

Output only the JSON above, no other text."""

    logger.info(
        "Calling AI to analyze metadata (recommend fields + neutralization datasets), expected 10-60 seconds..."
    )
    try:
        raw = ai_researcher._call_ai(prompt, json_mode=True)
    except Exception as e:
        err_type = type(e).__name__
        err_msg = str(e)
        logger.error(
            "AI metadata analysis failed | Error type: %s | Details: %s | "
            "Please check config/api_keys.json, network, and config/ai_config.py API_CONFIG (timeout/max_retries)",
            err_type,
            err_msg,
        )
        logger.exception("AI metadata analysis failed (full stack)")
        return {
            "recommended_fields": [],
            "neutralization_datasets": [],
            "error": f"{err_type}: {err_msg}",
        }

    recommended = raw.get("recommended_fields") or []
    neutralization = raw.get("neutralization_datasets") or []

    # 构建 field_id -> normalized_type 映射
    field_type_map = {}
    for fm in field_metadata or []:
        fid = fm.get("field_id") or fm.get("id") or fm.get("field_name")
        if fid:
            nt = fm.get("normalized_type") or ""
            if not nt:
                # 尝试从 type 字段推断
                raw_type = fm.get("type") or ""
                if raw_type.upper() == "MATRIX":
                    nt = "vector"
                elif raw_type.upper() == "VECTOR":
                    nt = "event"
            field_type_map[str(fid)] = nt

    # Deduplicate by field_id within single analysis (keep first occurrence)
    seen = set()
    deduped = []
    for item in recommended:
        if not isinstance(item, dict):
            continue
        fid = item.get("field_id") or item.get("field_name")
        if fid in seen:
            continue
        seen.add(fid)

        # 从 field_metadata 回填 normalized_type
        normalized_type = item.get("field_type") or item.get("normalized_type") or ""
        if not normalized_type:
            normalized_type = field_type_map.get(str(fid), "")

        deduped.append({
            "dataset_id": str(item.get("dataset_id", "")),
            "field_id": str(item.get("field_id") or item.get("field_name", "")),
            "reason": str(item.get("reason", "")),
            "priority": int(item.get("priority", 3)) if isinstance(item.get("priority"), (int, float)) else 3,
            "normalized_type": normalized_type,
        })

    # Limit priority to 1-5
    for r in deduped:
        r["priority"] = max(1, min(5, r["priority"]))

    neutralization_out = []
    for n in neutralization:
        if not isinstance(n, dict):
            continue
        neutralization_out.append({
            "dataset_id": str(n.get("dataset_id", "")),
            "reason": str(n.get("reason", "")),
        })

    return {
        "recommended_fields": deduped,
        "neutralization_datasets": neutralization_out,
    }
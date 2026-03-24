# -*- coding: utf-8 -*-
"""
模板加载器模块
- 支持通用模板（default）
- 支持针对性模板（specialized，按数据集自动查找）
- 支持自定义路径模板
- 支持研究方向引导加载
"""
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# 默认模板路径
DEFAULT_TEMPLATES_PATH = Path(__file__).parent.parent / "config" / "templates.json"
# 针对性模板目录
DATASET_TEMPLATES_DIR = Path(__file__).parent.parent / "config" / "dataset_templates"
# VECTOR 类型专用模板路径
VECTOR_TEMPLATES_PATH = Path(__file__).parent.parent / "config" / "vector_templates.json"


def load_templates(
    template_mode: str = "default",
    dataset_id: Optional[str] = None,
    templates_path: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    加载模板列表。

    Args:
        template_mode: 模板模式
            - "default": 使用通用模板（config/templates.json）
            - "specialized": 使用针对性模板（按数据集查找）
        dataset_id: 数据集 ID（用于 specialized 模式）
        templates_path: 自定义模板文件路径（优先级最高）

    Returns:
        (templates, source_info)
        - templates: 模板列表
        - source_info: 模板来源描述
    """
    # 优先级 1: 自定义路径
    if templates_path:
        path = Path(templates_path)
        if not path.exists():
            logger.warning(f"自定义模板文件不存在: {path}，回退到默认模板")
            return _load_default_templates()
        templates = _load_templates_from_file(path)
        if templates:
            logger.info(f"使用自定义模板: {path}，共 {len(templates)} 个")
            return templates, f"custom:{path}"
        else:
            logger.warning(f"自定义模板文件加载失败: {path}，回退到默认模板")

    # 优先级 2: 针对性模板
    if template_mode == "specialized" and dataset_id:
        specialized_path = DATASET_TEMPLATES_DIR / f"{dataset_id}_templates.json"
        if specialized_path.exists():
            templates = _load_templates_from_file(specialized_path)
            if templates:
                logger.info(f"使用针对性模板: {specialized_path}，共 {len(templates)} 个")
                return templates, f"specialized:{dataset_id}"
            else:
                logger.warning(f"针对性模板加载失败: {specialized_path}")
        else:
            logger.info(f"未找到针对性模板: {specialized_path}，回退到默认模板")

    # 优先级 3: 默认模板
    return _load_default_templates()


def _load_default_templates() -> Tuple[List[Dict[str, Any]], str]:
    """加载默认通用模板。"""
    templates = _load_templates_from_file(DEFAULT_TEMPLATES_PATH)
    if templates:
        logger.info(f"使用默认模板: {DEFAULT_TEMPLATES_PATH}，共 {len(templates)} 个")
        return templates, "default"
    else:
        logger.error(f"默认模板加载失败: {DEFAULT_TEMPLATES_PATH}")
        return [], "default:failed"


def _load_templates_from_file(path: Path) -> List[Dict[str, Any]]:
    """从文件加载模板列表。"""
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        else:
            logger.warning(f"模板文件格式错误，期望列表: {path}")
            return []
    except Exception as e:
        logger.error(f"加载模板文件失败: {path}, 错误: {e}")
        return []


def load_backtest_params(dataset_id: str) -> Optional[Dict[str, List]]:
    """
    从数据集 guidance.json 加载推荐的回测参数。

    Args:
        dataset_id: 数据集 ID

    Returns:
        推荐的回测参数字典，如 {"window": [5,10,20], "decay": [2,3], ...}
        不存在则返回 None
    """
    guidance = load_guidance(dataset_id)
    if not guidance:
        return None

    params = guidance.get("recommended_backtest_params")
    if not params:
        return None

    # 只返回有效的参数键
    valid_keys = {"window", "decay", "truncation", "neutralization"}
    result = {}
    for k in valid_keys:
        v = params.get(k)
        if v is not None:
            result[k] = v

    return result if result else None


def load_guidance(dataset_id: str) -> Optional[Dict[str, Any]]:
    """
    加载研究方向引导配置。

    Args:
        dataset_id: 数据集 ID

    Returns:
        引导配置字典，不存在则返回 None
    """
    guidance_path = DATASET_TEMPLATES_DIR / f"{dataset_id}_guidance.json"
    if not guidance_path.exists():
        logger.debug(f"未找到研究方向引导: {guidance_path}")
        return None

    try:
        with open(guidance_path, "r", encoding="utf-8") as f:
            guidance = json.load(f)
        logger.info(f"加载研究方向引导: {guidance_path}")
        return guidance
    except Exception as e:
        logger.error(f"加载研究方向引导失败: {guidance_path}, 错误: {e}")
        return None


def get_guidance_prompt(dataset_id: str) -> Optional[str]:
    """
    获取研究方向引导的 Prompt 片段。

    Args:
        dataset_id: 数据集 ID

    Returns:
        可直接注入 AI prompt 的引导文本，不存在则返回 None
    """
    guidance = load_guidance(dataset_id)
    if not guidance:
        return None

    prompt = guidance.get("guidance_prompt")
    if prompt:
        return prompt

    # 如果没有预定义的 guidance_prompt，动态生成
    directions = guidance.get("research_directions", [])
    priority_fields = guidance.get("priority_fields", [])
    field_pairs = guidance.get("field_pairs", [])

    parts = [f"**{guidance.get('dataset_name', dataset_id)} 数据集研究方向指引（重要）：**"]
    parts.append(f"此数据集为 {guidance.get('description', '数据集')}。请按以下研究方向推荐字段组合：")

    for i, direction in enumerate(directions, 1):
        parts.append(f"\n**研究方向 {i}：{direction['name']}**")
        parts.append(f"- {direction['description']}")
        if direction.get("field_patterns"):
            parts.append(f"- 字段模式: {', '.join(direction['field_patterns'])}")
        if direction.get("alpha_logic"):
            parts.append(f"- Alpha 逻辑: {direction['alpha_logic']}")

    if priority_fields:
        parts.append("\n**优先推荐字段：**")
        for pf in priority_fields[:5]:
            parts.append(f"- {pf['field_id']}: {pf.get('reason', '')}")

    if field_pairs:
        parts.append("\n**推荐字段配对：**")
        for fp in field_pairs[:3]:
            parts.append(f"- {fp['field1']} 与 {fp['field2']}: {fp['logic']}")

    return "\n".join(parts)


def get_field_hints_for_template(
    template: Dict[str, Any],
    dataset_id: str,
) -> Dict[str, List[str]]:
    """
    获取模板字段的候选字段列表（基于 field_hints 和 guidance）。

    Args:
        template: 模板配置
        dataset_id: 数据集 ID

    Returns:
        {field_placeholder: [candidate_fields]} 映射
    """
    guidance = load_guidance(dataset_id)
    if not guidance:
        return {}

    field_hints = template.get("field_hints", {})
    result = {}

    for field_placeholder, pattern in field_hints.items():
        candidates = _find_fields_by_pattern(pattern, guidance)
        if candidates:
            result[field_placeholder] = candidates

    return result


def _find_fields_by_pattern(pattern: str, guidance: Dict[str, Any]) -> List[str]:
    """根据模式匹配优先字段。"""
    priority_fields = guidance.get("priority_fields", [])
    field_pairs = guidance.get("field_pairs", [])

    candidates = []

    # 从 priority_fields 中匹配
    for pf in priority_fields:
        field_id = pf.get("field_id", "")
        if _match_pattern(field_id, pattern):
            candidates.append(field_id)

    # 从 field_pairs 中匹配
    for fp in field_pairs:
        for key in ["field1", "field2"]:
            field_id = fp.get(key, "")
            if field_id and _match_pattern(field_id, pattern) and field_id not in candidates:
                candidates.append(field_id)

    return candidates


def _match_pattern(value: str, pattern: str) -> bool:
    """简化的模式匹配。"""
    if pattern.startswith("*") and pattern.endswith("*"):
        # 包含匹配
        return pattern[1:-1] in value
    elif pattern.startswith("*"):
        # 后缀匹配
        return value.endswith(pattern[1:])
    elif pattern.endswith("*"):
        # 前缀匹配
        return value.startswith(pattern[:-1])
    else:
        # 精确匹配
        return pattern in value


def match_fields_by_hints(
    field_hints: Dict[str, str],
    recommended_fields: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    根据 field_hints 的 pattern 匹配合适的字段。

    Args:
        field_hints: {placeholder: pattern} 映射，如 {"field": "mdl250_eq_*", "field1": "*_score"}
        recommended_fields: 推荐字段列表，每个元素包含 field_id

    Returns:
        {placeholder: matched_field_id} 映射
    """
    result = {}
    used_fields = set()  # 避免重复使用同一字段

    for placeholder, pattern in field_hints.items():
        for f in recommended_fields:
            field_id = f.get("field_id", "")
            if not field_id or field_id in used_fields:
                continue
            if _match_pattern(field_id, pattern):
                result[placeholder] = field_id
                used_fields.add(field_id)
                break

    return result


def list_available_specialized_templates() -> List[str]:
    """列出所有可用的针对性模板数据集。"""
    if not DATASET_TEMPLATES_DIR.exists():
        return []

    datasets = set()
    for f in DATASET_TEMPLATES_DIR.glob("*_templates.json"):
        # 提取数据集 ID
        dataset_id = f.stem.replace("_templates", "")
        datasets.add(dataset_id)

    return sorted(list(datasets))


def load_vector_templates() -> List[Dict[str, Any]]:
    """
    加载 VECTOR 类型专用模板。

    Returns:
        VECTOR 模板列表
    """
    if not VECTOR_TEMPLATES_PATH.exists():
        logger.warning(f"VECTOR 模板文件不存在: {VECTOR_TEMPLATES_PATH}")
        return []

    templates = _load_templates_from_file(VECTOR_TEMPLATES_PATH)
    if templates:
        logger.info(f"加载 VECTOR 模板: {len(templates)} 个")
    return templates


def load_mixed_templates(
    template_mode: str = "default",
    dataset_id: Optional[str] = None,
    templates_path: Optional[str] = None,
    include_vector: bool = True,
    min_templates: int = 30,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    加载混合模板：MATRIX 模板 + VECTOR 模板。

    根据模板模式加载基础模板，并可选地添加 VECTOR 专用模板。
    这样可以确保 MATRIX 和 VECTOR 类型的字段都能被正确处理。

    当针对性模板数量不足时，自动补充默认模板以满足 min_templates 要求。

    Args:
        template_mode: 模板模式 ("default" 或 "specialized")
        dataset_id: 数据集 ID（用于 specialized 模式）
        templates_path: 自定义模板路径
        include_vector: 是否包含 VECTOR 专用模板
        min_templates: 最小模板数量，不足时从默认模板补充

    Returns:
        (templates, source_info)
    """
    # 加载基础模板
    base_templates, source = load_templates(
        template_mode=template_mode,
        dataset_id=dataset_id,
        templates_path=templates_path,
    )

    # 如果是针对性模式且模板数量不足，从默认模板补充
    # 但需要检查针对性模板的字段类型兼容性
    if template_mode == "specialized" and len(base_templates) < min_templates:
        # 检查针对性模板是否全部是 event 类型专用
        all_event_only = all(
            _template_supports_type(t, "event") and not _template_supports_type(t, "vector")
            for t in base_templates
            if t.get("field_types")
        )

        if all_event_only:
            # 如果针对性模板全部是 event 类型专用，不补充默认模板
            # 因为默认模板不支持 event 类型字段
            logger.info(f"针对性模板全部为 event 类型专用，不补充默认模板")
            source = f"{source}(event_only)"
        else:
            # 混合类型或无类型约束，可以补充
            default_templates, _ = load_templates("default")
            # 去重：排除已存在的表达式
            existing_exprs = {t.get("expression") for t in base_templates}
            supplement = [t for t in default_templates if t.get("expression") not in existing_exprs]
            # 补充到 min_templates
            needed = min_templates - len(base_templates)
            if needed > 0 and supplement:
                base_templates = base_templates + supplement[:needed]
                source = f"{source}+default({needed})"
                logger.info(f"针对性模板不足 ({len(base_templates) - needed} 个)，从默认模板补充 {needed} 个")

    if not include_vector:
        return base_templates, source

    # 加载 VECTOR 模板
    vector_templates = load_vector_templates()
    if not vector_templates:
        return base_templates, source

    # 合并模板
    combined = base_templates + vector_templates
    logger.info(f"混合模板: 基础 {len(base_templates)} 个 + VECTOR {len(vector_templates)} 个 = {len(combined)} 个")

    return combined, f"{source}+vector"


def filter_templates_by_field_type(
    templates: List[Dict[str, Any]],
    field_normalized_type: str,
) -> List[Dict[str, Any]]:
    """
    根据字段类型筛选匹配的模板。

    Args:
        templates: 模板列表
        field_normalized_type: 字段的归一化类型 ("vector", "event", "group", "symbol")

    Returns:
        匹配的模板列表
    """
    field_type = field_normalized_type.lower().strip()

    if field_type == "vector":
        # MATRIX 类型字段：可用所有不要求 event 类型的模板
        return [
            t for t in templates
            if _template_supports_type(t, "vector")
        ]
    elif field_type == "event":
        # VECTOR 类型字段：仅可用 vec_* 模板
        return [
            t for t in templates
            if _template_supports_type(t, "event")
        ]
    elif field_type == "group":
        # 分组字段：仅用于 group_neutralize 等
        return [
            t for t in templates
            if _template_supports_type(t, "group")
        ]
    else:
        # 未知类型：返回所有模板（由下游处理）
        return templates


def _template_supports_type(template: Dict[str, Any], field_type: str) -> bool:
    """
    检查模板是否支持指定的字段类型。

    Args:
        template: 模板配置
        field_type: 字段类型 ("vector", "event", "group")

    Returns:
        是否支持
    """
    field_types = template.get("field_types", [])

    if not field_types:
        # 没有类型约束的模板，默认支持 vector 类型
        return field_type == "vector"

    # 检查每个字段槽位的类型要求
    for slot_types in field_types:
        if isinstance(slot_types, list):
            if field_type in [t.lower() for t in slot_types]:
                return True
        elif isinstance(slot_types, str):
            if slot_types.lower() == field_type:
                return True

    return False


def get_templates_for_fields(
    recommended_fields: List[Dict[str, Any]],
    template_mode: str = "default",
    dataset_id: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
    """
    根据推荐字段的类型，智能选择合适的模板。

    Args:
        recommended_fields: 推荐字段列表，每个字段包含 field_id 和 normalized_type
        template_mode: 模板模式
        dataset_id: 数据集 ID

    Returns:
        (templates, field_type_distribution)
        - templates: 合并后的模板列表
        - field_type_distribution: 各类型字段数量统计
    """
    # 统计字段类型分布
    type_counts = {}
    for f in recommended_fields:
        ft = (f.get("normalized_type") or "unknown").lower()
        type_counts[ft] = type_counts.get(ft, 0) + 1

    # 检查是否有 VECTOR 类型字段
    has_event_fields = type_counts.get("event", 0) > 0

    # 加载模板
    templates, source = load_mixed_templates(
        template_mode=template_mode,
        dataset_id=dataset_id,
        include_vector=has_event_fields,
    )

    logger.info(f"智能模板选择: 字段类型分布={type_counts}, 模板数={len(templates)}, 来源={source}")

    return templates, type_counts


if __name__ == "__main__":
    # 测试
    print("=== 测试模板加载器 ===")

    # 测试默认模板
    templates, source = load_templates("default")
    print(f"默认模板: {len(templates)} 个, 来源: {source}")

    # 测试针对性模板
    templates, source = load_templates("specialized", "analyst4")
    print(f"analyst4 针对性模板: {len(templates)} 个, 来源: {source}")

    # 测试研究方向引导
    prompt = get_guidance_prompt("analyst4")
    if prompt:
        print(f"\n引导 Prompt 片段:\n{prompt[:200]}...")

    # 测试可用针对性模板
    available = list_available_specialized_templates()
    print(f"\n可用的针对性模板数据集: {available}")
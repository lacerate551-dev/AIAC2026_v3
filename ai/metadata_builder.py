# -*- coding: utf-8 -*-
"""
Dataset / Field Metadata 构建模块
从 BRAIN API（DataManager）拉取数据，构建两级 metadata 供 AI 数据分析使用。
- dataset level: dataset_id, dataset_name, category, coverage, region, frequency
- field level: dataset_id, field_id, field_name, description, coverage, type, frequency（可选，来自缓存或回测）
"""
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd

from ai.frequency_inference import infer_dataset_frequency

logger = logging.getLogger(__name__)

# 默认频率（当推断失败时使用）
DEFAULT_FREQUENCY = "daily"

# 字段类型映射（可配置/可扩展）：raw type -> normalized_type
# 说明：BRAIN 返回的 type/dataType/data_type 在不同数据集可能不一致，这里做统一归一化，供下游做类型约束。
#
# BRAIN 平台字段类型语义（经实测验证 2026-03-19）：
# - MATRIX: 数值时间序列矩阵，可直接用于 ts_mean/rank/zscore/scale 等数值操作符
# - VECTOR: 事件型数据（event data），需要 vec_sum/vec_avg/vec_count 等操作符转换为 vector 后才能使用
# - GROUP: 分组字段（如 industry, sector），仅用于 group_neutralize 等分组操作的第二参数
# - SYMBOL: 标识符字段（如 ticker, cusip），不能用于数值计算
# - EVENT: 事件类型，需要 vec_* 操作符处理（与 VECTOR 相同）
FIELD_TYPE_MAPPING = {
    # 可直接用于数值计算的类型 → vector
    "MATRIX": "vector",      # BRAIN 中 MATRIX 是数值时间序列，可直接用于 ts_*/rank/zscore 等

    # 事件型数据 → event（需要 vec_* 操作符转换）
    # 实测验证：rank(vector_field) 报错 "does not support event inputs"
    # 正确用法：rank(vec_sum(vector_field)) 或 rank(vec_avg(vector_field))
    "VECTOR": "event",       # 事件型数据，需要 vec_sum/vec_avg 等转换为 vector

    "FLOAT": "vector",
    "DOUBLE": "vector",
    "INT": "vector",
    "INTEGER": "vector",
    "NUMERIC": "vector",
    "NUMBER": "vector",

    # 分组/分类类型 → group（仅用于 group_neutralize 等分组操作）
    "GROUP": "group",
    "INDUSTRY": "group",
    "SECTOR": "group",
    "SUBINDUSTRY": "group",

    # 标识符类型 → symbol（不能用于数值计算）
    "SYMBOL": "symbol",
    "STRING": "symbol",
    "TICKER": "symbol",
    "CUSIP": "symbol",
    "ISIN": "symbol",
    "SEDOL": "symbol",

    # 事件类型 → event（需要 vec_* 操作符）
    "EVENT": "event",

    # Universe 类型 → universe
    "UNIVERSE": "universe",

    # 标量
    "SCALAR": "scalar",
}

# 记录无法映射的原始类型（便于后续补齐映射表）
UNKNOWN_FIELD_TYPES: set[str] = set()


def normalize_field_type(raw_type: str) -> str:
    """
    将平台原始字段类型归一化为内部类型标识。

    BRAIN 平台类型说明（经实测验证 2026-03-19）：
    - vector: MATRIX 类型字段，可直接用于数值计算（ts_mean/rank/zscore/scale 等）
    - event: VECTOR 类型字段（事件型数据），需要 vec_sum/vec_avg/vec_count 等转换为 vector
    - group: 分组字段，仅用于 group_neutralize 等操作的第二参数
    - symbol: 标识符字段，不能用于数值计算

    Returns:
        one of: vector/group/symbol/event/universe/scalar/unknown
    """
    if raw_type is None:
        UNKNOWN_FIELD_TYPES.add("")
        return "unknown"
    t = str(raw_type).strip()
    if not t:
        UNKNOWN_FIELD_TYPES.add("")
        return "unknown"
    key = t.upper()
    if key in FIELD_TYPE_MAPPING:
        return FIELD_TYPE_MAPPING[key]
    # 兼容包含式（例如 "VECTOR_FLOAT" 之类）
    for k, v in FIELD_TYPE_MAPPING.items():
        if k and k in key:
            return v
    UNKNOWN_FIELD_TYPES.add(key)
    return "unknown"

# 频率检测结果缓存目录（按 region_dataset 存储，便于复用）
FREQUENCY_CACHE_DIR = Path(__file__).parent.parent / "cache" / "frequency_detection"


def _normalize_col(df: pd.DataFrame, possible_names: List[str], default: Any = "") -> pd.Series:
    """从 DataFrame 中取第一个存在的列名，否则返回默认值序列。"""
    for c in possible_names:
        if c in df.columns:
            return df[c]
    return pd.Series([default] * len(df), index=df.index)


def build_dataset_metadata(
    datasets_df: pd.DataFrame,
    region: str,
    frequency: str = None,  # 改为可选，None 时自动推断
) -> List[Dict[str, Any]]:
    """
    从 DataManager.get_datasets() 返回的 DataFrame 构建 dataset 级 metadata。

    Args:
        datasets_df: 数据集列表 DataFrame（来自 BRAIN API）
        region: 区域代码
        frequency: 频率，若为 None 则自动推断

    Returns:
        [{"dataset_id", "dataset_name", "category", "coverage", "region", "frequency"}, ...]
    """
    if datasets_df is None or datasets_df.empty:
        return []

    out = []
    ids = _normalize_col(datasets_df, ["id", "dataset_id"])
    names = _normalize_col(datasets_df, ["name", "dataset_name", "title"])
    descriptions = _normalize_col(datasets_df, ["description", "desc"])
    categories = _normalize_col(datasets_df, ["category", "subcategory", "type"])
    coverages = _normalize_col(datasets_df, ["coverage"])

    for i in range(len(datasets_df)):
        cov = coverages.iloc[i]
        if isinstance(cov, (int, float)) and 0 <= cov <= 1:
            pass
        elif isinstance(cov, (int, float)) and cov > 1:
            cov = cov / 100.0 if cov > 1 else cov
        else:
            try:
                cov = float(cov) if cov is not None else 0.0
            except (TypeError, ValueError):
                cov = 0.0

        # 获取数据集信息
        dataset_id = str(ids.iloc[i]) if ids.iloc[i] is not None else ""
        dataset_name = str(names.iloc[i]) if names.iloc[i] is not None else ""
        description = str(descriptions.iloc[i]) if descriptions.iloc[i] is not None else ""

        # 推断频率（如果未指定）
        if frequency is None:
            # 尝试从原始 DataFrame 获取 category 对象
            row = datasets_df.iloc[i]
            category_obj = row.get("category") if isinstance(row.get("category"), dict) else None
            subcategory_obj = row.get("subcategory") if isinstance(row.get("subcategory"), dict) else None

            inferred_freq = infer_dataset_frequency(
                dataset_id,
                dataset_name,
                description,
                category_obj,
                subcategory_obj
            )
        else:
            inferred_freq = frequency

        out.append({
            "dataset_id": dataset_id,
            "dataset_name": dataset_name,
            "category": str(categories.iloc[i]) if categories.iloc[i] is not None else "",
            "coverage": cov,
            "region": region,
            "frequency": inferred_freq,
        })
    return out


def build_field_metadata(
    fields_df: pd.DataFrame,
    dataset_id: str,
    dataset_frequency: Optional[str] = None,
    field_frequency_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    从 DataManager.get_fields() 返回的 DataFrame 构建 field 级 metadata。

    字段更新频率来源优先级：
    1. fields_df 中已有的 frequency 列（来自 detect_field_frequency 写回的缓存）
    2. field_frequency_map[field_id/field_name]（来自本次或历史的回测检测结果）
    3. dataset_frequency（数据集级推断）
    4. 不写入 frequency（由下游用默认值）

    Args:
        fields_df: 字段列表 DataFrame（来自 BRAIN API 或带 frequency 的缓存）
        dataset_id: 所属数据集 ID
        dataset_frequency: 数据集级频率，用于无字段级信息时的回退
        field_frequency_map: 可选，field_id 或 field_name -> {frequency, confidence, method/source}

    Returns:
        [{"dataset_id", "field_id", "field_name", "description", "coverage", "type", "frequency?", "frequency_confidence?", "frequency_source?"}, ...]
    """
    if fields_df is None or fields_df.empty:
        return []

    field_frequency_map = field_frequency_map or {}
    freq_col = _normalize_col(fields_df, ["frequency"], None)
    freq_conf_col = _normalize_col(fields_df, ["frequency_confidence"], None)
    freq_src_col = _normalize_col(fields_df, ["frequency_method", "frequency_source"], None)

    out = []
    ids = _normalize_col(fields_df, ["id", "field_id", "name"])
    names = _normalize_col(fields_df, ["name", "field_name", "id"])
    descriptions = _normalize_col(fields_df, ["description", "desc"])
    types = _normalize_col(fields_df, ["type", "dataType", "data_type"])
    coverages = _normalize_col(fields_df, ["coverage"])

    for i in range(len(fields_df)):
        cov = coverages.iloc[i]
        if isinstance(cov, (int, float)) and 0 <= cov <= 1:
            pass
        elif isinstance(cov, (int, float)) and cov > 1:
            cov = cov / 100.0 if cov > 1 else cov
        else:
            try:
                cov = float(cov) if cov is not None else 0.0
            except (TypeError, ValueError):
                cov = 0.0

        field_id = str(ids.iloc[i]) if ids.iloc[i] is not None else ""
        field_name = str(names.iloc[i]) if names.iloc[i] is not None else ""

        raw_type = str(types.iloc[i]) if types.iloc[i] is not None else ""
        normalized_type = normalize_field_type(raw_type)

        row = {
            "dataset_id": dataset_id,
            "field_id": field_id,
            "field_name": field_name,
            "description": str(descriptions.iloc[i]) if descriptions.iloc[i] is not None else "",
            "coverage": cov,
            "type": raw_type,
            "normalized_type": normalized_type,
        }

        # 频率：优先缓存列，再回测 map，再数据集级
        frequency = None
        frequency_confidence = None
        frequency_source = None
        _fval = freq_col.iloc[i] if freq_col is not None and i < len(freq_col) else None
        _fval_ok = _fval is not None and str(_fval).strip() not in ("", "nan")

        if _fval_ok:
            frequency = str(_fval).strip()
            _c = freq_conf_col.iloc[i] if freq_conf_col is not None and i < len(freq_conf_col) else None
            frequency_confidence = float(_c) if _c is not None and str(_c).strip() not in ("", "nan") else 0.8
            _s = freq_src_col.iloc[i] if freq_src_col is not None and i < len(freq_src_col) else None
            frequency_source = str(_s).strip() if _s is not None and str(_s).strip() not in ("", "nan") else "cache"
        else:
            info = field_frequency_map.get(field_id) or field_frequency_map.get(field_name)
            if info and info.get("frequency"):
                frequency = str(info["frequency"])
                frequency_confidence = float(info.get("confidence", 0.7))
                frequency_source = str(info.get("method", info.get("source", "backtest")))
            elif dataset_frequency:
                frequency = dataset_frequency
                frequency_confidence = 0.5
                frequency_source = "dataset"

        if frequency:
            row["frequency"] = frequency
            if frequency_confidence is not None:
                row["frequency_confidence"] = frequency_confidence
            if frequency_source:
                row["frequency_source"] = frequency_source

        out.append(row)
    return out


def _get_field_frequency_map_for_dataset(
    session,
    region: str,
    dataset_id: str,
    field_names: List[str],
    use_cache: bool = True,
    force_refresh: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """
    获取某数据集的字段级频率映射：优先读缓存，否则用已有操作符回测后写入缓存。

    Args:
        session: BRAIN 会话
        region: 区域代码
        dataset_id: 数据集 ID
        field_names: 字段名列表（用于回测）
        use_cache: 是否优先使用缓存
        force_refresh: 是否强制重新回测

    Returns:
        { field_name: {"frequency": str, "confidence": float, "method": "backtest", "reasoning": list} }
    """
    FREQUENCY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = FREQUENCY_CACHE_DIR / f"{region}_{dataset_id}_frequency.json"

    if use_cache and not force_refresh and cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            # 兼容格式：可能是 {field: str} 或 {field: {frequency, confidence, ...}}
            out = {}
            for k, v in data.items():
                if isinstance(v, dict):
                    out[k] = v
                else:
                    out[k] = {"frequency": str(v), "confidence": 0.8, "method": "cache"}
            logger.info(f"已从缓存加载字段频率: {region}/{dataset_id} ({len(out)} 个)")
            return out
        except Exception as e:
            logger.warning(f"读取频率缓存失败: {e}")

    from core.frequency_detector import FrequencyDetector

    detector = FrequencyDetector(session, region)
    results = detector.batch_detect_fields(field_names, dataset_id=dataset_id)

    out = {}
    for name, res in results.items():
        out[name] = {
            "frequency": res.get("frequency", "unknown"),
            "confidence": res.get("confidence", 0.0),
            "method": "backtest",
            "reasoning": res.get("reasoning", []),
        }

    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        logger.info(f"已保存字段频率缓存: {cache_file}")
    except Exception as e:
        logger.warning(f"保存频率缓存失败: {e}")

    return out


def build_metadata_for_region_datasets(
    session,
    region: str,
    dataset_ids: List[str],
    data_manager,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
    force_refresh: bool = False,
    use_backtest_frequency: bool = False,
    frequency_use_cache: bool = True,
) -> Dict[str, Any]:
    """
    为指定 region 和 dataset 列表构建完整 metadata（dataset + field）。

    使用 data_manager.get_datasets 与 data_manager.get_fields 构建两级 metadata。
    若 use_backtest_frequency=True，会利用已有操作符（如 days_from_last_change、ts_count_nans、ts_delta）
    对字段进行回测，根据回测结果推断准确的更新频率并写入 field_metadata。

    Args:
        session: BRAIN 会话
        region: 区域代码
        dataset_ids: 数据集 ID 列表（如 ["pv1", "analyst15"]）
        data_manager: DataManager 类或实例，需有 get_datasets / get_fields
        universe: 可选
        delay: 可选
        force_refresh: 是否强制刷新数据缓存
        use_backtest_frequency: 是否对字段做回测以得到准确更新频率（会写缓存，下次可复用）
        frequency_use_cache: 在 use_backtest_frequency 时是否优先使用已有频率缓存

    Returns:
        {
            "region": str,
            "dataset_metadata": [dataset_level dict, ...],
            "field_metadata": [field_level dict, ...],
        }
    """
    region = region.upper()
    get_datasets = getattr(data_manager, "get_datasets", None)
    get_fields = getattr(data_manager, "get_fields", None)
    if not get_datasets or not get_fields:
        raise ValueError("data_manager 需提供 get_datasets 与 get_fields")

    # 拉取数据集列表（用于 dataset 级 metadata）
    datasets_df = get_datasets(
        session, region,
        universe=universe, delay=delay,
        force_refresh=force_refresh,
    )
    id_set = set(str(x) for x in dataset_ids)
    id_col = "id" if "id" in datasets_df.columns else "dataset_id"
    if id_col in datasets_df.columns:
        datasets_df = datasets_df[datasets_df[id_col].astype(str).isin(id_set)]
    dataset_metadata = build_dataset_metadata(datasets_df, region)
    ds_freq_map = {m["dataset_id"]: m.get("frequency", DEFAULT_FREQUENCY) for m in dataset_metadata}

    # 拉取每个数据集的字段并构建 field metadata（可选：回测得到字段频率）
    field_metadata = []
    for ds_id in dataset_ids:
        try:
            fields_df = get_fields(
                session, region, ds_id,
                universe=universe, delay=delay,
                force_refresh=force_refresh,
            )
            if fields_df is None or fields_df.empty:
                continue

            dataset_frequency = ds_freq_map.get(ds_id, DEFAULT_FREQUENCY)
            field_frequency_map = None

            if use_backtest_frequency:
                id_col_field = "id" if "id" in fields_df.columns else "name"
                col = id_col_field if id_col_field in fields_df.columns else fields_df.columns[0]
                names = fields_df[col].astype(str).tolist()
                field_frequency_map = _get_field_frequency_map_for_dataset(
                    session,
                    region,
                    ds_id,
                    names,
                    use_cache=frequency_use_cache,
                    force_refresh=force_refresh,
                )

            field_metadata.extend(
                build_field_metadata(
                    fields_df,
                    ds_id,
                    dataset_frequency=dataset_frequency,
                    field_frequency_map=field_frequency_map,
                )
            )
        except Exception as e:
            logger.warning(f"获取 {region}/{ds_id} 字段失败: {e}")

    return {
        "region": region,
        "dataset_metadata": dataset_metadata,
        "field_metadata": field_metadata,
    }

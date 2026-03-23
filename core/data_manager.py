"""
数据管理模块 - 统一管理数据集、字段、操作符的获取与缓存
整合了原项目中 step01_cache_check.py, step02_query_fields.py,
fetch_usa_datasets.py, fetch_usa_news12_pv1_fields.py 等 10+ 个散乱脚本的功能
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from . import ace_lib
from config.settings import (
    OPERATORS_CACHE,
    REGION_DATASETS_CACHE,
    DATASET_FIELDS_CACHE,
    REGION_DEFAULTS,
)


class DataManager:
    """数据获取与缓存管理器"""

    # ==================== 操作符管理 ====================

    @staticmethod
    def get_operators(session, force_refresh: bool = False) -> pd.DataFrame:
        """
        获取平台可用操作符列表（带缓存）。

        Args:
            session: 已认证的 Session 对象
            force_refresh: 是否强制刷新缓存

        Returns:
            包含操作符信息的 DataFrame
        """
        cache_path = OPERATORS_CACHE

        if cache_path.exists() and not force_refresh:
            print(f"[Cache] 从缓存加载操作符: {cache_path.name}")
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return pd.DataFrame(data)

        print("[Fetch] 正在从API获取操作符列表...")
        operators_df = ace_lib.get_operators(session)

        # 缓存到文件
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        operators_df.to_json(cache_path, orient="records", force_ascii=False, indent=2)
        print(f"[OK] 操作符已缓存: {len(operators_df)} 个操作符")
        return operators_df

    # ==================== 数据集管理 ====================

    @staticmethod
    def get_datasets(
        session,
        region: str,
        universe: str = None,
        delay: int = None,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        获取指定区域的可用数据集列表（带缓存）。

        Args:
            session: 已认证的 Session 对象
            region: 区域代码 (USA, IND, CHN 等)
            universe: Universe, 默认使用区域默认值
            delay: 延迟天数, 默认使用区域默认值
            force_refresh: 是否强制刷新缓存

        Returns:
            包含数据集信息的 DataFrame
        """
        region = region.upper()
        defaults = REGION_DEFAULTS.get(region, {"universe": "TOP3000", "delay": 1})
        universe = universe or defaults["universe"]
        delay = delay if delay is not None else defaults["delay"]

        cache_path = REGION_DATASETS_CACHE(region)

        if cache_path.exists() and not force_refresh:
            print(f"[Cache] 从缓存加载 {region} 数据集: {cache_path.name}")
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return pd.DataFrame(data.get("datasets", data))

        print(f"[Fetch] 正在从API获取 {region} (universe={universe}, delay={delay}) 数据集...")
        datasets_df = ace_lib.get_datasets(
            session, region=region, universe=universe, delay=delay
        )

        # 缓存到文件
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_data = {
            "region": region,
            "universe": universe,
            "delay": delay,
            "query_date": datetime.now().strftime("%Y-%m-%d"),
            "total_datasets": len(datasets_df),
            "datasets": datasets_df.to_dict(orient="records"),
        }
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

        print(f"[OK] {region} 数据集已缓存: {len(datasets_df)} 个数据集")
        return datasets_df

    # ==================== 字段管理 ====================

    @staticmethod
    def get_fields(
        session,
        region: str,
        dataset_id: str,
        universe: str = None,
        delay: int = None,
        data_type: str = "ALL",
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        获取指定数据集的字段列表（带缓存）。

        Args:
            session: 已认证的 Session 对象
            region: 区域代码
            dataset_id: 数据集ID (如 "pv1", "analyst4", "fundamental17")
            universe: Universe, 默认使用区域默认值
            delay: 延迟天数, 默认使用区域默认值
            data_type: 数据类型 ("ALL", "MATRIX", "TEXT")
            force_refresh: 是否强制刷新缓存

        Returns:
            包含字段信息的 DataFrame
        """
        region = region.upper()
        defaults = REGION_DEFAULTS.get(region, {"universe": "TOP3000", "delay": 1})
        universe = universe or defaults["universe"]
        delay = delay if delay is not None else defaults["delay"]

        cache_path = DATASET_FIELDS_CACHE(region, dataset_id)

        if cache_path.exists() and not force_refresh:
            print(f"[Cache] 从缓存加载 {region}/{dataset_id} 字段: {cache_path.name}")
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return pd.DataFrame(data.get("fields", data))

        print(f"[Fetch] 正在从API获取 {region}/{dataset_id} 字段 (type={data_type})...")
        fields_df = ace_lib.get_datafields(
            session,
            region=region,
            universe=universe,
            delay=delay,
            dataset_id=dataset_id,
            data_type=data_type,
        )

        # 缓存到文件
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_data = {
            "region": region,
            "dataset_id": dataset_id,
            "universe": universe,
            "delay": delay,
            "data_type": data_type,
            "query_date": datetime.now().strftime("%Y-%m-%d"),
            "total_fields": len(fields_df),
            "fields": fields_df.to_dict(orient="records"),
        }
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

        print(f"[OK] {region}/{dataset_id} 字段已缓存: {len(fields_df)} 个字段")
        return fields_df

    # ==================== 多数据集联合字段管理 ====================

    @staticmethod
    def get_multi_fields(
        session,
        region: str,
        dataset_ids: list,
        universe: str = None,
        delay: int = None,
        data_type: str = "MATRIX",
        force_refresh: bool = False,
    ) -> tuple:
        """
        获取多个数据集的字段并合并（带缓存，复用 get_fields）。

        Args:
            session: 已认证的 Session 对象
            region: 区域代码
            dataset_ids: 数据集 ID 列表（如 ["pv1", "analyst15"]）
            universe: Universe, 默认使用区域默认值
            delay: 延迟天数, 默认使用区域默认值
            data_type: 数据类型
            force_refresh: 是否强制刷新缓存

        Returns:
            (merged_df, per_dataset_dict):
                merged_df: 合并后的 DataFrame（含 source_dataset 列，按 coverage 降序）
                per_dataset_dict: {dataset_id: DataFrame} 各数据集原始字段
        """
        import logging
        logger = logging.getLogger(__name__)

        per_dataset_dict = {}
        all_frames = []

        for ds_id in dataset_ids:
            try:
                fields_df = DataManager.get_fields(
                    session, region, ds_id,
                    universe=universe, delay=delay,
                    data_type=data_type, force_refresh=force_refresh
                )
                fields_df = fields_df.copy()
                fields_df["source_dataset"] = ds_id
                per_dataset_dict[ds_id] = fields_df
                all_frames.append(fields_df)
                logger.info(f"已加载 {region}/{ds_id}: {len(fields_df)} 个字段")
            except Exception as e:
                logger.error(f"获取 {region}/{ds_id} 字段失败: {e}")
                print(f"⚠️ 获取 {ds_id} 字段失败: {e}")

        if not all_frames:
            raise RuntimeError("所有数据集字段获取均失败")

        # 合并
        merged_df = pd.concat(all_frames, ignore_index=True)

        # 确定字段名列
        field_name_col = "id" if "id" in merged_df.columns else "name"

        # 去重：同名字段保留 coverage 更高的
        if "coverage" in merged_df.columns:
            merged_df = merged_df.sort_values("coverage", ascending=False)
        merged_df = merged_df.drop_duplicates(subset=[field_name_col], keep="first")

        # 按 coverage 降序排列
        if "coverage" in merged_df.columns:
            merged_df = merged_df.sort_values("coverage", ascending=False).reset_index(drop=True)

        logger.info(f"多数据集合并完成: {len(dataset_ids)} 个数据集, {len(merged_df)} 个唯一字段")
        print(f"[OK] 多数据集合并: {' + '.join(dataset_ids)} = {len(merged_df)} 个唯一字段")

        return merged_df, per_dataset_dict

    # ==================== 缓存查询工具 ====================

    @staticmethod
    def list_cached_regions() -> list[str]:
        """列出所有已缓存的区域"""
        regions_dir = OPERATORS_CACHE.parent / "regions"
        if not regions_dir.exists():
            return []
        return [
            f.stem.replace("_datasets", "")
            for f in regions_dir.glob("*_datasets.json")
        ]

    @staticmethod
    def list_cached_fields(region: str = None) -> list[str]:
        """
        列出已缓存的数据集字段文件。

        Args:
            region: 可选，指定区域过滤

        Returns:
            缓存文件名列表
        """
        fields_dir = OPERATORS_CACHE.parent / "dataset_fields"
        if not fields_dir.exists():
            return []
        pattern = f"{region.upper()}_*_fields.json" if region else "*_fields.json"
        return [f.stem for f in fields_dir.glob(pattern)]

    @staticmethod
    def get_region_config(region: str) -> dict:
        """
        获取指定区域的默认配置。

        Args:
            region: 区域代码

        Returns:
            包含 universe, delay 等的配置字典
        """
        region = region.upper()
        config = REGION_DEFAULTS.get(region)
        if config is None:
            print(f"⚠️ 未找到 {region} 的默认配置，使用 USA 默认值")
            config = REGION_DEFAULTS["USA"]
        return {**config, "region": region}

    @staticmethod
    def print_cache_status():
        """打印当前缓存状态概览"""
        print("\n📊 缓存状态:")
        print(f"  操作符缓存: {'[OK] 存在' if OPERATORS_CACHE.exists() else '❌ 不存在'}")

        cached_regions = DataManager.list_cached_regions()
        print(f"  已缓存区域: {', '.join(cached_regions) if cached_regions else '无'}")

        cached_fields = DataManager.list_cached_fields()
        print(f"  已缓存字段: {len(cached_fields)} 个数据集")
        for field_name in cached_fields[:10]:
            print(f"    - {field_name}")
        if len(cached_fields) > 10:
            print(f"    ... 还有 {len(cached_fields) - 10} 个")
        print()

    # ==================== 自愈系统：知识刷新 ====================

    @staticmethod
    def force_refresh_fields(
        session,
        region: str,
        dataset_id: str,
        universe: str = None,
        delay: int = None,
        data_type: str = "ALL"
    ) -> pd.DataFrame:
        """
        强制刷新字段列表（绕过缓存）- 用于自愈系统

        Args:
            session: 已认证的 Session 对象
            region: 区域代码
            dataset_id: 数据集ID
            universe: Universe
            delay: 延迟天数
            data_type: 数据类型

        Returns:
            最新的字段 DataFrame
        """
        import logging
        logger = logging.getLogger(__name__)

        logger.info(f"[Self-Healing] 正在刷新数据集 '{dataset_id}' 元数据...")

        # 直接调用 API，强制刷新
        fields_df = DataManager.get_fields(
            session, region, dataset_id,
            universe=universe, delay=delay,
            data_type=data_type,
            force_refresh=True  # 关键：强制刷新
        )

        logger.info(f"[Self-Healing] 已更新缓存: {len(fields_df)} 个字段")
        return fields_df

    @staticmethod
    def force_refresh_operators(session) -> pd.DataFrame:
        """
        强制刷新操作符列表（绕过缓存）- 用于自愈系统

        Args:
            session: 已认证的 Session 对象

        Returns:
            最新的操作符 DataFrame
        """
        import logging
        logger = logging.getLogger(__name__)

        logger.info(f"[Self-Healing] 正在刷新操作符列表...")

        # 直接调用 API，强制刷新
        operators_df = DataManager.get_operators(session, force_refresh=True)

        logger.info(f"[Self-Healing] 已更新缓存: {len(operators_df)} 个操作符")
        return operators_df

    @staticmethod
    def find_similar_field(
        target_field: str,
        available_fields: list,
        threshold: float = 0.6,
        max_results: int = 5
    ) -> list:
        """
        查找相似字段（编辑距离算法）- 用于自愈系统

        Args:
            target_field: 目标字段（如 "fnd13_xxx"）
            available_fields: 可用字段列表
            threshold: 相似度阈值（0.0-1.0，默认 0.6）
            max_results: 最多返回结果数（默认 5）

        Returns:
            相似字段列表（按相似度排序）
        """
        import logging
        from difflib import get_close_matches

        logger = logging.getLogger(__name__)

        # 使用 difflib 快速匹配
        matches = get_close_matches(
            target_field,
            available_fields,
            n=max_results,
            cutoff=threshold
        )

        if matches:
            logger.info(f"[Self-Healing] 找到相似字段: {matches}")
        else:
            logger.warning(f"[Self-Healing] 未找到相似字段（阈值: {threshold}）")

        return matches

    @staticmethod
    def find_similar_operator(
        target_operator: str,
        available_operators: list,
        threshold: float = 0.6,
        max_results: int = 5
    ) -> list:
        """
        查找相似操作符（编辑距离算法）- 用于自愈系统

        Args:
            target_operator: 目标操作符（如 "ts_mean"）
            available_operators: 可用操作符列表
            threshold: 相似度阈值（0.0-1.0，默认 0.6）
            max_results: 最多返回结果数（默认 5）

        Returns:
            相似操作符列表（按相似度排序）
        """
        import logging
        from difflib import get_close_matches

        logger = logging.getLogger(__name__)

        # 使用 difflib 快速匹配
        matches = get_close_matches(
            target_operator,
            available_operators,
            n=max_results,
            cutoff=threshold
        )

        if matches:
            logger.info(f"[Self-Healing] 找到相似操作符: {matches}")
        else:
            logger.warning(f"[Self-Healing] 未找到相似操作符（阈值: {threshold}）")

        return matches

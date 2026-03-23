# -*- coding: utf-8 -*-
"""
AI 量化研究员核心模块
负责数据分析、策略构建、闭环优化
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
from openai import OpenAI

from config.settings import API_KEYS_FILE
from config.ai_config import (
    AI_MODELS,
    DEFAULT_PROVIDER,
    API_CONFIG,
    STRATEGY_CONFIG,
    OPTIMIZATION_CONFIG
)


class AIResearcher:
    """AI 量化研究员核心类"""

    def __init__(self, provider: Optional[str] = None):
        """
        初始化 AI 客户端（统一使用 OpenAI SDK）

        Args:
            provider: 模型提供商（claude/codex/deepseek），默认使用配置文件中的 current_provider
        """
        self.logger = logging.getLogger(__name__)

        # 读取 API Keys 配置
        self.api_keys_config = self._load_api_keys()

        # 确定使用的提供商
        self.provider = provider or self.api_keys_config.get("current_provider", DEFAULT_PROVIDER)

        if self.provider not in AI_MODELS:
            raise ValueError(f"不支持的模型提供商: {self.provider}，可选: {list(AI_MODELS.keys())}")

        # 获取提供商配置
        provider_config = self.api_keys_config.get(self.provider)
        if not provider_config or not isinstance(provider_config, dict):
            raise ValueError(f"请在 {API_KEYS_FILE} 中配置 {self.provider} 的完整信息")

        # 验证 API Key
        api_key = provider_config.get("api_key", "")
        if not api_key or api_key.startswith("your-"):
            raise ValueError(f"请在 {API_KEYS_FILE} 中配置有效的 {self.provider} API Key")

        # 获取模型配置
        self.model_config = AI_MODELS[self.provider]
        self.base_url = provider_config["base_url"]
        self.model_name = provider_config["model"]
        self.api_key = api_key

        # 初始化 OpenAI 客户端（统一接口）
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=API_CONFIG["timeout"]
        )

        self.logger.info(
            f"AI 研究员已初始化 | 提供商: {self.provider} | "
            f"模型: {self.model_name} | Base URL: {self.base_url}"
        )

    def _load_api_keys(self) -> Dict[str, Any]:
        """读取 API Keys 配置"""
        if not API_KEYS_FILE.exists():
            raise FileNotFoundError(f"API Keys 配置文件不存在: {API_KEYS_FILE}")

        with open(API_KEYS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    def _call_ai(self, prompt: str, system_prompt: str = "", json_mode: bool = True) -> Dict[str, Any]:
        """
        调用 AI 模型（统一使用 OpenAI SDK，支持重试）

        Args:
            prompt: 用户提示词
            system_prompt: 系统提示词
            json_mode: 是否启用 JSON 模式

        Returns:
            AI 返回的 JSON 对象（如果 json_mode=True）或文本字符串
        """
        for attempt in range(API_CONFIG["max_retries"]):
            try:
                if attempt == 0:
                    self.logger.info(
                        f"正在请求 AI | 提供商: {self.provider} | 模型: {self.model_name} | "
                        f"超时: {API_CONFIG['timeout']}s (第 1 次请求)"
                    )
                elif attempt > 0:
                    self.logger.info(f"正在重试 AI 请求 (第 {attempt + 1}/{API_CONFIG['max_retries']} 次)")
                # 构建消息
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": prompt})

                # 构建请求参数
                kwargs = {
                    "model": self.model_name,
                    "messages": messages,
                    "temperature": self.model_config["temperature"],
                    "max_tokens": self.model_config["max_tokens"]
                }

                # 如果启用 JSON 模式，添加 response_format
                if json_mode:
                    kwargs["response_format"] = {"type": "json_object"}

                # 调用 API
                response = self.client.chat.completions.create(**kwargs)
                content = response.choices[0].message.content

                # 记录日志
                if API_CONFIG["enable_logging"]:
                    self.logger.info(
                        f"AI 调用成功 | 提供商: {self.provider} | "
                        f"Token 消耗: {response.usage.total_tokens if hasattr(response, 'usage') else 'N/A'}"
                    )

                # 解析 JSON
                if json_mode:
                    try:
                        return json.loads(content)
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"JSON 解析失败，尝试提取 JSON 块: {e}")
                        # 尝试提取 ```json ... ``` 块
                        import re
                        json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
                        if json_match:
                            return json.loads(json_match.group(1))
                        else:
                            # 尝试提取 { ... } 块
                            json_match = re.search(r'\{.*\}', content, re.DOTALL)
                            if json_match:
                                return json.loads(json_match.group(0))
                            else:
                                raise ValueError(f"无法解析 AI 返回的 JSON: {content[:200]}")
                else:
                    return {"content": content}

            except Exception as e:
                err_type = type(e).__name__
                err_msg = str(e)
                self.logger.error(
                    f"AI 调用失败 (尝试 {attempt + 1}/{API_CONFIG['max_retries']}) | "
                    f"类型: {err_type} | 提供商: {self.provider} | 模型: {self.model_name} | "
                    f"base_url: {self.base_url} | 错误: {err_msg}"
                )
                if "timeout" in err_msg.lower() or err_type in ("Timeout", "ConnectTimeout", "ReadTimeout"):
                    self.logger.error(
                        "若为超时：请检查网络或增大 config/ai_config.py 中 API_CONFIG['timeout']（当前 %ss）",
                        API_CONFIG["timeout"],
                    )
                if attempt < API_CONFIG["max_retries"] - 1:
                    self.logger.info(f"将在 {API_CONFIG['retry_delay']}s 后重试...")
                    time.sleep(API_CONFIG["retry_delay"])
                else:
                    raise

    def _call_ai_with_history(self, messages: List[Dict[str, str]],
                              system_prompt: str = "", json_mode: bool = True) -> Dict[str, Any]:
        """
        支持多轮对话上下文的 AI 调用

        Args:
            messages: 消息列表 [{"role": "user"/"assistant", "content": "..."}]
            system_prompt: 系统提示词
            json_mode: 是否启用 JSON 模式

        Returns:
            AI 返回的 JSON 对象或文本
        """
        for attempt in range(API_CONFIG["max_retries"]):
            try:
                full_messages = []
                if system_prompt:
                    full_messages.append({"role": "system", "content": system_prompt})
                full_messages.extend(messages)

                kwargs = {
                    "model": self.model_name,
                    "messages": full_messages,
                    "temperature": self.model_config["temperature"],
                    "max_tokens": self.model_config["max_tokens"],
                }

                if json_mode:
                    kwargs["response_format"] = {"type": "json_object"}

                response = self.client.chat.completions.create(**kwargs)
                content = response.choices[0].message.content

                if API_CONFIG["enable_logging"]:
                    self.logger.info(
                        f"AI 多轮调用成功 | Token: "
                        f"{response.usage.total_tokens if hasattr(response, 'usage') else 'N/A'}"
                    )

                if json_mode:
                    try:
                        return json.loads(content)
                    except json.JSONDecodeError:
                        import re
                        json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
                        if json_match:
                            return json.loads(json_match.group(1))
                        json_match = re.search(r'\{.*\}', content, re.DOTALL)
                        if json_match:
                            return json.loads(json_match.group(0))
                        raise ValueError(f"无法解析 AI 返回的 JSON: {content[:200]}")
                else:
                    return {"content": content}

            except Exception as e:
                self.logger.error(f"AI 多轮调用失败 (尝试 {attempt + 1}/{API_CONFIG['max_retries']}): {e}")
                if attempt < API_CONFIG["max_retries"] - 1:
                    time.sleep(API_CONFIG["retry_delay"])
                else:
                    raise

    def analyze_dataset(self, region: str, dataset_id: Optional[str] = None, mode: str = "指定研究",
                       universe: Optional[str] = None, delay: Optional[int] = None,
                       data_type: str = "MATRIX") -> Dict[str, Any]:
        """
        数据分析阶段

        Args:
            region: 区域代码（USA/CHN/IND 等）
            dataset_id: 数据集 ID（如 pv1），mode="指定研究"时必填
            mode: "全局扫描" 或 "指定研究"
            universe: Universe 参数（可选，默认使用区域默认值）
            delay: Delay 参数（可选，默认使用区域默认值）
            data_type: 数据类型（MATRIX/TEXT/ALL，默认 MATRIX）

        Returns:
            {
                "recommended_datasets": [...],  # mode="全局扫描"时返回
                "core_fields": [                # Top 5 核心字段
                    {
                        "field_name": "close",
                        "field_type": "Price",
                        "logic": "收盘价，适合构建价格动量策略"
                    },
                    ...
                ],
                "available_operators": [...]    # 推荐的操作符
            }
        """
        from core.session_manager import SessionManager
        from core.data_manager import DataManager
        from ai.prompt_templates import DATA_ANALYSIS_PROMPT

        # 1. 获取 Session
        session_mgr = SessionManager()
        if not session_mgr.is_logged_in():
            raise RuntimeError("请先登录 BRAIN 平台（运行 main.py 选择登录）")
        session = session_mgr.get_session()

        # 2. 根据模式获取数据
        if mode == "全局扫描":
            self.logger.info(f"开始全局扫描 {region} 区域的数据集...")

            # 获取该区域所有数据集
            datasets_df = DataManager.get_datasets(session, region, universe=universe, delay=delay)

            # 构建数据集元数据（简化版，避免 Token 过多）
            datasets_metadata = []
            for _, row in datasets_df.head(10).iterrows():  # 只取前 10 个数据集
                datasets_metadata.append({
                    "dataset_id": row.get("dataset", ""),
                    "description": row.get("description", "")[:100]  # 截断描述
                })

            # 获取操作符列表
            operators_df = DataManager.get_operators(session)
            regular_ops = operators_df[operators_df["scope"] == "REGULAR"]["name"].unique().tolist() if "scope" in operators_df.columns else operators_df["name"].tolist()[:20]
            operators_list = sorted(regular_ops)

            # 构建 Prompt
            prompt = f"""
你是一位资深量化研究员。请分析以下区域的数据集，推荐最适合量化因子挖掘的 Top 3 数据集。

**区域信息：**
- Region: {region}
- 可用数据集（前 10 个）：
{json.dumps(datasets_metadata, ensure_ascii=False, indent=2)}

- 可用操作符（前 20 个）：
{operators_list}

**任务要求：**
1. 推荐 Top 3 最有价值的数据集（考虑数据完整性、金融意义、可操作性）
2. 每个数据集需说明推荐理由（50 字以内）
3. 输出格式必须为 JSON

**输出示例：**
{{
    "recommended_datasets": [
        {{"dataset_id": "pv1", "reason": "包含价格和成交量数据，适合构建动量和价量策略"}},
        {{"dataset_id": "fundamental17", "reason": "基本面数据，适合价值因子挖掘"}},
        {{"dataset_id": "analyst4", "reason": "分析师数据，可用于市场情绪分析"}}
    ]
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

            # 调用 AI
            result = self._call_ai(prompt, json_mode=True)

            self.logger.info(f"全局扫描完成，推荐了 {len(result.get('recommended_datasets', []))} 个数据集")
            return result

        elif mode == "指定研究":
            if not dataset_id:
                raise ValueError("指定研究模式下必须提供 dataset_id")

            self.logger.info(f"开始分析 {region}/{dataset_id} 数据集（数据类型: {data_type}）...")

            # 获取字段列表
            fields_df = DataManager.get_fields(session, region, dataset_id, universe=universe, delay=delay, data_type=data_type)

            # 字段为空时自动 fallback 到 ALL 类型
            if fields_df is None or fields_df.empty:
                if data_type != "ALL":
                    self.logger.warning(f"{data_type} 类型下无字段，自动切换到 ALL 类型重试...")
                    print(f"⚠️  {data_type} 类型下无字段，自动切换到 ALL 类型重试...")
                    data_type = "ALL"
                    fields_df = DataManager.get_fields(session, region, dataset_id, universe=universe, delay=delay, data_type=data_type)

            # ALL 也为空则报错退出
            if fields_df is None or fields_df.empty:
                raise ValueError(
                    f"数据集 {dataset_id} 在 {region}/{universe}/delay={delay} 下没有任何字段（已尝试 MATRIX 和 ALL 类型）。\n"
                    f"请确认数据集 ID 是否正确，或该数据集在当前区域/Universe 下不可用。"
                )

            # 构建字段元数据（只传必要信息）
            # 兼容不同的字段名称格式（id 或 name）
            field_name_col = "id" if "id" in fields_df.columns else "name"

            fields_metadata = []
            for _, row in fields_df.iterrows():
                field_info = {
                    "name": row.get(field_name_col, ""),
                    "description": row.get("description", "")[:80] if pd.notna(row.get("description")) else "",
                    "type": row.get("type", data_type),  # 添加字段类型信息
                    "coverage": row.get("coverage"),         # 新增：数据覆盖率
                    "dateCoverage": row.get("dateCoverage")  # 新增：时间覆盖率
                }
                fields_metadata.append(field_info)

            total_fields = len(fields_metadata)

            # 动态 Token 管理：如果字段过多，智能筛选
            MAX_FIELDS = 100  # 最大字段数量限制
            if total_fields > MAX_FIELDS:
                self.logger.warning(f"字段数量 ({total_fields}) 超过限制 ({MAX_FIELDS})，将启动智能筛选")

                # 两阶段筛选策略
                print(f"\n📊 数据集共有 {total_fields} 个字段，超过 Token 限制")
                print("请选择筛选策略:")
                print("  1. 智能筛选（推荐）- AI 从全部字段中选出最有价值的 100 个")
                print("  2. 优先级筛选 - 优先保留有描述的字段（快速）")
                print("  3. 手动指定 - 输入字段名称列表")

                filter_choice = input("请选择 (1-3, 默认 1): ").strip() or "1"

                if filter_choice == "1":
                    # 智能筛选：让 AI 从全部字段中选出最有价值的
                    self.logger.info("启动智能筛选模式")
                    print(f"\n🤔 AI 正在从 {total_fields} 个字段中筛选最有价值的 {MAX_FIELDS} 个...")

                    # 构建筛选 Prompt
                    filter_prompt = f"""
你是一位资深量化研究员。请从以下 {total_fields} 个字段中筛选出最有价值的 {MAX_FIELDS} 个字段用于量化分析。

**字段列表：**
{json.dumps(fields_metadata, ensure_ascii=False, indent=2)}

**筛选标准（按优先级排序）：**
1. **数据覆盖率**：优先选择 coverage > 0.5 的字段
2. **金融意义**：优先选择有明确金融逻辑的字段（价格、成交量、基本面、技术指标）
3. **描述完整性**：优先选择有详细描述的字段
4. **字段类型**：确保覆盖多种类型（Price、Volume、Returns、Fundamental、Technical）
5. **时间覆盖**：优先选择 dateCoverage > 0.3 的字段

**任务要求：**
1. 筛选出 {MAX_FIELDS} 个最有价值的字段
2. 确保字段类型多样化（不要只选一种类型）
3. 输出字段名称列表（JSON 数组格式）

**输出格式（必须为 JSON）：**
{{
    "selected_fields": ["field1", "field2", ..., "field{MAX_FIELDS}"],
    "selection_reason": "简要说明筛选理由（50 字以内）"
}}

请严格按照 JSON 格式输出，不要包含任何其他文字。
"""

                    # 调用 AI 筛选
                    filter_result = self._call_ai(filter_prompt, json_mode=True)
                    selected_field_names = set(filter_result.get("selected_fields", []))
                    selection_reason = filter_result.get("selection_reason", "N/A")

                    # 过滤字段
                    fields_metadata = [f for f in fields_metadata if f["name"] in selected_field_names]

                    print(f"✅ AI 筛选完成，保留 {len(fields_metadata)} 个字段")
                    print(f"📝 筛选理由: {selection_reason}")

                elif filter_choice == "2":
                    # 优先级筛选（原有逻辑）
                    self.logger.info("使用优先级筛选模式")
                    fields_with_desc = [f for f in fields_metadata if f["description"]]
                    fields_without_desc = [f for f in fields_metadata if not f["description"]]

                    if len(fields_with_desc) > MAX_FIELDS:
                        fields_metadata = fields_with_desc[:MAX_FIELDS]
                    else:
                        remaining = MAX_FIELDS - len(fields_with_desc)
                        fields_metadata = fields_with_desc + fields_without_desc[:remaining]

                    print(f"✅ 优先级筛选完成，保留 {len(fields_metadata)} 个字段")

                elif filter_choice == "3":
                    # 手动指定
                    self.logger.info("使用手动指定模式")
                    print("\n请输入字段名称（逗号分隔，最多 100 个）:")
                    field_names_input = input().strip()
                    if field_names_input:
                        selected_names = set(name.strip() for name in field_names_input.split(","))
                        fields_metadata = [f for f in fields_metadata if f["name"] in selected_names]
                        print(f"✅ 手动筛选完成，保留 {len(fields_metadata)} 个字段")
                    else:
                        # 回退到优先级筛选
                        self.logger.warning("未输入字段名称，回退到优先级筛选")
                        fields_with_desc = [f for f in fields_metadata if f["description"]]
                        fields_metadata = fields_with_desc[:MAX_FIELDS]
                        print(f"✅ 回退到优先级筛选，保留 {len(fields_metadata)} 个字段")

                self.logger.info(f"最终筛选至 {len(fields_metadata)} 个字段")

            # 询问用户是否全部分析（仅在字段数量适中时询问）
            elif total_fields > 50:
                print(f"\n📊 数据集共有 {total_fields} 个字段（类型: {data_type}）")
                analyze_all = input(f"是否分析全部字段？(y/N，默认只分析前 50 个): ").strip().lower()
                if analyze_all != "y":
                    self.logger.warning(f"用户选择仅分析前 50 个字段")
                    fields_metadata = fields_metadata[:50]
                else:
                    self.logger.info(f"用户选择分析全部 {total_fields} 个字段")

            # 获取操作符列表
            operators_df = DataManager.get_operators(session)
            regular_ops = operators_df[operators_df["scope"] == "REGULAR"]["name"].unique().tolist() if "scope" in operators_df.columns else operators_df["name"].tolist()[:30]
            operators_list = sorted(regular_ops)

            # 构建 Prompt
            prompt = DATA_ANALYSIS_PROMPT.format(
                region=region,
                dataset_id=dataset_id,
                fields_metadata=json.dumps(fields_metadata, ensure_ascii=False, indent=2),
                operators_metadata=json.dumps(operators_list, ensure_ascii=False)
            )

            # 调用 AI
            result = self._call_ai(prompt, json_mode=True)

            # 验证字段是否存在于平台库中
            available_fields = set(fields_df[field_name_col].tolist())
            validated_fields = []

            for field in result.get("core_fields", []):
                field_name = field.get("field_name", "")
                if field_name in available_fields:
                    validated_fields.append(field)
                else:
                    self.logger.warning(f"字段 {field_name} 不存在于平台库中，已过滤")

            # 覆盖率硬过滤
            from config.ai_config import COVERAGE_THRESHOLDS

            # 构建覆盖率查询表
            field_coverage_map = {}
            for _, row in fields_df.iterrows():
                fname = row.get(field_name_col, "")
                field_coverage_map[fname] = {
                    "coverage": row.get("coverage"),
                    "dateCoverage": row.get("dateCoverage"),
                    "data_type": row.get("type", data_type),  # 真实平台字段类型
                }

            # 硬过滤
            final_fields = []
            for field in validated_fields:
                fname = field.get("field_name", "")
                cov_info = field_coverage_map.get(fname, {})
                cov = cov_info.get("coverage")
                dcov = cov_info.get("dateCoverage")

                # 注入覆盖率值和真实平台类型
                field["coverage"] = cov
                field["dateCoverage"] = dcov
                field["data_type"] = cov_info.get("data_type")  # 真实平台字段类型（MATRIX/VECTOR等）

                if cov is not None and cov < COVERAGE_THRESHOLDS["min_coverage"]:
                    self.logger.warning(f"字段 {fname} 覆盖率 {cov:.2f} < {COVERAGE_THRESHOLDS['min_coverage']}，已剔除")
                    continue

                if cov is not None and cov < COVERAGE_THRESHOLDS["low_coverage_warning"]:
                    self.logger.warning(f"字段 {fname} 覆盖率偏低 ({cov:.2f})，已标记警告")
                    field["coverage_warning"] = True

                if dcov is not None and dcov < COVERAGE_THRESHOLDS["min_date_coverage"]:
                    self.logger.warning(f"字段 {fname} 时间覆盖率 {dcov:.2f} 不足，已标记")
                    field["date_coverage_warning"] = True

                final_fields.append(field)

            result["core_fields"] = final_fields

            self.logger.info(f"数据分析完成，筛选出 {len(final_fields)} 个核心字段")
            return result

        else:
            raise ValueError(f"不支持的模式: {mode}，可选: 全局扫描, 指定研究")

    def analyze_multi_datasets(self, region: str, dataset_ids: list,
                               universe: str = None, delay: int = None,
                               data_type: str = "MATRIX") -> Dict[str, Any]:
        """
        多数据集联合分析阶段

        Args:
            region: 区域代码
            dataset_ids: 数据集 ID 列表（如 ["pv1", "analyst15"]）
            universe: Universe 参数
            delay: Delay 参数
            data_type: 数据类型

        Returns:
            与 analyze_dataset() 兼容的结果字典，额外包含 dataset_dimensions
        """
        from core.session_manager import SessionManager
        from core.data_manager import DataManager
        from ai.prompt_templates import MULTI_DATASET_ANALYSIS_PROMPT
        from config.ai_config import MULTI_DATASET_CONFIG, COVERAGE_THRESHOLDS

        # 1. 获取 Session
        session_mgr = SessionManager()
        if not session_mgr.is_logged_in():
            raise RuntimeError("请先登录 BRAIN 平台（运行 main.py 选择登录）")
        session = session_mgr.get_session()

        self.logger.info(f"开始多数据集联合分析: {region}/{dataset_ids}")

        # 2. 获取合并字段
        merged_df, per_dataset_dict = DataManager.get_multi_fields(
            session, region, dataset_ids,
            universe=universe, delay=delay, data_type=data_type
        )

        # 3. 分层筛选 — 第一层：规则筛选（不消耗 Token）
        fields_per_ds = MULTI_DATASET_CONFIG["fields_per_dataset"]
        total_limit = MULTI_DATASET_CONFIG["total_fields_limit"]

        filtered_frames = []
        for ds_id, ds_df in per_dataset_dict.items():
            ds_filtered = ds_df.copy()
            # 过滤 coverage < 0.3
            if "coverage" in ds_filtered.columns:
                ds_filtered = ds_filtered[
                    ds_filtered["coverage"].fillna(0) >= COVERAGE_THRESHOLDS["min_coverage"]
                ]
                ds_filtered = ds_filtered.sort_values("coverage", ascending=False)
            # 取 Top N
            ds_filtered = ds_filtered.head(fields_per_ds)
            filtered_frames.append(ds_filtered)
            self.logger.info(f"  {ds_id}: {len(ds_df)} → {len(ds_filtered)} 个字段（规则筛选）")

        filtered_df = pd.concat(filtered_frames, ignore_index=True)

        # 去重
        field_name_col = "id" if "id" in filtered_df.columns else "name"
        if "coverage" in filtered_df.columns:
            filtered_df = filtered_df.sort_values("coverage", ascending=False)
        filtered_df = filtered_df.drop_duplicates(subset=[field_name_col], keep="first")

        print(f"📊 规则筛选后: {len(filtered_df)} 个字段")

        # 第二层：超限时 AI 智能筛选
        if len(filtered_df) > total_limit:
            self.logger.warning(f"字段数 ({len(filtered_df)}) 超过限制 ({total_limit})，启动 AI 智能筛选")
            print(f"🤔 字段数超过 {total_limit}，AI 正在智能筛选...")

            # 构建筛选 Prompt
            fields_for_filter = []
            for _, row in filtered_df.iterrows():
                fields_for_filter.append({
                    "name": row.get(field_name_col, ""),
                    "source_dataset": row.get("source_dataset", ""),
                    "coverage": row.get("coverage"),
                    "description": str(row.get("description", ""))[:60] if pd.notna(row.get("description")) else "",
                })

            # 计算每个数据集最少保留数
            min_per_ds = max(5, total_limit // len(dataset_ids) // 2)

            filter_prompt = f"""
从以下 {len(fields_for_filter)} 个字段中筛选出最有价值的 {total_limit} 个用于跨数据集量化分析。
每个数据集至少保留 {min_per_ds} 个字段。

字段列表：
{json.dumps(fields_for_filter, ensure_ascii=False, indent=2)}

输出格式（必须为 JSON）：
{{"selected_fields": ["field1", "field2", ...], "selection_reason": "简要理由"}}
"""
            filter_result = self._call_ai(filter_prompt, json_mode=True)
            selected_names = set(filter_result.get("selected_fields", []))
            filtered_df = filtered_df[filtered_df[field_name_col].isin(selected_names)]
            print(f"✅ AI 筛选完成，保留 {len(filtered_df)} 个字段")

        # 4. 构建按数据集分组的字段元数据
        grouped_fields = {}
        for _, row in filtered_df.iterrows():
            ds = row.get("source_dataset", "unknown")
            if ds not in grouped_fields:
                grouped_fields[ds] = []
            field_info = {
                "name": row.get(field_name_col, ""),
                "description": str(row.get("description", ""))[:80] if pd.notna(row.get("description")) else "",
                "type": row.get("type", data_type),
                "coverage": row.get("coverage"),
                "dateCoverage": row.get("dateCoverage"),
            }
            grouped_fields[ds].append(field_info)

        # 格式化分组字段文本
        grouped_text_parts = []
        for ds_id, fields_list in grouped_fields.items():
            grouped_text_parts.append(f"\n--- 数据集: {ds_id} ({len(fields_list)} 个字段) ---")
            grouped_text_parts.append(json.dumps(fields_list, ensure_ascii=False, indent=2))
        grouped_fields_text = "\n".join(grouped_text_parts)

        # 5. 获取操作符
        operators_df = DataManager.get_operators(session)
        regular_ops = (
            operators_df[operators_df["scope"] == "REGULAR"]["name"].unique().tolist()
            if "scope" in operators_df.columns
            else operators_df["name"].tolist()[:30]
        )
        operators_list = sorted(regular_ops)

        # 6. 构建 Prompt 并调用 AI
        prompt = MULTI_DATASET_ANALYSIS_PROMPT.format(
            region=region,
            dataset_ids=", ".join(dataset_ids),
            grouped_fields_metadata=grouped_fields_text,
            operators_metadata=json.dumps(operators_list, ensure_ascii=False),
        )

        result = self._call_ai(prompt, json_mode=True)

        # 7. 验证字段存在性
        available_fields = set(filtered_df[field_name_col].tolist())
        validated_fields = []
        for field in result.get("core_fields", []):
            fname = field.get("field_name", "")
            if fname in available_fields:
                validated_fields.append(field)
            else:
                self.logger.warning(f"字段 {fname} 不存在于合并字段池中，已过滤")

        # 8. 覆盖率硬过滤
        field_coverage_map = {}
        for _, row in filtered_df.iterrows():
            fname = row.get(field_name_col, "")
            field_coverage_map[fname] = {
                "coverage": row.get("coverage"),
                "dateCoverage": row.get("dateCoverage"),
                "data_type": row.get("type", data_type),
                "source_dataset": row.get("source_dataset", ""),
            }

        final_fields = []
        for field in validated_fields:
            fname = field.get("field_name", "")
            cov_info = field_coverage_map.get(fname, {})
            cov = cov_info.get("coverage")
            dcov = cov_info.get("dateCoverage")

            field["coverage"] = cov
            field["dateCoverage"] = dcov
            field["data_type"] = cov_info.get("data_type")
            # 确保 source_dataset 存在
            if not field.get("source_dataset"):
                field["source_dataset"] = cov_info.get("source_dataset", "")

            if cov is not None and cov < COVERAGE_THRESHOLDS["min_coverage"]:
                self.logger.warning(f"字段 {fname} 覆盖率 {cov:.2f} < {COVERAGE_THRESHOLDS['min_coverage']}，已剔除")
                continue
            if cov is not None and cov < COVERAGE_THRESHOLDS["low_coverage_warning"]:
                field["coverage_warning"] = True
            if dcov is not None and dcov < COVERAGE_THRESHOLDS["min_date_coverage"]:
                field["date_coverage_warning"] = True

            final_fields.append(field)

        result["core_fields"] = final_fields

        # 9. 注入上下文
        result["context"] = {
            "region": region,
            "universe": universe,
            "delay": delay,
            "dataset_ids": dataset_ids,
            "data_type": data_type,
        }

        self.logger.info(f"多数据集联合分析完成，筛选出 {len(final_fields)} 个核心字段")
        return result

    def recommend_backtest_params(
        self,
        strategy_config: Dict[str, Any],
        analysis_result: Dict[str, Any],
        region: str = "USA",
        universe: str = "TOP3000",
        delay: int = 1
    ) -> Dict[str, Any]:
        """
        回测参数推荐阶段（新增）

        Args:
            strategy_config: build_strategy() 的返回结果
            analysis_result: analyze_dataset() 的返回结果
            region: 区域代码
            universe: Universe
            delay: Delay

        Returns:
            回测参数推荐结果（包含推荐理由和最终配置）
        """
        from ai.prompt_templates import BACKTEST_PARAMS_RECOMMENDATION_PROMPT

        self.logger.info("开始推荐回测参数...")

        # 构建 Prompt
        prompt = BACKTEST_PARAMS_RECOMMENDATION_PROMPT.format(
            strategy_config=json.dumps(strategy_config, ensure_ascii=False, indent=2),
            analysis_result=json.dumps(analysis_result, ensure_ascii=False, indent=2),
            region=region,
            universe=universe,
            delay=delay
        )

        # 调用 AI
        result = self._call_ai(prompt, json_mode=True)

        self.logger.info(f"回测参数推荐完成")

        return result

    def recommend_strategy(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        策略方向推荐阶段（新增）

        Args:
            analysis_result: analyze_dataset() 的返回结果

        Returns:
            策略推荐结果（包含 2-3 个推荐方向和自动选择）
        """
        from ai.prompt_templates import STRATEGY_RECOMMENDATION_PROMPT

        self.logger.info("开始推荐策略方向...")

        # 构建 Prompt
        prompt = STRATEGY_RECOMMENDATION_PROMPT.format(
            analysis_result=json.dumps(analysis_result, ensure_ascii=False, indent=2)
        )

        # 调用 AI
        result = self._call_ai(prompt, json_mode=True)

        self.logger.info(f"策略推荐完成，推荐 {len(result.get('recommended_strategies', []))} 个方向")

        return result

    def build_strategy(self, analysis_result: Dict[str, Any], strategy_focus: str = "动量反转", region: str = "USA", universe: str = "TOP3000", delay: int = 1) -> Dict[str, Any]:
        """
        策略构建阶段（支持多模板生成）

        Args:
            analysis_result: analyze_dataset() 的返回结果
            strategy_focus: 策略方向提示（如"动量反转"、"价值因子"）
            region: 区域代码（用于回测参数）
            universe: Universe（用于回测参数）
            delay: Delay（用于回测参数）

        Returns:
            策略配置（包含多个模板、字段规则、生成脚本、回测参数等）
        """
        from ai.prompt_templates import STRATEGY_BUILD_PROMPT
        from config.ai_config import STRATEGY_TYPE_PARAMS
        from datetime import datetime

        self.logger.info(f"开始构建策略，方向: {strategy_focus}")

        # 1. 提取核心字段、操作符、字段组合
        core_fields = analysis_result.get("core_fields", [])
        operators = analysis_result.get("available_operators", [])
        field_combinations = analysis_result.get("field_combinations", [])

        if not core_fields:
            raise ValueError("分析结果中没有核心字段，无法构建策略")

        # 2. 构建 Prompt（传递字段组合和回测参数）
        prompt = STRATEGY_BUILD_PROMPT.format(
            core_fields=json.dumps(core_fields, ensure_ascii=False, indent=2),
            field_combinations=json.dumps(field_combinations, ensure_ascii=False, indent=2),
            operators=json.dumps(operators, ensure_ascii=False),
            strategy_focus=strategy_focus,
            region=region,
            universe=universe,
            delay=delay
        )

        # 3. 调用 AI
        result = self._call_ai(prompt, json_mode=True)

        # 4. 验证模板数量（至少 2 个）
        templates = result.get("templates", [])
        if len(templates) < 2:
            self.logger.warning(f"AI 只生成了 {len(templates)} 个模板，建议至少 3 个")

        # 4.1 模板多样性检查
        if templates:
            template_types = set(t.get("template_type", "") for t in templates)
            if len(template_types) < 3 and len(templates) >= 3:
                self.logger.warning(f"模板类型多样性不足：仅 {len(template_types)} 种类型 ({', '.join(template_types)})，建议至少 3 种")

            # 检查外层操作符是否全部相同
            import re as _re
            outer_ops = []
            for t in templates:
                tpl = t.get("template", "")
                m = _re.match(r'([a-z_]+)\s*\(', tpl)
                if m:
                    outer_ops.append(m.group(1))
            if len(set(outer_ops)) == 1 and len(outer_ops) > 1:
                self.logger.warning(f"所有模板外层操作符相同 ({outer_ops[0]})，结构同质化风险高")

        # 5. 验证字段候选列表（支持多模板）
        available_field_names = set(f["field_name"] for f in core_fields)

        for template in templates:
            field_rules = template.get("field_rules", {})
            for placeholder, rule in field_rules.items():
                # 防御性检查：rule 可能为 None
                if rule is None:
                    self.logger.warning(f"模板 {template.get('template_type', 'unknown')} 的字段规则 {placeholder} 为空，已跳过")
                    field_rules[placeholder] = {"candidates": []}
                    continue

                candidates = rule.get("candidates", [])
                valid_candidates = []

                for field in candidates:
                    if field in available_field_names:
                        valid_candidates.append(field)
                    else:
                        self.logger.warning(f"字段 {field} 不在分析结果中，已过滤")

                rule["candidates"] = valid_candidates

        # 6. 根据 strategy_type 匹配回测参数
        strategy_type = result.get("strategy_type", "medium_term")
        backtest_params = STRATEGY_TYPE_PARAMS.get(strategy_type, STRATEGY_TYPE_PARAMS["medium_term"]).copy()

        # 移除 description 字段
        backtest_params.pop("description", None)

        # 注入区域/数据集等上下文信息（供下游溯源使用）
        backtest_params["region"] = region
        backtest_params["universe"] = universe
        backtest_params["delay"] = delay
        ctx = analysis_result.get("context", {})
        backtest_params["dataset_id"] = ctx.get("dataset_id") or "_".join(ctx.get("dataset_ids", ["unknown"]))

        # 传递完整上下文（供 strategy_generator 溯源）
        result["context"] = analysis_result.get("context", {})

        # 7. 计算预估生成数量（所有模板的总和）
        estimated_count = 0
        for template in templates:
            template_count = 1
            field_rules = template.get("field_rules", {})
            window_ranges = template.get("window_ranges", {})

            # 计算字段组合数
            for rule in field_rules.values():
                # 防御性检查：rule 可能为 None
                if rule is None:
                    continue
                candidates = rule.get("candidates", [])
                if candidates:
                    template_count *= len(candidates)

            # 计算参数组合数
            for values in window_ranges.values():
                if values:
                    template_count *= len(values)

            estimated_count += template_count
            template["estimated_count"] = template_count

        result["estimated_count"] = estimated_count
        result["backtest_params"] = backtest_params

        self.logger.info(f"策略构建完成，预估生成 {estimated_count} 个 Alpha（{len(templates)} 个模板）")

        return result

    def diagnose_and_fix(
        self,
        error_analysis: Dict[str, Any],
        failed_alpha: Dict[str, Any],
        session,
        region: str,
        dataset_id: str,
        universe: str = None,
        delay: int = None
    ) -> Dict[str, Any]:
        """
        诊断并修复错误（自愈系统核心方法）

        Args:
            error_analysis: ErrorAnalyzer.analyze_error() 的返回结果
            failed_alpha: 失败的 Alpha 配置（包含 expression, settings 等）
            session: BRAIN Session
            region: 区域代码
            dataset_id: 数据集 ID
            universe: Universe（可选）
            delay: Delay（可选）

        Returns:
            {
                "fixed_alpha": {...},  # 修复后的 Alpha 配置
                "fix_log": "...",      # 修复日志
                "confidence": 0.8,     # 修复置信度
                "success": True/False  # 是否修复成功
            }
        """
        from core.data_manager import DataManager
        from ai.prompt_templates import ERROR_FIX_PROMPT

        error_type = error_analysis["error_type"]
        error_category = error_analysis["error_category"]
        affected_entity = error_analysis["affected_entity"]
        error_message = error_analysis["error_message"]

        self.logger.info(f"[Self-Healing] 开始诊断错误: {error_category} - {affected_entity}")

        # 提取失败的表达式
        failed_expression = failed_alpha.get("expression", failed_alpha.get("regular", ""))
        if isinstance(failed_expression, dict):
            failed_expression = failed_expression.get("code", "")

        # 类型 A: 字段错误
        if error_type == "A":
            try:
                # 1. 刷新字段列表
                self.logger.info(f"[Self-Healing] 刷新数据集 '{dataset_id}' 字段列表...")
                fields_df = DataManager.force_refresh_fields(
                    session, region, dataset_id,
                    universe=universe or failed_alpha.get("settings", {}).get("universe"),
                    delay=delay if delay is not None else failed_alpha.get("settings", {}).get("delay")
                )

                # 2. 查找相似字段
                available_fields = fields_df["id"].tolist() if "id" in fields_df.columns else fields_df["name"].tolist()
                similar_fields = DataManager.find_similar_field(affected_entity, available_fields)

                if not similar_fields:
                    return {
                        "fixed_alpha": None,
                        "fix_log": f"未找到字段 '{affected_entity}' 的替代方案",
                        "confidence": 0.0,
                        "success": False
                    }

                # 3. 调用 AI 重构表达式
                prompt = ERROR_FIX_PROMPT.format(
                    failed_expression=failed_expression,
                    error_message=error_message,
                    error_type=f"{error_type} ({error_category})",
                    affected_entity=affected_entity,
                    suggested_alternatives=json.dumps(similar_fields, ensure_ascii=False)
                )

                fix_result = self._call_ai(prompt, json_mode=True)

                # 4. 构建修复后的 Alpha 配置
                fixed_alpha = failed_alpha.copy()
                fixed_alpha["expression"] = fix_result["fixed_expression"]
                if "regular" in fixed_alpha:
                    fixed_alpha["regular"] = fix_result["fixed_expression"]

                return {
                    "fixed_alpha": fixed_alpha,
                    "fix_log": f"字段 '{affected_entity}' → '{fix_result['selected_alternative']}' ({fix_result['selection_reason']})",
                    "confidence": fix_result.get("confidence", 0.7),
                    "success": True,
                    "changes_made": fix_result.get("changes_made", ""),
                    "risk_warning": fix_result.get("risk_warning", "")
                }

            except Exception as e:
                self.logger.error(f"[Self-Healing] 字段错误修复失败: {e}")
                return {
                    "fixed_alpha": None,
                    "fix_log": f"修复失败: {str(e)}",
                    "confidence": 0.0,
                    "success": False
                }

        # 类型 B: 操作符错误
        elif error_type == "B":
            try:
                # 1. 刷新操作符列表
                self.logger.info(f"[Self-Healing] 刷新操作符列表...")
                operators_df = DataManager.force_refresh_operators(session)

                # 2. 查找相似操作符
                available_operators = operators_df["name"].tolist()
                similar_operators = DataManager.find_similar_operator(affected_entity, available_operators)

                if not similar_operators:
                    return {
                        "fixed_alpha": None,
                        "fix_log": f"未找到操作符 '{affected_entity}' 的替代方案",
                        "confidence": 0.0,
                        "success": False
                    }

                # 3. 调用 AI 重构表达式
                prompt = ERROR_FIX_PROMPT.format(
                    failed_expression=failed_expression,
                    error_message=error_message,
                    error_type=f"{error_type} ({error_category})",
                    affected_entity=affected_entity,
                    suggested_alternatives=json.dumps(similar_operators, ensure_ascii=False)
                )

                fix_result = self._call_ai(prompt, json_mode=True)

                # 4. 构建修复后的 Alpha 配置
                fixed_alpha = failed_alpha.copy()
                fixed_alpha["expression"] = fix_result["fixed_expression"]
                if "regular" in fixed_alpha:
                    fixed_alpha["regular"] = fix_result["fixed_expression"]

                return {
                    "fixed_alpha": fixed_alpha,
                    "fix_log": f"操作符 '{affected_entity}' → '{fix_result['selected_alternative']}' ({fix_result['selection_reason']})",
                    "confidence": fix_result.get("confidence", 0.6),
                    "success": True,
                    "changes_made": fix_result.get("changes_made", ""),
                    "risk_warning": fix_result.get("risk_warning", "")
                }

            except Exception as e:
                self.logger.error(f"[Self-Healing] 操作符错误修复失败: {e}")
                return {
                    "fixed_alpha": None,
                    "fix_log": f"修复失败: {str(e)}",
                    "confidence": 0.0,
                    "success": False
                }

        # 类型 C: 配置错误
        elif error_type == "C":
            # 配置错误通常需要调整参数，暂不支持自动修复
            return {
                "fixed_alpha": None,
                "fix_log": f"配置错误暂不支持自动修复: {affected_entity}",
                "confidence": 0.0,
                "success": False
            }

        # 类型 D: 平台限制
        elif error_type == "D":
            # 平台限制错误（如批量数量超限）不需要修复表达式
            return {
                "fixed_alpha": None,
                "fix_log": f"平台限制错误，无需修复表达式: {error_message}",
                "confidence": 0.0,
                "success": False
            }

        # 类型 E: 语法错误
        elif error_type == "E":
            # 语法错误需要 AI 深度分析，暂不支持
            return {
                "fixed_alpha": None,
                "fix_log": f"语法错误暂不支持自动修复: {error_message}",
                "confidence": 0.0,
                "success": False
            }

        # 未知错误
        else:
            return {
                "fixed_alpha": None,
                "fix_log": f"未知错误类型，无法修复: {error_type}",
                "confidence": 0.0,
                "success": False
            }

    @staticmethod
    def analyze_backtest_results(results: List[Dict]) -> Dict[str, Any]:
        """
        纯统计分析回测结果（不调用 AI）

        Args:
            results: results.json 中的结果列表

        Returns:
            统计摘要字典
        """
        import statistics

        if not results:
            return {"total": 0, "error": "结果为空"}

        # 分离有效结果和错误结果
        valid_results = [r for r in results if r.get("sharpe") is not None]
        error_results = [r for r in results if r.get("error_message")]

        total = len(results)
        valid_count = len(valid_results)

        if not valid_results:
            return {
                "total": total,
                "valid_count": 0,
                "error_count": len(error_results),
                "error": "没有有效的回测结果",
                "error_results": error_results[:5]
            }

        # 提取指标
        sharpes = [r["sharpe"] for r in valid_results]
        fitnesses = [r.get("fitness", 0) for r in valid_results]
        turnovers = [r.get("turnover", 0) for r in valid_results]
        returns_list = [r.get("returns", 0) for r in valid_results]

        # 成功/失败统计
        target_sharpe = OPTIMIZATION_CONFIG["target_sharpe"]
        success = [r for r in valid_results if r.get("sharpe", 0) >= target_sharpe]
        negative = [r for r in valid_results if r.get("sharpe", 0) < 0]

        # 按 Sharpe 排序
        sorted_by_sharpe = sorted(valid_results, key=lambda x: x.get("sharpe", 0), reverse=True)
        top_n = OPTIMIZATION_CONFIG["top_n_success"]
        bottom_n = OPTIMIZATION_CONFIG["bottom_n_failure"]

        # 按 template_type 分组统计
        template_stats = {}
        for r in valid_results:
            ttype = r.get("template_type", "unknown")
            if ttype not in template_stats:
                template_stats[ttype] = {"count": 0, "sharpes": [], "fitnesses": [], "turnovers": []}
            template_stats[ttype]["count"] += 1
            template_stats[ttype]["sharpes"].append(r.get("sharpe", 0))
            template_stats[ttype]["fitnesses"].append(r.get("fitness", 0))
            template_stats[ttype]["turnovers"].append(r.get("turnover", 0))

        for ttype, stats in template_stats.items():
            s = stats["sharpes"]
            stats["sharpe_mean"] = statistics.mean(s) if s else 0
            stats["sharpe_max"] = max(s) if s else 0
            stats["fitness_mean"] = statistics.mean(stats["fitnesses"]) if stats["fitnesses"] else 0
            stats["turnover_mean"] = statistics.mean(stats["turnovers"]) if stats["turnovers"] else 0

        # 失败模式分类
        failure_patterns = {
            "sharpe_negative": len(negative),
            "high_turnover": len([r for r in valid_results if r.get("turnover", 0) > 0.70]),
            "low_fitness": len([r for r in valid_results if 0 <= r.get("fitness", 0) < 0.5]),
            "error_results": len(error_results),
        }

        # 错误类型分布
        error_type_dist = {}
        for r in results:
            ea = r.get("error_analysis")
            if ea and isinstance(ea, dict):
                etype = ea.get("error_type", "unknown")
                error_type_dist[etype] = error_type_dist.get(etype, 0) + 1

        def _safe_std(values):
            return statistics.stdev(values) if len(values) >= 2 else 0.0

        return {
            "total": total,
            "valid_count": valid_count,
            "success_count": len(success),
            "fail_count": valid_count - len(success),
            "negative_count": len(negative),
            "success_rate": len(success) / valid_count if valid_count > 0 else 0,
            "sharpe": {
                "mean": statistics.mean(sharpes),
                "median": statistics.median(sharpes),
                "max": max(sharpes),
                "min": min(sharpes),
                "std": _safe_std(sharpes),
            },
            "fitness": {
                "mean": statistics.mean(fitnesses),
                "median": statistics.median(fitnesses),
                "max": max(fitnesses),
                "min": min(fitnesses),
            },
            "turnover": {
                "mean": statistics.mean(turnovers),
                "median": statistics.median(turnovers),
                "max": max(turnovers),
                "min": min(turnovers),
            },
            "returns": {
                "mean": statistics.mean(returns_list),
                "max": max(returns_list),
                "min": min(returns_list),
            },
            "top_n": sorted_by_sharpe[:top_n],
            "bottom_n": sorted_by_sharpe[-bottom_n:] if len(sorted_by_sharpe) >= bottom_n else sorted_by_sharpe,
            "template_type_stats": template_stats,
            "failure_patterns": failure_patterns,
            "error_type_dist": error_type_dist,
            "error_results": error_results[:5],
        }

    def optimize_strategy(self, backtest_results: List[Dict[str, Any]], original_strategy: Dict[str, Any]) -> Dict[str, Any]:
        """
        闭环优化阶段

        Args:
            backtest_results: results.json 中的结果列表
            original_strategy: 原始策略配置（可选，可为空字典）

        Returns:
            AI 分析结果 + 改进策略
        """
        from ai.prompt_templates import OPTIMIZATION_PROMPT

        # 1. 统计分析
        stats = self.analyze_backtest_results(backtest_results)

        if stats.get("valid_count", 0) < OPTIMIZATION_CONFIG["min_results_for_optimization"]:
            return {
                "stats": stats,
                "error": f"有效结果数量不足（{stats.get('valid_count', 0)} < {OPTIMIZATION_CONFIG['min_results_for_optimization']}），无法优化"
            }

        # 2. 格式化成功/失败案例
        def _format_case(r):
            return (
                f"  expression: {r.get('expression', 'N/A')[:80]}\n"
                f"  sharpe: {r.get('sharpe', 'N/A')}, fitness: {r.get('fitness', 'N/A')}, "
                f"turnover: {r.get('turnover', 'N/A')}, template_type: {r.get('template_type', 'N/A')}"
            )

        success_cases_str = "\n\n".join(_format_case(r) for r in stats["top_n"])
        failure_cases_str = "\n\n".join(_format_case(r) for r in stats["bottom_n"])

        # 3. 格式化失败模式
        fp = stats["failure_patterns"]
        failure_pattern_lines = [
            f"- Sharpe 为负: {fp['sharpe_negative']} 个",
            f"- Turnover 过高 (>0.70): {fp['high_turnover']} 个",
            f"- Fitness 偏低 (<0.5): {fp['low_fitness']} 个",
            f"- 回测错误: {fp['error_results']} 个",
        ]
        failure_pattern_summary = "\n".join(failure_pattern_lines)

        # 4. 格式化模板类型统计
        ts = stats["template_type_stats"]
        template_lines = []
        for ttype, tstat in ts.items():
            template_lines.append(
                f"- {ttype}: {tstat['count']} 个, Sharpe 均值 {tstat['sharpe_mean']:.3f}, "
                f"最大 {tstat['sharpe_max']:.3f}, Fitness 均值 {tstat['fitness_mean']:.3f}, "
                f"Turnover 均值 {tstat['turnover_mean']:.3f}"
            )
        template_type_stats_str = "\n".join(template_lines) if template_lines else "无模板类型信息"

        # 5. 构建 Prompt
        s = stats["sharpe"]
        f = stats["fitness"]
        t = stats["turnover"]
        ret = stats["returns"]

        prompt = OPTIMIZATION_PROMPT.format(
            original_strategy=json.dumps(original_strategy, ensure_ascii=False, indent=2)[:3000],
            total_count=stats["total"],
            success_count=stats["success_count"],
            fail_count=stats["fail_count"],
            negative_count=stats["negative_count"],
            success_rate=stats["success_rate"],
            sharpe_mean=s["mean"],
            sharpe_median=s["median"],
            sharpe_max=s["max"],
            sharpe_min=s["min"],
            fitness_mean=f["mean"],
            fitness_median=f["median"],
            turnover_mean=t["mean"],
            turnover_median=t["median"],
            returns_mean=ret["mean"],
            failure_pattern_summary=failure_pattern_summary,
            top_n=OPTIMIZATION_CONFIG["top_n_success"],
            success_cases=success_cases_str or "无成功案例",
            bottom_n=OPTIMIZATION_CONFIG["bottom_n_failure"],
            failure_cases=failure_cases_str or "无失败案例",
            template_type_stats=template_type_stats_str,
        )

        # 6. 调用 AI
        self.logger.info("调用 AI 进行闭环优化分析...")
        ai_result = self._call_ai(prompt, json_mode=True)

        # 7. 防御性检查
        if "updated_strategy" not in ai_result:
            self.logger.warning("AI 返回结果缺少 updated_strategy")
            ai_result["updated_strategy"] = {}

        updated = ai_result["updated_strategy"]
        if "generation_script" not in updated:
            self.logger.warning("AI 返回的 updated_strategy 缺少 generation_script")

        # 注入统计数据
        ai_result["stats"] = stats

        return ai_result

    def generate_dataset_config(
        self,
        region: str,
        dataset_id: str,
        dataset_name: str,
        dataset_description: str,
        fields_metadata: List[Dict[str, Any]],
        operators_list: List[str],
        num_templates: int = 20,
        num_directions: int = 4,
    ) -> Dict[str, Any]:
        """
        AI 自动分析新数据集并生成研究方向和模板配置。

        Args:
            region: 区域代码（如 USA）
            dataset_id: 数据集 ID（如 analyst10）
            dataset_name: 数据集名称
            dataset_description: 数据集描述
            fields_metadata: 字段元数据列表
            operators_list: 可用操作符列表
            num_templates: 生成模板数量（默认 20）
            num_directions: 研究方向数量（默认 4）

        Returns:
            {
                "dataset_id": "...",
                "dataset_name": "...",
                "research_directions": [...],
                "priority_fields": [...],
                "field_pairs": [...],
                "templates": [...],
                "guidance": {...}
            }
        """
        from ai.prompt_templates import DATASET_CONFIG_GENERATION_PROMPT

        self.logger.info(f"开始生成数据集配置: {dataset_id}")

        # 1. 准备字段元数据字符串（限制数量避免 token 过多）
        max_fields = 150
        fields_to_send = fields_metadata[:max_fields]

        fields_str_lines = []
        for f in fields_to_send:
            field_id = f.get("field_id", f.get("id", "unknown"))
            field_name = f.get("field_name", f.get("name", ""))
            coverage = f.get("coverage", 0)
            field_type = f.get("type", f.get("normalized_type", "MATRIX"))
            description = f.get("description", "")[:50] if f.get("description") else ""

            fields_str_lines.append(
                f"- {field_id}: type={field_type}, coverage={coverage:.2%}, desc={description}"
            )

        fields_str = "\n".join(fields_str_lines)
        if len(fields_metadata) > max_fields:
            fields_str += f"\n... (共 {len(fields_metadata)} 个字段，仅显示前 {max_fields} 个)"

        # 2. 准备操作符列表字符串
        operators_str = ", ".join(operators_list[:100])
        if len(operators_list) > 100:
            operators_str += f" ... (共 {len(operators_list)} 个)"

        # 3. 构建 Prompt
        prompt = DATASET_CONFIG_GENERATION_PROMPT.format(
            region=region,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            dataset_description=dataset_description or "无描述",
            total_fields=len(fields_metadata),
            fields_metadata=fields_str,
            operators_metadata=operators_str,
            num_directions=num_directions,
            num_templates=num_templates,
        )

        # 4. 调用 AI
        self.logger.info("调用 AI 生成数据集配置...")
        ai_result = self._call_ai(prompt, json_mode=True)

        # 5. 构建返回结果
        result = {
            "dataset_id": dataset_id,
            "dataset_name": dataset_name,
            "description": dataset_description,
            "field_analysis": {
                "total_fields": len(fields_metadata),
                "matrix_fields": sum(1 for f in fields_metadata if f.get("type", f.get("normalized_type", "")) in ["MATRIX", "vector"]),
                "vector_fields": sum(1 for f in fields_metadata if f.get("type", f.get("normalized_type", "")) in ["VECTOR", "event"]),
            },
            "research_directions": ai_result.get("research_directions", []),
            "priority_fields": ai_result.get("priority_fields", []),
            "field_pairs": ai_result.get("field_pairs", []),
            "templates": ai_result.get("templates", []),
            "guidance": {
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "description": dataset_description,
                "research_directions": ai_result.get("research_directions", []),
                "priority_fields": ai_result.get("priority_fields", []),
                "field_pairs": ai_result.get("field_pairs", []),
                "guidance_prompt": ai_result.get("guidance_prompt", ""),
            },
        }

        self.logger.info(
            f"数据集配置生成完成: {len(result['research_directions'])} 个研究方向, "
            f"{len(result['templates'])} 个模板"
        )

        return result

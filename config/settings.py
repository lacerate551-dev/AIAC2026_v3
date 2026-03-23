"""
全局设置常量 - AIAC2025_v2 量化因子挖掘平台
"""
import os
from pathlib import Path

# ==================== 路径配置 ====================
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
CONFIG_DIR = PROJECT_ROOT / "config"
CACHE_DIR = PROJECT_ROOT / "cache"
RESEARCH_DIR = PROJECT_ROOT / "research"
AI_DIR = PROJECT_ROOT / "ai"
AI_GENERATED_DIR = AI_DIR / "generated_strategies"

# 确保关键目录存在
CACHE_DIR.mkdir(exist_ok=True)
(CACHE_DIR / "regions").mkdir(exist_ok=True)
(CACHE_DIR / "dataset_fields").mkdir(exist_ok=True)
RESEARCH_DIR.mkdir(exist_ok=True)
AI_DIR.mkdir(exist_ok=True)
AI_GENERATED_DIR.mkdir(exist_ok=True)

# ==================== BRAIN 平台配置 ====================
BRAIN_API_URL = os.environ.get("BRAIN_API_URL", "https://api.worldquantbrain.com")
BRAIN_PLATFORM_URL = os.environ.get("BRAIN_URL", "https://platform.worldquantbrain.com")

# 凭证文件路径 (JSON: ["email", "password"])
CREDENTIALS_FILE = CONFIG_DIR / "credentials.json"

# AI API Keys 配置文件路径
API_KEYS_FILE = CONFIG_DIR / "api_keys.json"

# ==================== 区域默认配置 ====================
REGION_DEFAULTS = {
    "USA": {"universe": "TOP3000", "delay": 1},
    "IND": {"universe": "TOP500", "delay": 1},
    "CHN": {"universe": "TOP2000U", "delay": 1},
    "EUR": {"universe": "TOP1200", "delay": 1},
    "ASI": {"universe": "TOP1500", "delay": 1},
    "GLB": {"universe": "TOP3000", "delay": 1},
    "JPN": {"universe": "TOP500", "delay": 1},
    "KOR": {"universe": "TOP500", "delay": 1},
    "TWN": {"universe": "TOP500", "delay": 1},
}

# ==================== Alpha 回测默认参数 ====================
ALPHA_DEFAULTS = {
    "decay": 5,
    "truncation": 0.08,
    "neutralization": "INDUSTRY",
    "pasteurization": "ON",
    "unit_handling": "VERIFY",
    "nan_handling": "OFF",
}

# ==================== 回测筛选标准 ====================
MIN_SHARPE = 1.0
MIN_FITNESS = 0.5
MAX_TURNOVER = 0.70

# ==================== 缓存文件名模式 ====================
OPERATORS_CACHE = CACHE_DIR / "operators.json"
REGION_DATASETS_CACHE = lambda region: CACHE_DIR / "regions" / f"{region}_datasets.json"
DATASET_FIELDS_CACHE = lambda region, dataset: CACHE_DIR / "dataset_fields" / f"{region}_{dataset}_fields.json"

# ==================== Agent 记忆目录 ====================
MEMORY_DIR = PROJECT_ROOT / "memory"
MEMORY_DIR.mkdir(exist_ok=True)

# ==================== 批量回测配置 ====================
BATCH_SIZE = 10  # 每批回测的Alpha数量（BRAIN 平台限制最多 10 个）
MAX_CONCURRENT_SIMS = 3  # 最大并发数（已废弃，保留兼容性）

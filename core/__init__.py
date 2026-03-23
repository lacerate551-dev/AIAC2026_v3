"""
AIAC2025_v2 核心模块
- session_manager: 会话管理（登录/重连）
- data_manager: 数据获取与缓存
- alpha_builder: Alpha 表达式构建
- backtest_runner: 回测执行与报告
"""
from .session_manager import SessionManager
from .data_manager import DataManager
from .alpha_builder import AlphaBuilder
from .backtest_runner import BacktestRunner

__all__ = ["SessionManager", "DataManager", "AlphaBuilder", "BacktestRunner"]

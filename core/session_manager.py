"""
会话管理模块 - 封装 BRAIN 平台登录与会话生命周期管理
"""
import json
import sys
from pathlib import Path

# 将 core 目录的父级加入 Python path, 以便 ace_lib 能正确导入
sys.path.insert(0, str(Path(__file__).parent))

from . import ace_lib
from config.settings import CREDENTIALS_FILE


class SessionManager:
    """BRAIN 平台会话管理器（单例）"""

    _session = None

    @classmethod
    def login(cls, credentials_file: str = None) -> object:
        """
        登录 BRAIN 平台。

        优先使用 config/credentials.json 中的凭证。
        如果凭证文件不存在，则回退到 ace_lib 默认的交互式登录。

        Args:
            credentials_file: 可选的凭证文件路径

        Returns:
            已认证的 Session 对象
        """
        cred_path = Path(credentials_file) if credentials_file else CREDENTIALS_FILE

        # 如果项目内有凭证文件，则设置环境变量让 ace_lib 使用
        if cred_path.exists():
            import os
            with open(cred_path, "r", encoding="utf-8") as f:
                creds = json.load(f)
            if isinstance(creds, list) and len(creds) == 2:
                os.environ["BRAIN_CREDENTIAL_EMAIL"] = creds[0]
                os.environ["BRAIN_CREDENTIAL_PASSWORD"] = creds[1]
            elif isinstance(creds, dict):
                os.environ["BRAIN_CREDENTIAL_EMAIL"] = creds.get("email", "")
                os.environ["BRAIN_CREDENTIAL_PASSWORD"] = creds.get("password", "")

        cls._session = ace_lib.start_session()
        print("[OK] Login successful")
        return cls._session

    @classmethod
    def get_session(cls) -> object:
        """
        获取当前会话。如果尚未登录或已超时，自动登录/重连。

        Returns:
            已认证的 Session 对象
        """
        if cls._session is None:
            return cls.login()

        # 检查超时并自动重连
        cls._session = ace_lib.check_session_and_relogin(cls._session)
        return cls._session

    @classmethod
    def check_timeout(cls) -> int:
        """
        检查当前会话的剩余有效时间（秒）。

        Returns:
            剩余秒数，0 表示已过期
        """
        if cls._session is None:
            return 0
        return ace_lib.check_session_timeout(cls._session)

    @classmethod
    def is_logged_in(cls) -> bool:
        """是否已登录且会话有效"""
        return cls._session is not None and cls.check_timeout() > 0

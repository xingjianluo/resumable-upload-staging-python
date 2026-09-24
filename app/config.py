"""运行配置（暂存根目录、会话有效期、锁/IO 超时等）。

可通过环境变量覆盖：
- STAGING_DIR              暂存根目录（本地磁盘模拟共享存储）
- SESSION_TTL_SECONDS      会话默认有效期（秒）
- CLEANUP_INTERVAL_SECONDS 过期清理扫描间隔（秒）
- LOCK_TIMEOUT_SECONDS     会话文件锁等待上限（秒）
- FLUSH_RETRY_ATTEMPTS     分片落盘重试次数
- FLUSH_RETRY_BACKOFF_SECONDS 落盘重试退避基数（秒）
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    staging_dir: str = "staging_data"
    session_ttl_seconds: int = 3600
    cleanup_interval_seconds: int = 300
    file_lock_timeout_seconds: float = 10.0
    flush_retry_attempts: int = 3
    flush_retry_backoff_seconds: float = 0.05


def get_settings() -> Settings:
    """从环境变量读取配置，缺省值保持本地可直接运行。"""
    return Settings(
        staging_dir=os.environ.get("STAGING_DIR", "staging_data"),
        session_ttl_seconds=int(os.environ.get("SESSION_TTL_SECONDS", "3600")),
        cleanup_interval_seconds=int(os.environ.get("CLEANUP_INTERVAL_SECONDS", "300")),
        file_lock_timeout_seconds=float(os.environ.get("LOCK_TIMEOUT_SECONDS", "10.0")),
        flush_retry_attempts=int(os.environ.get("FLUSH_RETRY_ATTEMPTS", "3")),
        flush_retry_backoff_seconds=float(
            os.environ.get("FLUSH_RETRY_BACKOFF_SECONDS", "0.05")
        ),
    )

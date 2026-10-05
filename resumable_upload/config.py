"""暂存服务的配置项。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StagingConfig:
    """分片暂存相关配置。

    - staging_dir: 本地磁盘模拟远端共享存储的暂存根目录
    - session_ttl_seconds: 会话有效期，过期会话的写入/提交会被拒绝
    - cleanup_interval_seconds: 后台过期清理周期
    - chunk_max_bytes: 单个分片允许的最大字节数
    - max_staging_bytes: 整个暂存目录允许占用的最大字节数；None 表示不限
    """

    staging_dir: Path = Path("staging")
    session_ttl_seconds: float = 3600.0
    cleanup_interval_seconds: float = 300.0
    chunk_max_bytes: int = 16 * 1024 * 1024
    max_staging_bytes: int | None = None

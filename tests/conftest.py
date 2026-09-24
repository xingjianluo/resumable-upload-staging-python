"""测试夹具：独立暂存目录的 manager 与可预测的随机数据。"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

import pytest

from resumable_upload.config import StagingConfig
from resumable_upload.manager import UploadManager


@pytest.fixture
def staging_dir(tmp_path: Path) -> Path:
    path = tmp_path / "staging"
    path.mkdir()
    return path


@pytest.fixture
def config(staging_dir: Path) -> StagingConfig:
    return StagingConfig(
        staging_dir=staging_dir,
        session_ttl_seconds=3600,
        cleanup_interval_seconds=3600,
        chunk_max_bytes=4 * 1024 * 1024,
    )


@pytest.fixture
def manager(config: StagingConfig) -> UploadManager:
    mgr = UploadManager(config)
    mgr.recover()
    return mgr


@pytest.fixture(autouse=True)
def caplog_setup(caplog: pytest.LogCaptureFixture):
    """让单测日志打印会话标识、分片序号与判定依据（任务明确要求）。"""
    caplog.set_level(logging.INFO, logger="resumable_upload")
    yield


def make_payload(total_size: int, chunk_size: int) -> tuple[bytes, list[bytes], str]:
    """生成确定性测试数据：data[i] = i % 251，并切分分片。"""
    data = bytes(i % 251 for i in range(total_size))
    chunks = [data[i : i + chunk_size] for i in range(0, total_size, chunk_size)]
    return data, chunks, hashlib.sha256(data).hexdigest()

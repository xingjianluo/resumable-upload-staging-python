"""测试夹具：临时暂存目录、SessionManager 与 FastAPI 客户端。"""

from __future__ import annotations

import hashlib
import logging

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.session import SessionManager
from app.storage import DiskStore

logging.basicConfig(level=logging.INFO)


@pytest.fixture
def settings(tmp_path):
    return Settings(
        staging_dir=str(tmp_path / "staging"),
        session_ttl_seconds=3600,
        cleanup_interval_seconds=60,
        file_lock_timeout_seconds=5.0,
        flush_retry_attempts=3,
        flush_retry_backoff_seconds=0.01,
    )


@pytest.fixture
def store(settings):
    return DiskStore(settings.staging_dir)


@pytest.fixture
def manager(settings, store):
    return SessionManager(settings, store)


@pytest.fixture
def client(settings, manager):
    app = create_app(settings=settings, manager=manager)
    with TestClient(app) as client:
        yield client


@pytest.fixture
def clock():
    """可控时钟，供过期/恢复类测试推进时间。"""

    class Clock:
        def __init__(self, start: float = 1_000_000.0):
            self.now = start

        def __call__(self) -> float:
            return self.now

        def advance(self, seconds: float) -> None:
            self.now += seconds

    return Clock()


@pytest.fixture
def timed_manager(settings, store, clock):
    """使用可控时钟的 SessionManager。"""
    return SessionManager(settings, store, time_func=clock), clock


def make_payload(total: int, fill: int = 7) -> bytes:
    return bytes((i + fill) % 256 for i in range(total))


def chunk_checksums(payload: bytes, chunk_size: int, algo: str = "sha256") -> list[str]:
    return [
        hashlib.new(algo, payload[i : i + chunk_size]).hexdigest()
        for i in range(0, len(payload), chunk_size)
    ]


def split_chunks(payload: bytes, chunk_size: int) -> list[bytes]:
    return [payload[i : i + chunk_size] for i in range(0, len(payload), chunk_size)]

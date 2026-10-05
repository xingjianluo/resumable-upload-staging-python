"""暂存容量上限与回收单测：容量打满拒绝、原因可区分、释放后空间复用、并发不超限。"""

from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from resumable_upload import errors
from resumable_upload.config import StagingConfig
from resumable_upload.manager import UploadManager

from .conftest import make_payload


def _manager(staging_dir: Path, limit: int, ttl: float = 3600) -> UploadManager:
    return UploadManager(
        StagingConfig(
            staging_dir=staging_dir,
            session_ttl_seconds=ttl,
            max_staging_bytes=limit,
            chunk_max_bytes=4 * 1024 * 1024,
        )
    )


def test_create_session_rejected_when_staging_already_full(staging_dir: Path):
    manager = _manager(staging_dir, limit=250)
    _, chunks, final_sum = make_payload(250, 250)
    first = manager.create_session(
        filename="first", total_size=250, chunk_size=250,
        final_checksum=final_sum, owner_token="alice",
    )
    manager.put_chunk(first.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    assert manager.quota.load() == 250
    with pytest.raises(errors.QuotaExceeded) as exc:
        manager.create_session(
            filename="big", total_size=2000, chunk_size=10,
            final_checksum=final_sum, owner_token="bob",
        )
    assert exc.value.reason == "quota_exceeded"
    assert exc.value.limit == 250
    # 被拒后没有残留会话目录
    assert manager.storage.list_sessions() == [first.session_id]


def test_large_declared_size_session_can_be_created_before_any_usage(staging_dir: Path):
    # 声明大小大于上限本身不阻止创建（未占用任何暂存字节），
    # 上限在分片写入时强制
    manager = _manager(staging_dir, limit=100)
    _, _, final_sum = make_payload(10, 10)
    session = manager.create_session(
        filename="big", total_size=2000, chunk_size=10,
        final_checksum=final_sum, owner_token="alice",
    )
    assert session.session_id in manager.storage.list_sessions()


def test_chunk_write_rejected_when_quota_fills(staging_dir: Path):
    # 上限恰好只够前两个分片，第三个分片必须被拒绝
    manager = _manager(staging_dir, limit=500)
    data, chunks, final_sum = make_payload(1000, 250)
    session = manager.create_session(
        filename="a.bin", total_size=1000, chunk_size=250,
        final_checksum=final_sum, owner_token="alice",
    )
    manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    manager.put_chunk(session.session_id, "alice", 1, chunks[1],
                      hashlib.sha256(chunks[1]).hexdigest())
    with pytest.raises(errors.QuotaExceeded) as exc:
        manager.put_chunk(session.session_id, "alice", 2, chunks[2],
                          hashlib.sha256(chunks[2]).hexdigest())
    assert exc.value.reason == "quota_exceeded"
    # 已确认分片完好无损
    on_disk = manager.storage.load_metadata(session.session_id)
    assert sorted(on_disk.chunks) == [0, 1]
    assert manager.storage.read_chunk(session.session_id, 0) == chunks[0]
    assert manager.storage.read_chunk(session.session_id, 1) == chunks[1]
    assert manager.quota.load() == 500


def test_quota_rejection_is_distinguishable_from_expiry_and_unauthorized(
    staging_dir: Path,
):
    manager = _manager(staging_dir, limit=250)
    data, chunks, final_sum = make_payload(500, 250)
    session = manager.create_session(
        filename="a.bin", total_size=500, chunk_size=250,
        final_checksum=final_sum, owner_token="alice",
    )
    manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    # 容量不足
    with pytest.raises(errors.QuotaExceeded) as q:
        manager.put_chunk(session.session_id, "alice", 1, chunks[1],
                          hashlib.sha256(chunks[1]).hexdigest())
    assert q.value.reason not in ("session_expired", "unauthorized")
    # 越权
    with pytest.raises(errors.UnauthorizedSession):
        manager.put_chunk(session.session_id, "bob", 1, chunks[1],
                          hashlib.sha256(chunks[1]).hexdigest())


def test_reaped_expired_session_frees_quota_for_reuse(staging_dir: Path):
    manager = _manager(staging_dir, limit=500, ttl=100)
    _, chunks1, sum1 = make_payload(500, 250)
    # 先建空会话 s2（尚未占任何字节），再把配额占满
    s2 = manager.create_session(filename="new", total_size=250, chunk_size=250,
                                final_checksum=sum1, owner_token="bob")
    s1 = manager.create_session(filename="old", total_size=500, chunk_size=250,
                                final_checksum=sum1, owner_token="alice")
    for i, part in enumerate(chunks1):
        manager.put_chunk(s1.session_id, "alice", i, part,
                          hashlib.sha256(part).hexdigest())
    assert manager.quota.load() == 500
    # 容量已满，新会话的分片写不进去
    with pytest.raises(errors.QuotaExceeded):
        manager.put_chunk(s2.session_id, "bob", 0, chunks1[0],
                          hashlib.sha256(chunks1[0]).hexdigest())
    # 让 s1 过期并回收，空间应重新可用
    old_state = manager.storage.load_metadata(s1.session_id)
    old_state.expires_at = 0
    manager.storage.save_metadata(old_state)
    removed = manager.cleanup_expired()
    assert removed == [s1.session_id]
    assert manager.quota.load() == 0
    manager.put_chunk(s2.session_id, "bob", 0, chunks1[0],
                      hashlib.sha256(chunks1[0]).hexdigest())
    assert manager.quota.load() == 250


def test_failed_session_is_reaped_and_quota_reused(staging_dir: Path):
    manager = _manager(staging_dir, limit=500)
    _, chunks, _ = make_payload(500, 250)
    bad = manager.create_session(filename="bad", total_size=500, chunk_size=250,
                                 final_checksum="0" * 64, owner_token="alice")
    for i, part in enumerate(chunks):
        manager.put_chunk(bad.session_id, "alice", i, part,
                          hashlib.sha256(part).hexdigest())
    assert manager.quota.load() == 500
    with pytest.raises(errors.FinalChecksumMismatch):
        manager.commit(bad.session_id, "alice")
    # FAILED 会话立即具备回收资格，清理后空间复用
    removed = manager.cleanup_expired()
    assert removed == [bad.session_id]
    assert manager.quota.load() == 0
    _, chunks2, sum2 = make_payload(250, 250)
    good = manager.create_session(filename="good", total_size=250, chunk_size=250,
                                  final_checksum=sum2, owner_token="bob")
    manager.put_chunk(good.session_id, "bob", 0, chunks2[0],
                      hashlib.sha256(chunks2[0]).hexdigest())
    assert manager.quota.load() == 250


def test_successful_commit_releases_staged_quota(staging_dir: Path):
    manager = _manager(staging_dir, limit=500)
    data, chunks, final_sum = make_payload(500, 250)
    session = manager.create_session(filename="a.bin", total_size=500, chunk_size=250,
                                     final_checksum=final_sum, owner_token="alice")
    for i, part in enumerate(chunks):
        manager.put_chunk(session.session_id, "alice", i, part,
                          hashlib.sha256(part).hexdigest())
    assert manager.quota.load() == 500
    result = manager.commit(session.session_id, "alice")
    assert Path(result.result_path).read_bytes() == data
    # 暂存分片已回收，配额归零；结果文件保留
    assert manager.quota.load() == 0
    # 重复提交仍幂等返回同一路径
    again = manager.commit(session.session_id, "alice")
    assert again.result_path == result.result_path


def test_concurrent_chunk_writes_never_exceed_quota(staging_dir: Path):
    limit = 4 * 250
    manager = _manager(staging_dir, limit=limit)
    _, chunks, final_sum = make_payload(10 * 250, 250)
    session = manager.create_session(
        filename="a.bin", total_size=10 * 250, chunk_size=250,
        final_checksum=final_sum, owner_token="alice",
    )
    rejections: list[errors.QuotaExceeded] = []
    rejections_lock = threading.Lock()

    def writer(start: int):
        # 四个线程各自尝试写不相交的分片集合
        for i in range(start, 10, 4):
            try:
                manager.put_chunk(
                    session.session_id, "alice", i, chunks[i],
                    hashlib.sha256(chunks[i]).hexdigest(),
                )
            except errors.QuotaExceeded as exc:
                with rejections_lock:
                    rejections.append(exc)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(writer, range(4)))

    # 账本占用绝不能超过上限
    assert manager.quota.load() <= limit
    on_disk = manager.storage.load_metadata(session.session_id)
    assert len(on_disk.chunks) * 250 == manager.quota.load() <= limit
    # 容量装不下全部 10 个分片，必须出现明确拒绝
    assert len(on_disk.chunks) == 4
    assert rejections


def test_quota_is_reconciled_on_recover(staging_dir: Path):
    config = StagingConfig(staging_dir=staging_dir, max_staging_bytes=10_000)
    manager1 = UploadManager(config)
    _, chunks, final_sum = make_payload(500, 250)
    session = manager1.create_session(filename="a.bin", total_size=500, chunk_size=250,
                                      final_checksum=final_sum, owner_token="alice")
    for i, part in enumerate(chunks):
        manager1.put_chunk(session.session_id, "alice", i, part,
                           hashlib.sha256(part).hexdigest())
    assert manager1.quota.load() == 500
    # 模拟账本丢失/漂移（崩溃后新实例）：recover 按磁盘校准
    manager2 = UploadManager(config)
    manager2.quota._store(9999)  # noqa: SLF001 - 人为制造漂移账本
    manager2.recover()
    assert manager2.quota.load() == 500

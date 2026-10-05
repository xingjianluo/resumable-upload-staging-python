"""暂存总量约束：打满拒绝、原因可区分、回收后空间可复用、并发不超配。"""

from __future__ import annotations

import hashlib
import threading
import time
from pathlib import Path

import pytest

from resumable_upload import errors
from resumable_upload.config import StagingConfig
from resumable_upload.manager import UploadManager
from resumable_upload.models import SessionStatus

from .conftest import make_payload


def _config(staging_dir: Path, max_bytes: int, ttl: float = 3600.0) -> StagingConfig:
    return StagingConfig(
        staging_dir=staging_dir,
        session_ttl_seconds=ttl,
        max_staging_bytes=max_bytes,
    )


def test_create_session_rejected_when_quota_full(staging_dir: Path, caplog):
    _, chunks, final_sum = make_payload(400, 100)
    manager = UploadManager(_config(staging_dir, max_bytes=500))
    session = manager.create_session(filename="a", total_size=400, chunk_size=100,
                                     final_checksum=final_sum, owner_token="alice")
    # 实际写入 400 字节，把空间占到接近上限
    for i, part in enumerate(chunks):
        manager.put_chunk(session.session_id, "alice", i, part,
                          hashlib.sha256(part).hexdigest())
    with pytest.raises(errors.QuotaExceeded) as exc:
        manager.create_session(filename="b", total_size=200, chunk_size=100,
                               final_checksum=final_sum, owner_token="alice")
    # 拒绝原因必须与会话过期、越权可区分
    assert exc.value.reason == "quota_exceeded"
    assert exc.value.reason not in ("session_expired", "unauthorized")
    assert exc.value.limit_bytes == 500
    assert any("reason=quota_exceeded" in r.message for r in caplog.records)


def test_put_chunk_rejected_when_quota_full_and_confirmed_chunks_intact(staging_dir: Path):
    _, chunks_a, sum_a = make_payload(400, 200)
    _, chunks_b, sum_b = make_payload(200, 200)
    manager = UploadManager(_config(staging_dir, max_bytes=500))
    session_a = manager.create_session(filename="a", total_size=400, chunk_size=200,
                                       final_checksum=sum_a, owner_token="alice")
    filler = manager.create_session(filename="filler", total_size=200, chunk_size=200,
                                    final_checksum=sum_b, owner_token="bob")
    manager.put_chunk(session_a.session_id, "alice", 0, chunks_a[0],
                      hashlib.sha256(chunks_a[0]).hexdigest())
    manager.put_chunk(filler.session_id, "bob", 0, chunks_b[0],
                      hashlib.sha256(chunks_b[0]).hexdigest())
    # 已占用 400，再写 200 会超过 500 上限
    with pytest.raises(errors.QuotaExceeded) as exc:
        manager.put_chunk(session_a.session_id, "alice", 1, chunks_a[1],
                          hashlib.sha256(chunks_a[1]).hexdigest())
    assert exc.value.reason == "quota_exceeded"
    # 已确认分片不能被弄坏
    on_disk = manager.storage.load_metadata(session_a.session_id)
    assert sorted(on_disk.chunks) == [0]
    assert manager.storage.read_chunk(session_a.session_id, 0) == chunks_a[0]
    assert manager.storage.read_chunk(filler.session_id, 0) == chunks_b[0]
    assert not manager.storage.chunk_exists(session_a.session_id, 1)


def test_quota_rejection_differs_from_expired(staging_dir: Path):
    # 同一会话：先因容量被拒（未过期），后因过期被拒 —— 两种原因必须可区分
    _, chunks, final_sum = make_payload(200, 200)
    config = _config(staging_dir, max_bytes=200, ttl=0.05)
    manager = UploadManager(config)
    session = manager.create_session(filename="a", total_size=200, chunk_size=200,
                                     final_checksum=final_sum, owner_token="alice")
    # 把目录塞满：另一个会话先占掉空间
    _, filler_chunks, filler_sum = make_payload(200, 200)
    filler = manager.create_session(filename="filler", total_size=200, chunk_size=200,
                                    final_checksum=filler_sum, owner_token="bob")
    manager.put_chunk(filler.session_id, "bob", 0, filler_chunks[0],
                      hashlib.sha256(filler_chunks[0]).hexdigest())
    with pytest.raises(errors.QuotaExceeded):
        manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                          hashlib.sha256(chunks[0]).hexdigest())
    time.sleep(0.08)
    with pytest.raises(errors.SessionExpired):
        manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                          hashlib.sha256(chunks[0]).hexdigest())


def test_space_reclaimed_after_cleanup_allows_new_uploads(staging_dir: Path):
    data, chunks, final_sum = make_payload(300, 300)
    config = _config(staging_dir, max_bytes=400, ttl=3600)
    manager = UploadManager(config)
    session = manager.create_session(filename="a", total_size=300, chunk_size=300,
                                     final_checksum=final_sum, owner_token="alice")
    manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    # 打满：新会话被拒
    with pytest.raises(errors.QuotaExceeded):
        manager.create_session(filename="b", total_size=300, chunk_size=300,
                               final_checksum=final_sum, owner_token="alice")
    # 让旧会话过期并回收，空间必须能被复用
    state = manager.storage.load_metadata(session.session_id)
    state.expires_at = time.time() - 1
    manager.storage.save_metadata(state)
    removed = manager.cleanup_expired()
    assert removed == [session.session_id]

    session2 = manager.create_session(filename="b", total_size=300, chunk_size=300,
                                      final_checksum=final_sum, owner_token="alice")
    manager.put_chunk(session2.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    result = manager.commit(session2.session_id, "alice")
    assert result.status == SessionStatus.COMPLETED
    assert Path(result.result_path).read_bytes() == data


def test_failed_session_space_is_reclaimed(staging_dir: Path):
    data, chunks, _ = make_payload(300, 300)
    config = _config(staging_dir, max_bytes=400, ttl=3600)
    manager = UploadManager(config)
    bad = manager.create_session(filename="bad", total_size=300, chunk_size=300,
                                 final_checksum="0" * 64, owner_token="alice")
    manager.put_chunk(bad.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    with pytest.raises(errors.FinalChecksumMismatch):
        manager.commit(bad.session_id, "alice")
    # 失败会话过期后被回收，空间释放
    state = manager.storage.load_metadata(bad.session_id)
    assert state.status == SessionStatus.FAILED
    state.expires_at = time.time() - 1
    manager.storage.save_metadata(state)
    assert manager.cleanup_expired() == [bad.session_id]
    ok = manager.create_session(filename="ok", total_size=300, chunk_size=300,
                                final_checksum=hashlib.sha256(data).hexdigest(),
                                owner_token="alice")
    manager.put_chunk(ok.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    assert manager.commit(ok.session_id, "alice").status == SessionStatus.COMPLETED


def test_concurrent_writes_and_cleanup_never_exceed_quota(staging_dir: Path):
    # 多会话并发写 + 后台回收同时进行：占用不能明显超过上限，活跃会话不被误回收
    limit = 2048
    config = _config(staging_dir, max_bytes=limit, ttl=0.06)
    manager = UploadManager(config)
    _, chunks, final_sum = make_payload(256, 256)
    stop = threading.Event()
    violations: list[str] = []

    def writer(idx: int):
        while not stop.is_set():
            try:
                session = manager.create_session(
                    filename=f"w{idx}", total_size=256, chunk_size=256,
                    final_checksum=final_sum, owner_token=f"w{idx}",
                )
            except errors.QuotaExceeded:
                time.sleep(0.005)
                continue
            try:
                manager.put_chunk(session.session_id, f"w{idx}", 0, chunks[0],
                                  hashlib.sha256(chunks[0]).hexdigest())
            except (errors.QuotaExceeded, errors.SessionExpired, errors.SessionNotFound):
                pass  # 容量/过期拒绝是合法结论
            time.sleep(0.002)

    def cleaner():
        while not stop.is_set():
            manager.cleanup_expired()
            usage = manager.storage.usage_bytes()
            # 元数据本身有开销，允许少量超出，但不能明显超配
            if usage > limit + 1024:
                violations.append(f"usage={usage} limit={limit}")
            time.sleep(0.003)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    threads.append(threading.Thread(target=cleaner))
    for t in threads:
        t.start()
    time.sleep(0.6)
    stop.set()
    for t in threads:
        t.join()

    assert violations == []
    # 仍在有效期内的会话数据不能被当成垃圾清掉
    for session_id in manager.storage.list_sessions():
        state = manager.storage.load_metadata(session_id)
        assert state is not None  # 目录与元数据一致，没有清出半截会话

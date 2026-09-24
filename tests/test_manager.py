"""UploadManager 的单测：并发写入、断点恢复、幂等与越权拒绝。"""

from __future__ import annotations

import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from resumable_upload import errors
from resumable_upload.config import StagingConfig
from resumable_upload.manager import UploadManager
from resumable_upload.models import SessionStatus

from .conftest import make_payload


def _upload_all_chunks(manager: UploadManager, session, owner: str, chunks: list[bytes]):
    for i, part in enumerate(chunks):
        manager.put_chunk(
            session.session_id, owner, i, part, hashlib.sha256(part).hexdigest()
        )


def test_create_session_and_progress(manager: UploadManager, caplog):
    data, chunks, final_sum = make_payload(1000, 256)
    session = manager.create_session(
        filename="a.bin", total_size=1000, chunk_size=256,
        final_checksum=final_sum, owner_token="alice",
    )
    assert session.total_chunks == 4
    manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    progress = manager.get_progress(session.session_id, "alice")
    assert progress["received_chunks"] == [0]
    assert progress["missing_chunks"] == [1, 2, 3]
    # 日志包含会话标识、分片序号与判定依据
    records = [r.message for r in caplog.records]
    assert any(f"session={session.session_id}" in m and "session created" in m for m in records)
    assert any(
        f"session={session.session_id}" in m and "chunk=0" in m and "chunk confirmed" in m
        for m in records
    )


def test_concurrent_writes_same_chunk_are_idempotent(manager: UploadManager):
    data, chunks, final_sum = make_payload(1000, 256)
    session = manager.create_session(
        filename="c.bin", total_size=1000, chunk_size=256,
        final_checksum=final_sum, owner_token="alice",
    )
    errors_box: list[Exception] = []

    def writer(repeat: int):
        try:
            for i, part in enumerate(chunks):
                # 每个分片被不同线程重复写，正确的重复必须幂等
                manager.put_chunk(
                    session.session_id, "alice", i, part,
                    hashlib.sha256(part).hexdigest(),
                )
        except Exception as exc:  # noqa: BLE001
            errors_box.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(writer, range(8)))
    assert errors_box == []

    on_disk = manager.storage.load_metadata(session.session_id)
    assert sorted(on_disk.chunks) == [0, 1, 2, 3]
    # 磁盘分片内容没有被覆盖破坏
    for i, part in enumerate(chunks):
        assert manager.storage.read_chunk(session.session_id, i) == part

    result = manager.commit(session.session_id, "alice")
    assert result.status == SessionStatus.COMPLETED
    assert Path(result.result_path).read_bytes() == data


def test_concurrent_writes_different_sessions_are_isolated(manager: UploadManager):
    data1, chunks1, sum1 = make_payload(800, 400)
    data2, chunks2, sum2 = make_payload(600, 300)
    s1 = manager.create_session(filename="1.bin", total_size=800, chunk_size=400,
                                final_checksum=sum1, owner_token="alice")
    s2 = manager.create_session(filename="2.bin", total_size=600, chunk_size=300,
                                final_checksum=sum2, owner_token="bob")
    barrier = threading.Barrier(2)

    def write_pair(manager, session, owner, chunks):
        barrier.wait()
        _upload_all_chunks(manager, session, owner, chunks)

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(write_pair, manager, s1, "alice", chunks1)
        f2 = pool.submit(write_pair, manager, s2, "bob", chunks2)
        f1.result(); f2.result()

    r1 = manager.commit(s1.session_id, "alice")
    r2 = manager.commit(s2.session_id, "bob")
    assert Path(r1.result_path).read_bytes() == data1
    assert Path(r2.result_path).read_bytes() == data2
    assert r1.result_path != r2.result_path


def test_unauthorized_writes_are_rejected(manager: UploadManager, caplog):
    data, chunks, final_sum = make_payload(500, 500)
    session = manager.create_session(
        filename="secret.bin", total_size=500, chunk_size=500,
        final_checksum=final_sum, owner_token="alice",
    )
    with pytest.raises(errors.UnauthorizedSession) as exc:
        manager.put_chunk(session.session_id, "mallory", 0, chunks[0],
                          hashlib.sha256(chunks[0]).hexdigest())
    assert exc.value.reason == "unauthorized"
    with pytest.raises(errors.UnauthorizedSession):
        manager.commit(session.session_id, "mallory")
    with pytest.raises(errors.UnauthorizedSession):
        manager.get_progress(session.session_id, "mallory")
    # 越权的写入没有落任何分片
    assert manager.storage.load_metadata(session.session_id).chunks == {}
    assert any("reason=unauthorized" in r.message for r in caplog.records)


def test_unknown_session_is_not_found(manager: UploadManager):
    with pytest.raises(errors.SessionNotFound) as exc:
        manager.get_progress("nope", "alice")
    assert exc.value.reason == "session_not_found"


def test_checksum_mismatch_is_distinguishable(manager: UploadManager):
    data, chunks, final_sum = make_payload(500, 500)
    session = manager.create_session(
        filename="a.bin", total_size=500, chunk_size=500,
        final_checksum=final_sum, owner_token="alice",
    )
    bad = b"x" * 500
    with pytest.raises(errors.ChecksumMismatch) as exc:
        manager.put_chunk(session.session_id, "alice", 0, bad,
                          hashlib.sha256(bad).hexdigest()[:-1] + "0")
    assert exc.value.reason == "checksum_mismatch"
    assert not manager.storage.chunk_exists(session.session_id, 0)


def test_duplicate_chunk_with_different_checksum_conflicts(manager: UploadManager):
    data, chunks, final_sum = make_payload(500, 500)
    session = manager.create_session(
        filename="a.bin", total_size=500, chunk_size=500,
        final_checksum=final_sum, owner_token="alice",
    )
    good = chunks[0]
    manager.put_chunk(session.session_id, "alice", 0, good,
                      hashlib.sha256(good).hexdigest())
    bad = b"y" * 500
    with pytest.raises(errors.ChecksumMismatch):
        manager.put_chunk(session.session_id, "alice", 0, bad,
                          hashlib.sha256(bad).hexdigest())
    # 原确认分片不受影响
    assert manager.storage.read_chunk(session.session_id, 0) == good


def test_chunk_out_of_range(manager: UploadManager):
    _, _, final_sum = make_payload(10, 10)
    session = manager.create_session(filename="a", total_size=10, chunk_size=10,
                                     final_checksum=final_sum, owner_token="alice")
    with pytest.raises(errors.ChunkOutOfRange) as exc:
        manager.put_chunk(session.session_id, "alice", 5, b"z",
                          hashlib.sha256(b"z").hexdigest())
    assert exc.value.reason == "chunk_out_of_range"


def test_commit_rejects_missing_chunks_with_reason(manager: UploadManager):
    data, chunks, final_sum = make_payload(1000, 256)
    session = manager.create_session(
        filename="a.bin", total_size=1000, chunk_size=256,
        final_checksum=final_sum, owner_token="alice",
    )
    manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    with pytest.raises(errors.MissingChunks) as exc:
        manager.commit(session.session_id, "alice")
    assert exc.value.reason == "missing_chunks"
    assert exc.value.missing == [1, 2, 3]
    # 缺片失败后补齐仍可提交（非终态数据丢失，失败原因明确为缺片而非损坏）
    for i in range(1, 4):
        manager.put_chunk(session.session_id, "alice", i, chunks[i],
                          hashlib.sha256(chunks[i]).hexdigest())
    result = manager.commit(session.session_id, "alice")
    assert result.status == SessionStatus.COMPLETED
    assert Path(result.result_path).read_bytes() == data


def test_commit_rejects_final_checksum_mismatch(manager: UploadManager):
    data, chunks, _ = make_payload(1000, 256)
    session = manager.create_session(
        filename="a.bin", total_size=1000, chunk_size=256,
        final_checksum="0" * 64, owner_token="alice",
    )
    _upload_all_chunks(manager, session, "alice", chunks)
    with pytest.raises(errors.FinalChecksumMismatch) as exc:
        manager.commit(session.session_id, "alice")
    assert exc.value.reason == "final_checksum_mismatch"
    on_disk = manager.storage.load_metadata(session.session_id)
    assert on_disk.status == SessionStatus.FAILED
    assert on_disk.fail_reason == "final_checksum_mismatch"
    # 拼装产物必须被删除，不能静默留下错误文件
    assert not Path(manager.storage.completed_path(on_disk)).exists()


def test_duplicate_commit_produces_single_result(manager: UploadManager):
    data, chunks, final_sum = make_payload(1000, 256)
    session = manager.create_session(
        filename="a.bin", total_size=1000, chunk_size=256,
        final_checksum=final_sum, owner_token="alice",
    )
    _upload_all_chunks(manager, session, "alice", chunks)
    results: list = []
    barrier = threading.Barrier(5)

    def do_commit():
        barrier.wait()
        results.append(manager.commit(session.session_id, "alice"))

    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(lambda _: do_commit(), range(5)))

    assert len({r.result_path for r in results}) == 1
    assert all(r.status == SessionStatus.COMPLETED for r in results)
    completed_files = list((manager.storage.completed_root()).iterdir())
    assert len(completed_files) == 1
    assert completed_files[0].read_bytes() == data


def test_expired_session_writes_and_commit_are_rejected(staging_dir: Path):
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=0.02)
    manager = UploadManager(config)
    data, chunks, final_sum = make_payload(500, 500)
    session = manager.create_session(filename="a", total_size=500, chunk_size=500,
                                     final_checksum=final_sum, owner_token="alice")
    time.sleep(0.05)
    with pytest.raises(errors.SessionExpired) as exc:
        manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                          hashlib.sha256(chunks[0]).hexdigest())
    assert exc.value.reason == "session_expired"
    with pytest.raises(errors.SessionExpired):
        manager.commit(session.session_id, "alice")


def test_cleanup_removes_only_expired_pending_sessions(staging_dir: Path):
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=3600)
    manager = UploadManager(config)
    data, chunks, final_sum = make_payload(200, 200)
    old = manager.create_session(filename="old", total_size=200, chunk_size=200,
                                 final_checksum=final_sum, owner_token="alice")
    fresh = manager.create_session(filename="fresh", total_size=200, chunk_size=200,
                                   final_checksum=final_sum, owner_token="alice")
    done = manager.create_session(filename="done", total_size=200, chunk_size=200,
                                  final_checksum=final_sum, owner_token="alice")
    _upload_all_chunks(manager, done, "alice", chunks)
    manager.commit(done.session_id, "alice")
    # 人为把 old 置为过去时间；completed 会话即使到期也保留
    past = time.time() - 10
    old_state = manager.storage.load_metadata(old.session_id)
    old_state.expires_at = past
    manager.storage.save_metadata(old_state)
    done_state = manager.storage.load_metadata(done.session_id)
    done_state.expires_at = past
    manager.storage.save_metadata(done_state)

    removed = manager.cleanup_expired()
    assert removed == [old.session_id]
    assert manager.storage.load_metadata(old.session_id) is None
    assert manager.storage.load_metadata(fresh.session_id) is not None
    assert manager.storage.load_metadata(done.session_id) is not None


def test_commit_and_cleanup_race_has_deterministic_outcome(staging_dir: Path):
    # TTL 极短：commit 与 cleanup 并发，结论只能是"过期拒绝"或"唯一成功"之一
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=0.05)
    manager = UploadManager(config)
    data, chunks, final_sum = make_payload(500, 250)
    outcomes = {"expired": 0, "success": 0, "missing": 0}

    for trial in range(20):
        session = manager.create_session(
            filename=f"race-{trial}", total_size=500, chunk_size=250,
            final_checksum=final_sum, owner_token="alice",
        )
        _upload_all_chunks(manager, session, "alice", chunks)
        time.sleep(0.04)  # 逼近过期时刻
        commit_result: list = []
        t1 = threading.Thread(target=lambda: _collect_commit(manager, session.session_id, commit_result))
        t2 = threading.Thread(target=lambda: manager.cleanup_expired())
        t1.start(); t2.start(); t1.join(); t2.join()
        state = manager.storage.load_metadata(session.session_id)
        if commit_result and commit_result[0] == "ok":
            outcomes["success"] += 1
            assert state is not None and state.status == SessionStatus.COMPLETED
            assert Path(state.result_path).exists()
        else:
            outcomes["expired"] += 1
            # 过期路径：会话被清理，或会话仍在但提交被拒；二者必居其一且确定
            assert commit_result[0] in ("expired", "missing")
    assert outcomes["success"] + outcomes["expired"] == 20


def _collect_commit(manager, session_id, sink):
    try:
        manager.commit(session_id, "alice")
        sink.append("ok")
    except errors.SessionExpired:
        sink.append("expired")
    except errors.SessionNotFound:
        sink.append("expired")
    except errors.MissingChunks:
        sink.append("missing")


def test_progress_survives_restart(staging_dir: Path, config: StagingConfig):
    manager1 = UploadManager(config)
    data, chunks, final_sum = make_payload(1000, 256)
    session = manager1.create_session(
        filename="a.bin", total_size=1000, chunk_size=256,
        final_checksum=final_sum, owner_token="alice",
    )
    # 模拟中断：先传 0,2 两个分片
    for i in (0, 2):
        manager1.put_chunk(session.session_id, "alice", i, chunks[i],
                           hashlib.sha256(chunks[i]).hexdigest())

    # 重启：全新 manager 实例，recover 后已确认进度仍在
    manager2 = UploadManager(config)
    count = manager2.recover()
    assert count == 1
    progress = manager2.get_progress(session.session_id, "alice")
    assert progress["received_chunks"] == [0, 2]
    assert progress["missing_chunks"] == [1, 3]

    # 断点续传：补齐剩余分片后提交
    for i in (1, 3):
        manager2.put_chunk(session.session_id, "alice", i, chunks[i],
                           hashlib.sha256(chunks[i]).hexdigest())
    result = manager2.commit(session.session_id, "alice")
    assert Path(result.result_path).read_bytes() == data


def test_orphan_chunk_after_crash_is_discarded_on_recover(staging_dir: Path, config: StagingConfig):
    manager1 = UploadManager(config)
    data, chunks, final_sum = make_payload(512, 256)
    session = manager1.create_session(
        filename="a.bin", total_size=512, chunk_size=256,
        final_checksum=final_sum, owner_token="alice",
    )
    manager1.put_chunk(session.session_id, "alice", 0, chunks[0],
                       hashlib.sha256(chunks[0]).hexdigest())
    # 模拟"数据已写、元数据未更新"的崩溃窗口：直接写分片文件但不更新 meta
    manager1.storage.write_chunk_atomic(
        session.session_id, 1, chunks[1], hashlib.sha256(chunks[1]).hexdigest()
    )
    manager2 = UploadManager(config)
    manager2.recover()
    progress = manager2.get_progress(session.session_id, "alice")
    assert progress["received_chunks"] == [0]  # 孤儿分片不被当成已确认
    manager2.put_chunk(session.session_id, "alice", 1, chunks[1],
                       hashlib.sha256(chunks[1]).hexdigest())
    result = manager2.commit(session.session_id, "alice")
    assert result.status == SessionStatus.COMPLETED


def test_empty_file_session_commits(manager: UploadManager):
    final_sum = hashlib.sha256(b"").hexdigest()
    session = manager.create_session(filename="empty", total_size=0, chunk_size=100,
                                     final_checksum=final_sum, owner_token="alice")
    result = manager.commit(session.session_id, "alice")
    assert result.status == SessionStatus.COMPLETED
    assert Path(result.result_path).read_bytes() == b""

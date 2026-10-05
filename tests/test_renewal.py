"""会话续期/保活单测：顺延有效期、保活期间不被回收、停止续期后按时拒绝与回收。"""

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


def _make_manager(staging_dir: Path, ttl: float, max_ttl: float = 0) -> UploadManager:
    return UploadManager(
        StagingConfig(
            staging_dir=staging_dir,
            session_ttl_seconds=ttl,
            max_session_ttl_seconds=max_ttl,
        )
    )


def test_renew_extends_expiry_and_keeps_progress(staging_dir: Path):
    manager = _make_manager(staging_dir, ttl=100)
    data, chunks, final_sum = make_payload(1000, 256)
    session = manager.create_session(
        filename="a.bin", total_size=1000, chunk_size=256,
        final_checksum=final_sum, owner_token="alice",
    )
    for i in (0, 2):
        manager.put_chunk(
            session.session_id, "alice", i, chunks[i],
            hashlib.sha256(chunks[i]).hexdigest(),
        )
    before = session.expires_at
    renewed = manager.renew_session(session.session_id, "alice", 1800)
    assert renewed.expires_at > before
    # 已确认分片与校验结论不变
    assert renewed.status == SessionStatus.PENDING
    assert sorted(renewed.chunks) == [0, 2]
    progress = manager.get_progress(session.session_id, "alice")
    assert progress["received_chunks"] == [0, 2]
    assert progress["missing_chunks"] == [1, 3]
    # 续期后仍可继续写入并提交
    for i in (1, 3):
        manager.put_chunk(
            session.session_id, "alice", i, chunks[i],
            hashlib.sha256(chunks[i]).hexdigest(),
        )
    result = manager.commit(session.session_id, "alice")
    assert Path(result.result_path).read_bytes() == data


def test_renewed_session_is_not_reaped_while_kept_alive(staging_dir: Path):
    manager = _make_manager(staging_dir, ttl=0.15)
    _, chunks, final_sum = make_payload(500, 250)
    session = manager.create_session(
        filename="a.bin", total_size=500, chunk_size=250,
        final_checksum=final_sum, owner_token="alice",
    )
    manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    # 持续续期，期间多次触发清理都不能回收
    for _ in range(3):
        time.sleep(0.1)
        manager.renew_session(session.session_id, "alice", 0.3)
        assert manager.cleanup_expired() == []
        assert manager.storage.load_metadata(session.session_id) is not None
    assert manager.get_progress(session.session_id, "alice")["received_chunks"] == [0]


def test_session_expires_after_renewal_stops(staging_dir: Path):
    manager = _make_manager(staging_dir, ttl=0.1)
    _, chunks, final_sum = make_payload(250, 250)
    session = manager.create_session(
        filename="a.bin", total_size=250, chunk_size=250,
        final_checksum=final_sum, owner_token="alice",
    )
    manager.renew_session(session.session_id, "alice", 0.2)
    # 在新的有效期内仍可写
    manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    # 停止续期，超过延长后的有效期 -> 写/提交被拒，并可被清理
    time.sleep(0.25)
    with pytest.raises(errors.SessionExpired):
        manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                          hashlib.sha256(chunks[0]).hexdigest())
    with pytest.raises(errors.SessionExpired):
        manager.commit(session.session_id, "alice")
    removed = manager.cleanup_expired()
    assert removed == [session.session_id]
    assert manager.storage.load_metadata(session.session_id) is None


def test_renew_after_expiry_is_rejected(staging_dir: Path):
    manager = _make_manager(staging_dir, ttl=0.05)
    _, _, final_sum = make_payload(10, 10)
    session = manager.create_session(
        filename="a", total_size=10, chunk_size=10,
        final_checksum=final_sum, owner_token="alice",
    )
    time.sleep(0.08)
    with pytest.raises(errors.SessionExpired):
        manager.renew_session(session.session_id, "alice", 60)


def test_renew_finalized_or_failed_session_is_rejected(manager: UploadManager):
    data, chunks, final_sum = make_payload(500, 500)
    done = manager.create_session(
        filename="done", total_size=500, chunk_size=500,
        final_checksum=final_sum, owner_token="alice",
    )
    manager.put_chunk(done.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    manager.commit(done.session_id, "alice")
    with pytest.raises(errors.SessionAlreadyFinalized):
        manager.renew_session(done.session_id, "alice", 60)

    failed = manager.create_session(
        filename="bad", total_size=500, chunk_size=500,
        final_checksum="0" * 64, owner_token="alice",
    )
    manager.put_chunk(failed.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    with pytest.raises(errors.FinalChecksumMismatch):
        manager.commit(failed.session_id, "alice")
    with pytest.raises(errors.SessionFailed):
        manager.renew_session(failed.session_id, "alice", 60)


def test_renew_unauthorized_is_rejected(manager: UploadManager):
    _, _, final_sum = make_payload(10, 10)
    session = manager.create_session(
        filename="a", total_size=10, chunk_size=10,
        final_checksum=final_sum, owner_token="alice",
    )
    with pytest.raises(errors.UnauthorizedSession):
        manager.renew_session(session.session_id, "mallory", 60)


def test_renewed_expiry_survives_restart(staging_dir: Path):
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=3600)
    manager1 = UploadManager(config)
    _, _, final_sum = make_payload(10, 10)
    session = manager1.create_session(
        filename="a", total_size=10, chunk_size=10,
        final_checksum=final_sum, owner_token="alice",
    )
    renewed = manager1.renew_session(session.session_id, "alice", 7200)
    expected_expiry = renewed.expires_at

    manager2 = UploadManager(config)
    manager2.recover()
    progress = manager2.get_progress(session.session_id, "alice")
    assert progress["expires_at"] == expected_expiry
    on_disk = manager2.storage.load_metadata(session.session_id)
    assert on_disk.expires_at == expected_expiry


def test_renew_respects_max_ttl_cap(manager: UploadManager):
    _, _, final_sum = make_payload(10, 10)
    session = manager.create_session(
        filename="a", total_size=10, chunk_size=10,
        final_checksum=final_sum, owner_token="alice",
    )
    # 默认 max_session_ttl_seconds = 7 天：续期 30 天也不能越过创建时刻 + 上限
    renewed = manager.renew_session(session.session_id, "alice", 30 * 24 * 3600)
    assert renewed.expires_at <= session.created_at + manager.config.max_session_ttl_seconds


def test_renew_and_cleanup_race_in_threads_has_single_outcome(staging_dir: Path):
    manager = _make_manager(staging_dir, ttl=0.05)
    _, chunks, final_sum = make_payload(200, 200)
    renewed_kept = 0
    expired_gone = 0
    for trial in range(30):
        session = manager.create_session(
            filename=f"race-{trial}", total_size=200, chunk_size=200,
            final_checksum=final_sum, owner_token="alice",
        )
        manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                          hashlib.sha256(chunks[0]).hexdigest())
        outcome: list[str] = []

        def do_renew():
            try:
                manager.renew_session(session.session_id, "alice", 3600)
                outcome.append("renewed")
            except errors.UploadError as exc:
                outcome.append(exc.reason)

        def do_cleanup():
            removed = manager.cleanup_expired()
            outcome.append("cleanup:" + (",".join(removed) if removed else "none"))

        t1 = threading.Thread(target=do_renew)
        t2 = threading.Thread(target=do_cleanup)
        time.sleep(0.04)
        t1.start(); t2.start(); t1.join(); t2.join()

        on_disk = manager.storage.load_metadata(session.session_id)
        if on_disk is not None:
            # 续期成功：会话必存活且有效期在很久以后，随后的清理也不能删
            renewed_kept += 1
            assert "renewed" in outcome
            assert on_disk.expires_at > time.time() + 3000
            assert on_disk.chunks[0].checksum == hashlib.sha256(chunks[0]).hexdigest()
            assert manager.cleanup_expired() == []
        else:
            # 清理先赢：会话被删除，续期方只能得到过期/不存在结论，状态唯一
            expired_gone += 1
            assert any(o in ("session_expired", "session_not_found") for o in outcome)
    # 进程内续期通常先拿到锁；至少续期成功的分支必须成立，且两种结局都
    # 满足"唯一结论 + 数据完好"。清理先赢的分支由多进程竞态用例覆盖。
    assert renewed_kept + expired_gone == 30
    assert renewed_kept > 0


def test_renew_never_shortens_when_cap_below_current_ttl(staging_dir: Path):
    # 当前 TTL=3600，而总时长上限只给 60s（已被当前有效期越过），
    # 续期也不能把有效期缩短，只能保持原 expires_at
    config = StagingConfig(
        staging_dir=staging_dir, session_ttl_seconds=3600, max_session_ttl_seconds=60
    )
    manager = UploadManager(config)
    _, _, final_sum = make_payload(10, 10)
    session = manager.create_session(
        filename="a", total_size=10, chunk_size=10,
        final_checksum=final_sum, owner_token="alice",
    )
    renewed = manager.renew_session(session.session_id, "alice", 10)
    assert renewed.expires_at == session.expires_at

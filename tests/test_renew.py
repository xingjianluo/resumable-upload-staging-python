"""会话续期/保活：延长有效期、与回收的竞争、重启后有效期不丢。"""

from __future__ import annotations

import hashlib
import threading
import time
from pathlib import Path

import pytest

from resumable_upload import errors
from resumable_upload.config import StagingConfig
from resumable_upload.manager import UploadManager

from .conftest import make_payload


def _new_session(manager: UploadManager, name: str = "renew.bin", size: int = 500):
    data, chunks, final_sum = make_payload(size, 250)
    session = manager.create_session(
        filename=name, total_size=size, chunk_size=250,
        final_checksum=final_sum, owner_token="alice",
    )
    return session, data, chunks, final_sum


def test_renew_extends_expiry_and_keeps_confirmed_progress(staging_dir: Path):
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=0.2)
    manager = UploadManager(config)
    session, _, chunks, _ = _new_session(manager)
    manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    before = manager.get_progress(session.session_id, "alice")

    renewed = manager.renew_session(session.session_id, "alice")

    assert renewed.expires_at > before["expires_at"]
    after = manager.get_progress(session.session_id, "alice")
    # 已确认分片与校验结论不因续期改变
    assert after["received_chunks"] == before["received_chunks"] == [0]
    assert after["missing_chunks"] == before["missing_chunks"] == [1]
    on_disk = manager.storage.load_metadata(session.session_id)
    assert on_disk.chunks[0].checksum == hashlib.sha256(chunks[0]).hexdigest()


def test_renew_never_shortens_expiry(staging_dir: Path):
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=3600)
    manager = UploadManager(config)
    session, _, _, _ = _new_session(manager)
    original = session.expires_at
    # 用很短的续期时长，有效期不允许被缩短
    renewed = manager.renew_session(session.session_id, "alice", extend_seconds=0.001)
    assert renewed.expires_at == original


def test_continuous_renewal_survives_cleanup(staging_dir: Path):
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=0.4)
    manager = UploadManager(config)
    session, _, chunks, _ = _new_session(manager)
    manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                      hashlib.sha256(chunks[0]).hexdigest())
    # 持续续期 0.8s，远超原始 TTL；期间多次清理都不该回收
    deadline = time.time() + 0.8
    while time.time() < deadline:
        manager.renew_session(session.session_id, "alice")
        removed = manager.cleanup_expired()
        assert session.session_id not in removed
        time.sleep(0.05)
    progress = manager.get_progress(session.session_id, "alice")
    assert progress["received_chunks"] == [0]


def test_stopping_renewal_leads_to_reject_and_reclaim(staging_dir: Path, caplog):
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=0.3)
    manager = UploadManager(config)
    session, _, chunks, _ = _new_session(manager)
    manager.renew_session(session.session_id, "alice")
    time.sleep(0.4)  # 停止续期并超过有效期
    with pytest.raises(errors.SessionExpired) as exc:
        manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                          hashlib.sha256(chunks[0]).hexdigest())
    assert exc.value.reason == "session_expired"
    with pytest.raises(errors.SessionExpired):
        manager.commit(session.session_id, "alice")
    with pytest.raises(errors.SessionExpired):
        manager.renew_session(session.session_id, "alice")
    removed = manager.cleanup_expired()
    assert removed == [session.session_id]
    assert manager.storage.load_metadata(session.session_id) is None
    assert any(
        f"session={session.session_id}" in r.message and "expired-removed" in r.message
        for r in caplog.records
    )


def test_renew_and_cleanup_race_has_unique_outcome(staging_dir: Path):
    # 续期与回收并发：最终结论唯一 —— 要么续期成功且会话存活，要么会话已被回收
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=0.05)
    manager = UploadManager(config)
    outcomes = {"renewed": 0, "removed": 0}
    for trial in range(20):
        session, _, _, _ = _new_session(manager, name=f"race-{trial}")
        time.sleep(0.04)  # 逼近过期时刻
        renew_result: list = []

        def do_renew(session_id=session.session_id, sink=renew_result):
            try:
                manager.renew_session(session_id, "alice")
                sink.append("ok")
            except (errors.SessionNotFound, errors.SessionExpired):
                sink.append("lost")

        t1 = threading.Thread(target=do_renew)
        t2 = threading.Thread(target=lambda: manager.cleanup_expired())
        t1.start(); t2.start(); t1.join(); t2.join()

        state = manager.storage.load_metadata(session.session_id)
        if renew_result[0] == "ok":
            outcomes["renewed"] += 1
            # 续期成功：会话必须还在，且有效期已被延长到原有效期之后
            assert state is not None
            assert state.expires_at > session.expires_at
        else:
            outcomes["removed"] += 1
            # 续期失败：会话要么已被回收，要么仍过期（下一轮清理必回收）
            if state is not None:
                assert state.expires_at < time.time()
                assert manager.cleanup_expired() == [session.session_id]
    assert outcomes["renewed"] + outcomes["removed"] == 20


def test_renewed_expiry_survives_restart(staging_dir: Path):
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=0.5)
    manager1 = UploadManager(config)
    session, _, chunks, _ = _new_session(manager1)
    manager1.put_chunk(session.session_id, "alice", 0, chunks[0],
                       hashlib.sha256(chunks[0]).hexdigest())
    renewed = manager1.renew_session(session.session_id, "alice", extend_seconds=3600)

    # 重启：新实例看到的有效期必须是延长后的值，已确认进度一致
    manager2 = UploadManager(config)
    manager2.recover()
    progress = manager2.get_progress(session.session_id, "alice")
    assert progress["expires_at"] == pytest.approx(renewed.expires_at)
    assert progress["received_chunks"] == [0]
    # 原 TTL 早已过去，但延长后的有效期内清理不得回收
    time.sleep(0.6)
    assert manager2.cleanup_expired() == []


def test_renew_rejects_terminal_and_foreign_sessions(staging_dir: Path):
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=3600)
    manager = UploadManager(config)
    session, _, chunks, _ = _new_session(manager)
    # 越权
    with pytest.raises(errors.UnauthorizedSession):
        manager.renew_session(session.session_id, "mallory")
    # 已完成
    for i, part in enumerate(chunks):
        manager.put_chunk(session.session_id, "alice", i, part,
                          hashlib.sha256(part).hexdigest())
    manager.commit(session.session_id, "alice")
    with pytest.raises(errors.SessionAlreadyFinalized):
        manager.renew_session(session.session_id, "alice")
    # 不存在
    with pytest.raises(errors.SessionNotFound):
        manager.renew_session("nope", "alice")

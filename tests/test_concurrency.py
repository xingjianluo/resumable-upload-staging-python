"""并发写入与并发提交一致性测试。

验证点：
- 同一会话并发上传不同分片：互不覆盖、互不丢失；
- 同一会话并发上传同一分片（内容一致）：全部幂等成功，最终只有一份数据；
- 不同会话互不干扰；
- 并发提交只产生一份结果；
- 提交与过期清理并发时最终状态确定（要么完成、要么被清理）。
"""

from __future__ import annotations

import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import SessionExpired, SessionNotFound
from app.models import CreateSessionRequest, SESSION_COMPLETED

logger = logging.getLogger("tests.concurrency")

CHUNK_SIZE = 8
NUM_CHUNKS = 32


def _make_session(manager, num_chunks: int = NUM_CHUNKS):
    chunks = [
        bytes([(i * 37 + j) % 256 for j in range(CHUNK_SIZE)])
        for i in range(num_chunks)
    ]
    req = CreateSessionRequest(
        filename="concurrent.bin",
        total_size=num_chunks * CHUNK_SIZE,
        chunk_size=CHUNK_SIZE,
        chunk_checksums=[hashlib.sha256(c).hexdigest() for c in chunks],
        final_checksum=hashlib.sha256(b"".join(chunks)).hexdigest(),
        ttl_seconds=3600,
    )
    state = manager.create_session(req)
    return state, chunks


def test_concurrent_uploads_no_loss_no_overwrite(manager):
    state, chunks = _make_session(manager)
    logger.info("会话=%s 用例=并发写入 %d 分片", state.session_id, NUM_CHUNKS)

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(
            pool.map(
                lambda i: manager.upload_chunk(
                    state.session_id, i, chunks[i], state.owner_token
                ),
                range(NUM_CHUNKS),
            )
        )

    assert all(r.duplicate is False for r in results)
    state_after = manager.get_session(state.session_id)
    assert state_after.received_indexes == list(range(NUM_CHUNKS))
    for i, chunk in enumerate(chunks):
        assert manager.store.read_chunk(state.session_id, i) == chunk
    logger.info(
        "会话=%s 判定=并发写入无丢失无覆盖 依据=已确认%d/%d且磁盘逐片比对一致",
        state.session_id,
        len(state_after.chunks),
        NUM_CHUNKS,
    )

    commit = manager.commit_session(state.session_id, state.owner_token)
    assert open(commit.result_path, "rb").read() == b"".join(chunks)


def test_concurrent_duplicate_uploads_idempotent(manager):
    state, chunks = _make_session(manager, num_chunks=4)

    def upload(index: int):
        return manager.upload_chunk(
            state.session_id, index, chunks[index], state.owner_token
        )

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(upload, [0] * 8 + [1] * 8 + [2] * 8 + [3] * 8))

    fresh = [r for r in results if not r.duplicate]
    dups = [r for r in results if r.duplicate]
    assert len(fresh) == 4 and len(dups) == 28
    assert {r.checksum for r in results} == {
        hashlib.sha256(c).hexdigest() for c in chunks
    }
    logger.info(
        "会话=%s 判定=并发重复提交幂等 依据=首次4次落盘其余28次命中already_committed",
        state.session_id,
    )


def test_sessions_are_isolated(manager):
    state_a, chunks_a = _make_session(manager, num_chunks=2)
    state_b, chunks_b = _make_session(manager, num_chunks=2)
    assert state_a.session_id != state_b.session_id

    manager.upload_chunk(state_a.session_id, 0, chunks_a[0], state_a.owner_token)
    manager.upload_chunk(state_b.session_id, 0, chunks_b[0], state_b.owner_token)

    assert manager.store.read_chunk(state_a.session_id, 0) == chunks_a[0]
    assert manager.store.read_chunk(state_b.session_id, 0) == chunks_b[0]
    logger.info(
        "会话A=%s 会话B=%s 判定=隔离 依据=同序号分片内容各自独立",
        state_a.session_id,
        state_b.session_id,
    )


def test_concurrent_commits_single_result(manager):
    state, chunks = _make_session(manager, num_chunks=4)
    for i, chunk in enumerate(chunks):
        manager.upload_chunk(state.session_id, i, chunk, state.owner_token)

    with ThreadPoolExecutor(max_workers=8) as pool:
        commits = list(
            pool.map(
                lambda _: manager.commit_session(state.session_id, state.owner_token),
                range(8),
            )
        )

    paths = {c.result_path for c in commits}
    assert len(paths) == 1, f"并发提交产生了多个结果: {paths}"
    assert all(c.status == SESSION_COMPLETED for c in commits)
    reasons = {c.reason for c in commits}
    assert reasons <= {"completed", "already_completed_same_result"}
    logger.info(
        "会话=%s 判定=并发提交仅一份结果 依据=result_path唯一 reasons=%s",
        state.session_id,
        reasons,
    )


def test_commit_vs_cleanup_race_is_deterministic(timed_manager):
    manager, clock = timed_manager
    state, chunks = _make_session(manager, num_chunks=2)
    for i, chunk in enumerate(chunks):
        manager.upload_chunk(state.session_id, i, chunk, state.owner_token)

    # 会话过期：提交与清理竞争，两者共享会话锁，结果必为二者之一
    clock.advance(3601)
    outcomes = {"commit": None, "cleanup": None}

    def do_commit():
        try:
            outcomes["commit"] = manager.commit_session(
                state.session_id, state.owner_token
            )
        except (SessionExpired, SessionNotFound) as exc:
            outcomes["commit"] = exc

    def do_cleanup():
        outcomes["cleanup"] = manager.cleanup_expired(now=clock.now)

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(do_commit)
        f2 = pool.submit(do_cleanup)
        f1.result()
        f2.result()

    commit_result = outcomes["commit"]
    removed = outcomes["cleanup"]
    if isinstance(commit_result, Exception):
        # 清理赢：会话被删除，提交失败原因可区分
        assert state.session_id in removed
        assert isinstance(commit_result, (SessionExpired, SessionNotFound))
        logger.info(
            "会话=%s 判定=清理获胜 依据=提交得到%s",
            state.session_id,
            type(commit_result).__name__,
        )
    else:
        # 提交赢：会话已 completed，清理必须跳过（completed 保留）
        assert commit_result.status == SESSION_COMPLETED
        assert state.session_id not in removed
        assert manager.get_session(state.session_id).status == SESSION_COMPLETED
        logger.info(
            "会话=%s 判定=提交获胜 依据=状态completed且清理跳过", state.session_id
        )


def test_cleanup_removes_expired_keeps_completed(timed_manager):
    manager, clock = timed_manager
    expired_state, _ = _make_session(manager, num_chunks=2)
    done_state, done_chunks = _make_session(manager, num_chunks=2)
    for i, chunk in enumerate(done_chunks):
        manager.upload_chunk(done_state.session_id, i, chunk, done_state.owner_token)
    manager.commit_session(done_state.session_id, done_state.owner_token)

    clock.advance(3601)
    removed = manager.cleanup_expired(now=clock.now)

    assert expired_state.session_id in removed
    assert done_state.session_id not in removed
    with pytest.raises(SessionNotFound):
        manager.get_session(expired_state.session_id)
    assert manager.get_session(done_state.session_id).status == SESSION_COMPLETED
    logger.info(
        "过期会话=%s 判定=已清理；完成会话=%s 判定=保留 依据=cleanup_expired规则",
        expired_state.session_id,
        done_state.session_id,
    )

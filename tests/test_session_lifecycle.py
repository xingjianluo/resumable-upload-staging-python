"""会话生命周期与完整性校验单测。

日志逐条打印 会话标识 / 分片序号 / 判定依据（见 app.session 日志输出）。
"""

from __future__ import annotations

import hashlib
import logging

import pytest

from app.errors import (
    ChecksumMismatch,
    ChunkIndexOutOfRange,
    ChunkSizeMismatch,
    FinalChecksumMismatch,
    MissingChunks,
    SessionExpired,
    SessionNotOpen,
    UnauthorizedSession,
)
from app.models import (
    CreateSessionRequest,
    SESSION_COMPLETED,
)

logger = logging.getLogger("tests.lifecycle")

CHUNK_SIZE = 4


def _req(payload: bytes, *, ttl: int | None = 3600, with_final: bool = True):
    checksums = [
        hashlib.sha256(c).hexdigest()
        for c in [payload[i : i + CHUNK_SIZE] for i in range(0, len(payload), CHUNK_SIZE)]
    ]
    return CreateSessionRequest(
        filename="big.bin",
        total_size=len(payload),
        chunk_size=CHUNK_SIZE,
        chunk_checksums=checksums,
        final_checksum=hashlib.sha256(payload).hexdigest() if with_final else None,
        ttl_seconds=ttl,
    )


def test_full_lifecycle_commit_success(manager):
    payload = b"abcdefghijk"  # 11 字节 -> 3 片 [abcd efgh ijk]
    state = manager.create_session(_req(payload))
    logger.info("会话=%s 用例=完整生命周期 判定依据=创建成功", state.session_id)

    for index, chunk in enumerate(
        [payload[i : i + CHUNK_SIZE] for i in range(0, len(payload), CHUNK_SIZE)]
    ):
        result = manager.upload_chunk(state.session_id, index, chunk, state.owner_token)
        assert result.duplicate is False
        logger.info(
            "会话=%s 分片=%d 判定依据=%s",
            state.session_id,
            index,
            result.reason,
        )

    commit = manager.commit_session(state.session_id, state.owner_token)
    assert commit.status == SESSION_COMPLETED
    assert open(commit.result_path, "rb").read() == payload
    logger.info(
        "会话=%s 判定=提交成功 result=%s 依据=分片齐全且整文件校验一致",
        state.session_id,
        commit.result_path,
    )


def test_duplicate_chunk_is_idempotent(manager):
    payload = b"abcdefgh"  # 2 片
    state = manager.create_session(_req(payload))
    first = manager.upload_chunk(state.session_id, 0, b"abcd", state.owner_token)
    second = manager.upload_chunk(state.session_id, 0, b"abcd", state.owner_token)
    assert first.duplicate is False and second.duplicate is True
    assert first.checksum == second.checksum
    logger.info(
        "会话=%s 分片=0 判定=重复提交幂等 依据=%s",
        state.session_id,
        second.reason,
    )


def test_duplicate_chunk_with_conflicting_content_rejected(manager):
    payload = b"abcdefgh"
    state = manager.create_session(_req(payload))
    manager.upload_chunk(state.session_id, 0, b"abcd", state.owner_token)
    with pytest.raises(ChecksumMismatch):
        manager.upload_chunk(state.session_id, 0, b"xbcd", state.owner_token)
    logger.info("会话=%s 分片=0 判定=冲突重传被拒 依据=checksum_mismatch", state.session_id)


def test_declared_checksum_must_match_actual(manager):
    payload = b"abcdefgh"
    state = manager.create_session(_req(payload))
    with pytest.raises(ChecksumMismatch):
        manager.upload_chunk(
            state.session_id,
            0,
            b"abcd",
            state.owner_token,
            checksum="deadbeef",
        )
    logger.info("会话=%s 分片=0 判定=拒绝 依据=声明校验和与实算不一致", state.session_id)


def test_chunk_index_out_of_range(manager):
    payload = b"abcdefgh"
    state = manager.create_session(_req(payload))
    with pytest.raises(ChunkIndexOutOfRange):
        manager.upload_chunk(state.session_id, 5, b"abcd", state.owner_token)
    logger.info("会话=%s 分片=5 判定=拒绝 依据=chunk_index_out_of_range", state.session_id)


def test_chunk_size_mismatch_rejected(manager):
    payload = b"abcdefghijk"  # 最后一片应为 3 字节
    state = manager.create_session(_req(payload))
    with pytest.raises(ChunkSizeMismatch):
        manager.upload_chunk(state.session_id, 2, b"ij", state.owner_token)
    logger.info("会话=%s 分片=2 判定=拒绝 依据=chunk_size_mismatch", state.session_id)


def test_commit_with_missing_chunks_fails_distinguishably(manager):
    payload = b"abcdefghijk"
    state = manager.create_session(_req(payload))
    manager.upload_chunk(state.session_id, 0, b"abcd", state.owner_token)
    manager.upload_chunk(state.session_id, 2, b"ijk", state.owner_token)
    with pytest.raises(MissingChunks) as exc:
        manager.commit_session(state.session_id, state.owner_token)
    assert "1" in str(exc.value)
    # 缺片失败后会话仍 open，补齐后可以成功
    manager.upload_chunk(state.session_id, 1, b"efgh", state.owner_token)
    commit = manager.commit_session(state.session_id, state.owner_token)
    assert commit.status == SESSION_COMPLETED
    logger.info(
        "会话=%s 判定=补齐后提交成功 依据=missing_chunks为可恢复失败", state.session_id
    )


def test_final_checksum_mismatch_rejected(manager):
    payload = b"abcdefghijk"
    req = _req(payload)
    req.final_checksum = hashlib.sha256(b"totally-different").hexdigest()
    state = manager.create_session(req)
    for index, chunk in enumerate(
        [payload[i : i + CHUNK_SIZE] for i in range(0, len(payload), CHUNK_SIZE)]
    ):
        manager.upload_chunk(state.session_id, index, chunk, state.owner_token)
    with pytest.raises(FinalChecksumMismatch):
        manager.commit_session(state.session_id, state.owner_token)
    logger.info("会话=%s 判定=提交拒绝 依据=final_checksum_mismatch", state.session_id)


def test_unauthorized_writes_rejected(manager):
    payload = b"abcdefgh"
    state = manager.create_session(_req(payload))
    with pytest.raises(UnauthorizedSession):
        manager.upload_chunk(state.session_id, 0, b"abcd", "wrong-token")
    with pytest.raises(UnauthorizedSession):
        manager.commit_session(state.session_id, "wrong-token")
    logger.info("会话=%s 判定=越权请求被拒 依据=unauthorized", state.session_id)


def test_expired_session_rejected_with_distinct_reason(timed_manager):
    manager, clock = timed_manager
    payload = b"abcdefgh"
    state = manager.create_session(_req(payload, ttl=10))
    clock.advance(11)
    with pytest.raises(SessionExpired):
        manager.upload_chunk(state.session_id, 0, b"abcd", state.owner_token)
    with pytest.raises(SessionExpired):
        manager.commit_session(state.session_id, state.owner_token)
    logger.info("会话=%s 判定=过期写入/提交被拒 依据=session_expired", state.session_id)


def test_write_after_commit_rejected(manager):
    payload = b"abcdefgh"
    state = manager.create_session(_req(payload))
    for index, chunk in enumerate(
        [payload[i : i + CHUNK_SIZE] for i in range(0, len(payload), CHUNK_SIZE)]
    ):
        manager.upload_chunk(state.session_id, index, chunk, state.owner_token)
    manager.commit_session(state.session_id, state.owner_token)
    with pytest.raises(SessionNotOpen):
        manager.upload_chunk(state.session_id, 0, b"abcd", state.owner_token)
    logger.info("会话=%s 分片=0 判定=完成后写入被拒 依据=session_not_open", state.session_id)

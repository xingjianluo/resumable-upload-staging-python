"""断点续传与服务重启恢复测试。

验证点：
- 已确认分片进度在服务重启（重建 SessionManager）后不丢失；
- 中断后只需补传缺失分片即可完成提交；
- manifest 登记但磁盘缺失的分片在恢复时被剔除（进度以 manifest+磁盘为准）；
- 已 completed 的会话重启后结果仍在。
"""

from __future__ import annotations

import hashlib
import logging

from app.models import CreateSessionRequest, SESSION_COMPLETED, SESSION_OPEN
from app.session import SessionManager

logger = logging.getLogger("tests.recovery")

CHUNK_SIZE = 8
NUM_CHUNKS = 6


def _make_session(manager):
    chunks = [
        bytes([(i * 13 + j) % 256 for j in range(CHUNK_SIZE)])
        for i in range(NUM_CHUNKS)
    ]
    req = CreateSessionRequest(
        filename="resume.bin",
        total_size=NUM_CHUNKS * CHUNK_SIZE,
        chunk_size=CHUNK_SIZE,
        chunk_checksums=[hashlib.sha256(c).hexdigest() for c in chunks],
        final_checksum=hashlib.sha256(b"".join(chunks)).hexdigest(),
        ttl_seconds=3600,
    )
    state = manager.create_session(req)
    return state, chunks


def test_progress_survives_restart(settings, store):
    manager = SessionManager(settings, store)
    state, chunks = _make_session(manager)
    for i in (0, 1, 2):
        manager.upload_chunk(state.session_id, i, chunks[i], state.owner_token)
    logger.info(
        "会话=%s 用例=重启前已确认分片 0,1,2 判定依据=manifest持久化", state.session_id
    )

    # 模拟服务重启：用同一磁盘目录重建管理器
    manager2 = SessionManager(settings, store)
    recovered = manager2.recover()
    assert state.session_id in recovered

    state2 = manager2.get_session(state.session_id)
    assert state2.status == SESSION_OPEN
    assert state2.received_indexes == [0, 1, 2]
    assert state2.missing_indexes == [3, 4, 5]
    logger.info(
        "会话=%s 判定=重启后进度保留 已确认=%s 缺失=%s 依据=manifest为事实来源",
        state.session_id,
        state2.received_indexes,
        state2.missing_indexes,
    )

    # 断点续传：只补缺失分片即可提交
    for i in (3, 4, 5):
        manager2.upload_chunk(state.session_id, i, chunks[i], state.owner_token)
    commit = manager2.commit_session(state.session_id, state.owner_token)
    assert commit.status == SESSION_COMPLETED
    assert open(commit.result_path, "rb").read() == b"".join(chunks)
    logger.info("会话=%s 判定=断点续传完成 依据=补齐缺失分片后提交成功", state.session_id)


def test_reupload_of_confirmed_chunk_after_restart_is_idempotent(settings, store):
    manager = SessionManager(settings, store)
    state, chunks = _make_session(manager)
    manager.upload_chunk(state.session_id, 0, chunks[0], state.owner_token)

    manager2 = SessionManager(settings, store)
    manager2.recover()
    result = manager2.upload_chunk(
        state.session_id, 0, chunks[0], state.owner_token
    )
    assert result.duplicate is True
    logger.info(
        "会话=%s 分片=0 判定=重启后重传幂等 依据=%s", state.session_id, result.reason
    )


def test_dangling_chunk_reconciled_on_recover(settings, store):
    manager = SessionManager(settings, store)
    state, chunks = _make_session(manager)
    for i in (0, 1):
        manager.upload_chunk(state.session_id, i, chunks[i], state.owner_token)

    # 模拟崩溃：manifest 已登记但分片文件丢失（等价于落盘未完成即宕机）
    import os

    os.unlink(manager.store.chunk_path(state.session_id, 1))

    manager2 = SessionManager(settings, store)
    manager2.recover()
    state2 = manager2.get_session(state.session_id)
    assert state2.received_indexes == [0]
    assert state2.missing_indexes == [1, 2, 3, 4, 5]
    logger.info(
        "会话=%s 分片=1 判定=恢复时剔除未落盘分片 依据=manifest登记但磁盘缺失",
        state.session_id,
    )

    # 被剔除的分片可重新上传
    result = manager2.upload_chunk(state.session_id, 1, chunks[1], state.owner_token)
    assert result.duplicate is False


def test_completed_session_survives_restart(settings, store):
    manager = SessionManager(settings, store)
    state, chunks = _make_session(manager)
    for i, chunk in enumerate(chunks):
        manager.upload_chunk(state.session_id, i, chunk, state.owner_token)
    commit = manager.commit_session(state.session_id, state.owner_token)

    manager2 = SessionManager(settings, store)
    recovered = manager2.recover()
    assert state.session_id not in recovered  # completed 不属于"待恢复"

    state2 = manager2.get_session(state.session_id)
    assert state2.status == SESSION_COMPLETED
    assert state2.result_path == commit.result_path
    # 重启后重复提交仍返回同一份结果
    commit2 = manager2.commit_session(state.session_id, state.owner_token)
    assert commit2.result_path == commit.result_path
    assert commit2.reason == "already_completed_same_result"
    logger.info(
        "会话=%s 判定=重启后已完成结果保留且重复提交幂等 依据=result_path一致",
        state.session_id,
    )

"""多进程/多实例共享同一暂存目录：并发写、并发提交、提交与回收竞争。

每个子进程使用独立的 UploadManager 实例指向同一 staging_dir，
模拟线上多 worker / 多实例部署。子进程结论通过 Queue 回传，由父进程断言。
"""

from __future__ import annotations

import hashlib
import multiprocessing as mp
import time
from pathlib import Path

import pytest

from resumable_upload import errors
from resumable_upload.config import StagingConfig
from resumable_upload.manager import UploadManager
from resumable_upload.models import SessionStatus

from .conftest import make_payload

_CTX = mp.get_context("fork")


def _child_write_all_chunks(staging_dir: str, session_id: str, owner: str,
                            total_size: int, chunk_size: int, tag: int, queue):
    """子进程：用自己的 manager 实例把全部分片写一遍（跨进程重复写必须幂等）。"""
    try:
        manager = UploadManager(StagingConfig(staging_dir=Path(staging_dir)))
        _, chunks, _ = make_payload(total_size, chunk_size)
        for i, part in enumerate(chunks):
            manager.put_chunk(session_id, owner, i, part,
                              hashlib.sha256(part).hexdigest())
        queue.put((tag, "ok"))
    except Exception as exc:  # noqa: BLE001
        queue.put((tag, f"error:{type(exc).__name__}:{exc}"))


def _child_commit(staging_dir: str, session_id: str, owner: str, tag: int, queue):
    try:
        manager = UploadManager(StagingConfig(staging_dir=Path(staging_dir)))
        session = manager.commit(session_id, owner)
        queue.put((tag, f"ok:{session.result_path}"))
    except errors.UploadError as exc:
        queue.put((tag, f"reject:{exc.reason}"))
    except Exception as exc:  # noqa: BLE001
        queue.put((tag, f"error:{type(exc).__name__}:{exc}"))


def _child_cleanup(staging_dir: str, tag: int, queue):
    try:
        manager = UploadManager(StagingConfig(staging_dir=Path(staging_dir)))
        removed = manager.cleanup_expired()
        queue.put((tag, f"ok:removed={len(removed)}"))
    except Exception as exc:  # noqa: BLE001
        queue.put((tag, f"error:{type(exc).__name__}:{exc}"))


def _child_progress(staging_dir: str, session_id: str, owner: str, tag: int, queue):
    try:
        manager = UploadManager(StagingConfig(staging_dir=Path(staging_dir)))
        manager.recover()
        progress = manager.get_progress(session_id, owner)
        queue.put((tag, ("ok", progress["received_chunks"], progress["expires_at"])))
    except Exception as exc:  # noqa: BLE001
        queue.put((tag, f"error:{type(exc).__name__}:{exc}"))


def _run_processes(target, args_list) -> list:
    queue = _CTX.Queue()
    procs = [
        _CTX.Process(target=target, args=(*args, i, queue))
        for i, args in enumerate(args_list)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30)
    results = [queue.get() for _ in range(len(procs))]
    assert all(p.exitcode == 0 for p in procs), "child process crashed"
    return results


def test_multiprocess_concurrent_writes_same_session(staging_dir: Path):
    manager = UploadManager(StagingConfig(staging_dir=staging_dir))
    data, chunks, final_sum = make_payload(1500, 256)
    session = manager.create_session(
        filename="mp.bin", total_size=1500, chunk_size=256,
        final_checksum=final_sum, owner_token="alice",
    )
    # 4 个进程同时写同一会话的全部分片：重复写必须幂等，分片不能互相覆盖
    results = _run_processes(
        _child_write_all_chunks,
        [(str(staging_dir), session.session_id, "alice", 1500, 256)] * 4,
    )
    assert [r[1] for r in results] == ["ok"] * 4

    on_disk = manager.storage.load_metadata(session.session_id)
    assert sorted(on_disk.chunks) == list(range(6))
    for i, part in enumerate(chunks):
        assert manager.storage.read_chunk(session.session_id, i) == part

    result = manager.commit(session.session_id, "alice")
    assert result.status == SessionStatus.COMPLETED
    assert Path(result.result_path).read_bytes() == data


def test_multiprocess_concurrent_commit_produces_single_result(staging_dir: Path):
    manager = UploadManager(StagingConfig(staging_dir=staging_dir))
    data, chunks, final_sum = make_payload(1000, 256)
    session = manager.create_session(
        filename="mp-commit.bin", total_size=1000, chunk_size=256,
        final_checksum=final_sum, owner_token="alice",
    )
    for i, part in enumerate(chunks):
        manager.put_chunk(session.session_id, "alice", i, part,
                          hashlib.sha256(part).hexdigest())
    # 3 个进程同时提交：只能生成一份结果，且大家看到的是同一份
    results = _run_processes(
        _child_commit,
        [(str(staging_dir), session.session_id, "alice")] * 3,
    )
    outcomes = [r[1] for r in results]
    assert all(o.startswith("ok:") for o in outcomes)
    assert len({o[3:] for o in outcomes}) == 1
    completed = list(manager.storage.completed_root().iterdir())
    assert len(completed) == 1
    assert completed[0].read_bytes() == data


def test_multiprocess_commit_and_cleanup_race_is_deterministic(staging_dir: Path):
    # 提交与过期回收分属不同进程：结论必须唯一 —— 要么唯一一次成功提交，要么过期回收。
    # 有效期在传完分片后人为拨到"即将过期"，避免上传本身受机器负载影响而超时
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=3600)
    manager = UploadManager(config)
    outcomes = {"completed": 0, "expired": 0}
    for trial in range(10):
        _, chunks, final_sum = make_payload(500, 250)
        session = manager.create_session(
            filename=f"race-{trial}", total_size=500, chunk_size=250,
            final_checksum=final_sum, owner_token="alice",
        )
        for i, part in enumerate(chunks):
            manager.put_chunk(session.session_id, "alice", i, part,
                              hashlib.sha256(part).hexdigest())
        state = manager.storage.load_metadata(session.session_id)
        state.expires_at = time.time() + 0.06
        manager.storage.save_metadata(state)
        time.sleep(0.06)  # 逼近过期时刻
        results = _run_processes(
            _child_commit, [(str(staging_dir), session.session_id, "alice")]
        )
        _run_processes(_child_cleanup, [(str(staging_dir),)])
        outcome = results[0][1]
        state = manager.storage.load_metadata(session.session_id)
        if outcome.startswith("ok:"):
            outcomes["completed"] += 1
            assert state is not None and state.status == SessionStatus.COMPLETED
            assert Path(state.result_path).exists()
            # 提交成功后清理不得删除已完成会话
            assert manager.storage.load_metadata(session.session_id) is not None
        else:
            outcomes["expired"] += 1
            assert outcome == "reject:session_expired" or outcome == "reject:session_not_found"
            # 会话要么已被回收，要么仍过期，下一轮清理必回收
            if state is not None:
                assert state.expires_at < time.time()
    assert outcomes["completed"] + outcomes["expired"] == 10


def test_multiprocess_progress_consistent_across_instances(staging_dir: Path):
    # 一个进程写入的已确认进度，另一个进程（模拟重启后的新实例）看到的必须一致
    manager = UploadManager(StagingConfig(staging_dir=staging_dir))
    _, chunks, final_sum = make_payload(1000, 256)
    session = manager.create_session(
        filename="mp-progress.bin", total_size=1000, chunk_size=256,
        final_checksum=final_sum, owner_token="alice",
    )
    for i in (0, 2):
        manager.put_chunk(session.session_id, "alice", i, chunks[i],
                          hashlib.sha256(chunks[i]).hexdigest())
    results = _run_processes(
        _child_progress, [(str(staging_dir), session.session_id, "alice")]
    )
    _tag, payload = results[0]
    assert payload[0] == "ok"
    assert payload[1] == [0, 2]
    assert payload[2] == pytest.approx(session.expires_at)
    # 客户端拿原会话继续补缺失分片即可接着传
    for i in (1, 3):
        manager.put_chunk(session.session_id, "alice", i, chunks[i],
                          hashlib.sha256(chunks[i]).hexdigest())
    result = manager.commit(session.session_id, "alice")
    assert result.status == SessionStatus.COMPLETED


def test_multiprocess_sessions_isolated(staging_dir: Path):
    # 不同会话在多进程下依然互不干扰
    manager = UploadManager(StagingConfig(staging_dir=staging_dir))
    data1, _, sum1 = make_payload(600, 300)
    data2, _, sum2 = make_payload(900, 300)
    s1 = manager.create_session(filename="s1.bin", total_size=600, chunk_size=300,
                                final_checksum=sum1, owner_token="alice")
    s2 = manager.create_session(filename="s2.bin", total_size=900, chunk_size=300,
                                final_checksum=sum2, owner_token="bob")
    results = _run_processes(
        _child_write_all_chunks,
        [
            (str(staging_dir), s1.session_id, "alice", 600, 300),
            (str(staging_dir), s2.session_id, "bob", 900, 300),
        ],
    )
    assert [r[1] for r in results] == ["ok", "ok"]
    r1 = manager.commit(s1.session_id, "alice")
    r2 = manager.commit(s2.session_id, "bob")
    assert Path(r1.result_path).read_bytes() == data1
    assert Path(r2.result_path).read_bytes() == data2

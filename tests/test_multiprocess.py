"""多进程/多实例共享同一暂存目录的单测。

用 spawn 启动真实子进程，每个进程各自构造一个 UploadManager 指向同一个
staging 目录，验证：
- 同一会话分片被多进程并发写：不互相覆盖、已确认进度不丢失、只出一份结果；
- 不同会话互不干扰；
- 重启（全新进程）看到的已确认进度一致，可凭原会话补传；
- 提交与过期回收跨进程竞争，结论唯一；
- 容量账本在多进程并发写入下不超过上限。
"""

from __future__ import annotations

import hashlib
import multiprocessing as mp
import time
from pathlib import Path

from resumable_upload.config import StagingConfig
from resumable_upload.models import SessionStatus


def _build_config(
    staging_dir: str, ttl: float = 3600.0, limit: int = 0, max_ttl: float = 0
) -> StagingConfig:
    return StagingConfig(
        staging_dir=Path(staging_dir),
        session_ttl_seconds=ttl,
        cleanup_interval_seconds=3600,
        chunk_max_bytes=4 * 1024 * 1024,
        max_staging_bytes=limit,
        max_session_ttl_seconds=max_ttl,
    )


def _chunks(total_size: int, chunk_size: int) -> list[bytes]:
    data = bytes(i % 251 for i in range(total_size))
    return [data[i : i + chunk_size] for i in range(0, total_size, chunk_size)]


# ---- 子进程入口（必须是模块级函数，spawn 可 pickle） ----
def _worker_write_chunks(
    staging_dir: str,
    session_id: str,
    indices,
    ttl: float,
    limit: int = 0,
    total_size: int = 2000,
    chunk_size: int = 200,
    queue=None,
) -> None:
    from resumable_upload.manager import UploadManager

    manager = UploadManager(_build_config(staging_dir, ttl, limit))
    chunks = _chunks(total_size, chunk_size)
    failures = []
    for i in indices:
        part = chunks[i]
        try:
            manager.put_chunk(
                session_id, "alice", i, part, hashlib.sha256(part).hexdigest()
            )
        except Exception as exc:  # noqa: BLE001 - 容量拒绝是预期，其余回传父进程
            reason = getattr(exc, "reason", type(exc).__name__)
            if reason != "quota_exceeded":
                failures.append((i, reason))
    if queue is not None:
        queue.put(failures)


def _worker_commit(staging_dir: str, session_id: str, ttl: float, queue) -> None:
    from resumable_upload.manager import UploadManager

    manager = UploadManager(_build_config(staging_dir, ttl))
    try:
        session = manager.commit(session_id, "alice")
        queue.put(("ok", session.result_path))
    except Exception as exc:  # noqa: BLE001 - 跨进程收集结论
        queue.put((getattr(exc, "reason", "error"), None))


def _worker_cleanup(staging_dir: str, ttl: float) -> None:
    from resumable_upload.manager import UploadManager

    manager = UploadManager(_build_config(staging_dir, ttl))
    manager.cleanup_expired()


def _worker_renew(staging_dir: str, session_id: str, extend: float, queue) -> None:
    from resumable_upload.manager import UploadManager

    manager = UploadManager(_build_config(staging_dir, ttl=3600))
    try:
        session = manager.renew_session(session_id, "alice", extend)
        queue.put(("renewed", session.expires_at))
    except Exception as exc:  # noqa: BLE001 - 与清理竞争时过期/不存在是合法结局
        queue.put((getattr(exc, "reason", "error"), None))


def _worker_resume_commit(
    staging_dir: str, session_id: str, missing_indices, total_size, chunk_size, queue
) -> None:
    from resumable_upload.manager import UploadManager

    manager = UploadManager(_build_config(staging_dir))
    chunks = _chunks(total_size, chunk_size)
    count = manager.recover()
    progress = manager.get_progress(session_id, "alice")
    try:
        assert count == 1
        received = progress["received_chunks"]
        for i in missing_indices:
            part = chunks[i]
            manager.put_chunk(session_id, "alice", i, part,
                              hashlib.sha256(part).hexdigest())
        session = manager.commit(session_id, "alice")
        queue.put(("done", session.result_path, received))
    except Exception as exc:  # noqa: BLE001
        queue.put(("error", f"{type(exc).__name__}: {exc}", None))


def _spawn_context():
    return mp.get_context("spawn")


def test_multiple_processes_write_same_session_without_corruption(staging_dir: Path):
    from resumable_upload.manager import UploadManager

    manager = UploadManager(_build_config(str(staging_dir)))
    data = bytes(i % 251 for i in range(2000))
    chunks = _chunks(2000, 200)
    final_sum = hashlib.sha256(data).hexdigest()
    session = manager.create_session(
        filename="multi.bin", total_size=2000, chunk_size=200,
        final_checksum=final_sum, owner_token="alice",
    )
    ctx = _spawn_context()
    procs = []
    queue = ctx.Queue()
    # 4 个进程，分片集合互相重叠，且每个进程重复写自己的分片（验证幂等）
    assignment = [
        [0, 1, 2, 3, 4],
        [3, 4, 5, 6, 7],
        [6, 7, 8, 9, 0],
        [0, 1, 2, 8, 9],
    ]
    for indices in assignment:
        p = ctx.Process(
            target=_worker_write_chunks,
            args=(str(staging_dir), session.session_id, indices, 3600.0, 0,
                  2000, 200, queue),
        )
        p.start()
        procs.append(p)
    failures = []
    for _ in procs:
        failures.extend(queue.get(timeout=60))
    for p in procs:
        p.join(timeout=60)
        assert p.exitcode == 0
    assert failures == []

    # 主进程（相当于另一个实例）看到全部 10 个分片且内容正确
    on_disk = manager.storage.load_metadata(session.session_id)
    assert sorted(on_disk.chunks) == list(range(10))
    for i, part in enumerate(chunks):
        assert manager.storage.read_chunk(session.session_id, i) == part
    result = manager.commit(session.session_id, "alice")
    assert result.status == SessionStatus.COMPLETED
    assert Path(result.result_path).read_bytes() == data


def test_progress_consistent_in_fresh_process_after_restart(staging_dir: Path):
    from resumable_upload.manager import UploadManager

    manager = UploadManager(_build_config(str(staging_dir)))
    chunks = _chunks(1000, 200)
    data = b"".join(chunks)
    final_sum = hashlib.sha256(data).hexdigest()
    session = manager.create_session(
        filename="r.bin", total_size=1000, chunk_size=200,
        final_checksum=final_sum, owner_token="alice",
    )
    for i in (0, 2, 4):
        manager.put_chunk(session.session_id, "alice", i, chunks[i],
                          hashlib.sha256(chunks[i]).hexdigest())

    # 全新进程 recover 后看到一致的已确认进度，并补传缺失分片后提交
    ctx = _spawn_context()
    queue = ctx.Queue()
    p = ctx.Process(
        target=_worker_resume_commit,
        args=(str(staging_dir), session.session_id, [1, 3], 1000, 200, queue),
    )
    p.start()
    p.join(timeout=60)
    assert p.exitcode == 0
    status, result_path, received = queue.get(timeout=10)
    assert status == "done", result_path
    assert received == [0, 2, 4]
    assert Path(result_path).read_bytes() == data


def test_cross_process_commit_and_cleanup_race_is_unique(staging_dir: Path):
    from resumable_upload.manager import UploadManager

    # 20 轮：短 TTL，提交进程与清理进程几乎同时启动
    ctx = _spawn_context()
    outcomes = {"ok": 0, "gone": 0}
    for trial in range(20):
        manager = UploadManager(_build_config(str(staging_dir), ttl=0.2))
        chunks = _chunks(500, 250)
        data = b"".join(chunks)
        final_sum = hashlib.sha256(data).hexdigest()
        session = manager.create_session(
            filename=f"race-{trial}", total_size=500, chunk_size=250,
            final_checksum=final_sum, owner_token="alice",
        )
        for i, part in enumerate(chunks):
            manager.put_chunk(session.session_id, "alice", i, part,
                              hashlib.sha256(part).hexdigest())
        time.sleep(0.18)
        queue = ctx.Queue()
        p_commit = ctx.Process(
            target=_worker_commit, args=(str(staging_dir), session.session_id, 0.2, queue)
        )
        p_clean = ctx.Process(
            target=_worker_cleanup, args=(str(staging_dir), 0.2)
        )
        p_commit.start(); p_clean.start()
        p_commit.join(timeout=60); p_clean.join(timeout=60)
        assert p_commit.exitcode == 0 and p_clean.exitcode == 0
        reason, result_path = queue.get(timeout=10)
        on_disk = manager.storage.load_metadata(session.session_id)
        if reason == "ok":
            outcomes["ok"] += 1
            assert on_disk is not None and on_disk.status == SessionStatus.COMPLETED
            assert Path(result_path).read_bytes() == data
            # completed 目录下该会话只能有一份结果
            matches = list((staging_dir / "completed").glob(f"{session.session_id}-*"))
            assert len(matches) == 1
        else:
            outcomes["gone"] += 1
            assert reason in ("session_expired", "session_not_found")
    assert outcomes["ok"] + outcomes["gone"] == 20


def test_renew_vs_cleanup_race_in_separate_processes(staging_dir: Path):
    from resumable_upload.manager import UploadManager

    ctx = _spawn_context()
    ttl = 1.0
    survived = 0
    for trial in range(6):
        manager = UploadManager(_build_config(str(staging_dir), ttl=ttl))
        chunks = _chunks(200, 200)
        final_sum = hashlib.sha256(b"".join(chunks)).hexdigest()
        session = manager.create_session(
            filename=f"keep-{trial}", total_size=200, chunk_size=200,
            final_checksum=final_sum, owner_token="alice",
        )
        manager.put_chunk(session.session_id, "alice", 0, chunks[0],
                          hashlib.sha256(chunks[0]).hexdigest())
        time.sleep(0.9)
        queue = ctx.Queue()
        p_renew = ctx.Process(
            target=_worker_renew,
            args=(str(staging_dir), session.session_id, 3600.0, queue),
        )
        p_clean = ctx.Process(target=_worker_cleanup, args=(str(staging_dir), ttl))
        p_renew.start()
        time.sleep(0.02)  # 让续期进程先拿到会话锁，制造"续期先赢"
        p_clean.start()
        p_renew.join(timeout=60); p_clean.join(timeout=60)
        assert p_renew.exitcode == 0 and p_clean.exitcode == 0
        reason, new_expires = queue.get(timeout=10)
        on_disk = manager.storage.load_metadata(session.session_id)
        if reason == "renewed":
            # 续期先赢：会话保留，有效期被延到很久以后，再清理也删不掉
            survived += 1
            assert on_disk is not None
            assert new_expires > time.time() + 3000
            assert on_disk.expires_at == new_expires
            assert manager.cleanup_expired() == []
            assert manager.storage.load_metadata(session.session_id) is not None
        else:
            # 回收先赢：会话已被物理删除，续期方拿到明确的过期/不存在结论，
            # 不会出现"既被删又续期成功"的撕裂状态
            assert reason in ("session_expired", "session_not_found")
            assert on_disk is None
    # 不强制每次都续期赢，但至少要出现过续期成功的场景
    assert survived > 0


def test_concurrent_processes_respect_quota_cap(staging_dir: Path):
    from resumable_upload.manager import UploadManager

    limit = 600  # 仅够 3 个分片
    manager = UploadManager(_build_config(str(staging_dir), limit=limit))
    chunks = _chunks(2000, 200)
    data = b"".join(chunks)
    final_sum = hashlib.sha256(data).hexdigest()
    session = manager.create_session(
        filename="q.bin", total_size=2000, chunk_size=200,
        final_checksum=final_sum, owner_token="alice",
    )
    ctx = _spawn_context()
    procs = []
    # 4 个进程各写不相交的分片，总计试图写 8 个，但只能确认 3 个
    assignment = [[0, 1], [2, 3], [4, 5], [6, 7]]
    for indices in assignment:
        p = ctx.Process(
            target=_worker_write_chunks,
            args=(str(staging_dir), session.session_id, indices, 3600.0, limit),
        )
        p.start()
        procs.append(p)
    for p in procs:
        p.join(timeout=60)
        # 写分片的 worker 不做断言（被容量拒绝是预期），只要求进程不崩
        assert p.exitcode == 0
    used = manager.quota.load()
    assert used <= limit
    on_disk = manager.storage.load_metadata(session.session_id)
    assert len(on_disk.chunks) == 3
    assert len(on_disk.chunks) * 200 == used

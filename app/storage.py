"""本地磁盘存储：分片落盘、manifest 原子读写、跨进程文件锁。

用本地磁盘模拟远端共享存储：
- 每次写入先写同目录临时文件，fsync 后再原子 rename（崩溃时只会看到
  旧版本或新版本，不会出现半截文件）；
- manifest 以 "写临时文件 + fsync 目录项 + rename + fsync 父目录"
  原子替换；
- 每个会话一把独立的 fcntl 文件锁（锁文件放在与会话目录同级、
  不随会话删除的 locks/ 中），串行化同一会话的所有状态变更；
- 不同会话对应不同锁文件，天然并发互不阻塞。
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import json
import os
import shutil
import tempfile
import time
import uuid
from collections.abc import Iterator
from typing import Any


class LockTimeout(OSError):
    """获取会话锁超时。"""


class DiskStore:
    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        self.sessions_root = os.path.join(self.root, "sessions")
        self.completed_root = os.path.join(self.root, "completed")
        self.locks_root = os.path.join(self.root, "locks")
        self.tmp_root = os.path.join(self.root, "tmp")

    # ---- 目录布局 -------------------------------------------------------

    def ensure_layout(self) -> None:
        for path in (
            self.root,
            self.sessions_root,
            self.completed_root,
            self.locks_root,
            self.tmp_root,
        ):
            os.makedirs(path, exist_ok=True)

    def session_dir(self, session_id: str) -> str:
        return os.path.join(self.sessions_root, session_id)

    def chunks_dir(self, session_id: str) -> str:
        return os.path.join(self.session_dir(session_id), "chunks")

    def completed_dir(self, session_id: str) -> str:
        return os.path.join(self.completed_root, session_id)

    def manifest_path(self, session_id: str) -> str:
        return os.path.join(self.session_dir(session_id), "manifest.json")

    def chunk_path(self, session_id: str, index: int) -> str:
        return os.path.join(self.chunks_dir(session_id), f"{index:08d}.part")

    def final_path(self, session_id: str, filename: str) -> str:
        return os.path.join(self.completed_dir(session_id), filename)

    def list_sessions(self) -> list[str]:
        try:
            entries = sorted(os.listdir(self.sessions_root))
        except FileNotFoundError:
            return []
        return [
            name
            for name in entries
            if os.path.isfile(os.path.join(self.sessions_root, name, "manifest.json"))
        ]

    def session_exists(self, session_id: str) -> bool:
        return os.path.isfile(self.manifest_path(session_id))

    # ---- manifest -------------------------------------------------------

    def read_manifest_raw(self, session_id: str) -> dict[str, Any] | None:
        path = self.manifest_path(session_id)
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except FileNotFoundError:
            return None

    def write_manifest_raw(
        self, session_id: str, data: dict[str, Any], fsync: bool = True
    ) -> None:
        session_dir = self.session_dir(session_id)
        os.makedirs(session_dir, exist_ok=True)
        payload = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
        self._atomic_write_bytes(
            self.manifest_path(session_id), payload.encode("utf-8"), fsync
        )

    # ---- 分片与成品 -----------------------------------------------------

    def write_chunk_atomic(
        self,
        session_id: str,
        index: int,
        payload: bytes,
        fsync: bool = True,
        retry_attempts: int = 1,
        retry_backoff: float = 0.0,
    ) -> None:
        target = self.chunk_path(session_id, index)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        last_error: OSError | None = None
        for attempt in range(1, max(retry_attempts, 1) + 1):
            try:
                self._atomic_write_bytes(target, payload, fsync)
                return
            except OSError as exc:
                last_error = exc
                if attempt >= retry_attempts or exc.errno not in (
                    errno.EINTR,
                    errno.EAGAIN,
                    errno.ENOSPC,
                ):
                    raise
                time.sleep(retry_backoff * attempt)
        assert last_error is not None
        raise last_error

    def read_chunk(self, session_id: str, index: int) -> bytes:
        with open(self.chunk_path(session_id, index), "rb") as fh:
            return fh.read()

    def chunk_present(self, session_id: str, index: int) -> bool:
        return os.path.isfile(self.chunk_path(session_id, index))

    def assemble_result(
        self, session_id: str, filename: str, indexes: list[int], fsync: bool = True
    ) -> str:
        """按序号顺序拼接分片到 completed/ 下的成品文件（临时文件 + 原子改名）。"""
        target_dir = self.completed_dir(session_id)
        os.makedirs(target_dir, exist_ok=True)
        target = self.final_path(session_id, filename)
        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix=f".{os.path.basename(filename)}.", suffix=".tmp", dir=target_dir
        )
        try:
            with os.fdopen(tmp_fd, "wb") as out:
                for index in indexes:
                    with open(self.chunk_path(session_id, index), "rb") as src:
                        shutil.copyfileobj(src, out, length=1024 * 1024)
                out.flush()
                if fsync:
                    os.fsync(out.fileno())
            os.replace(tmp_name, target)
            if fsync:
                self._fsync_parent(target_dir)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_name)
            raise
        return target

    def remove_session(self, session_id: str) -> None:
        shutil.rmtree(self.session_dir(session_id), ignore_errors=True)

    def remove_completed(self, session_id: str) -> None:
        shutil.rmtree(self.completed_dir(session_id), ignore_errors=True)

    # ---- 锁 -------------------------------------------------------------

    @contextlib.contextmanager
    def session_lock(self, session_id: str, timeout: float) -> Iterator[None]:
        """获取会话排他锁；锁文件独立存放，不随会话目录删除。"""
        os.makedirs(self.locks_root, exist_ok=True)
        lock_path = os.path.join(self.locks_root, f"{session_id}.lock")
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        acquired = False
        deadline = time.monotonic() + timeout
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise LockTimeout(
                            f"lock timeout for session {session_id}"
                        ) from None
                    time.sleep(0.01)
            yield
        finally:
            if acquired:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    # ---- 内部工具 -------------------------------------------------------

    def _atomic_write_bytes(self, path: str, payload: bytes, fsync: bool) -> None:
        directory = os.path.dirname(path)
        os.makedirs(directory, exist_ok=True)
        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix=f".{os.path.basename(path)}.", suffix=".tmp", dir=directory
        )
        try:
            with os.fdopen(tmp_fd, "wb") as fh:
                fh.write(payload)
                fh.flush()
                if fsync:
                    os.fsync(fh.fileno())
            os.replace(tmp_name, path)
            if fsync:
                self._fsync_parent(directory)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_name)
            raise

    def _fsync_parent(self, dir_path: str) -> None:
        fd = os.open(dir_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

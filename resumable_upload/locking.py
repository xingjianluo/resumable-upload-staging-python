"""跨进程/跨实例文件锁（基于 fcntl.flock）与进程内互斥的组合锁。

多个 worker 进程或多个实例指向同一个暂存目录时，仅靠线程锁无法互斥；
这里用每个会话一把 flock 锁文件实现跨进程互斥，锁文件放在共享暂存
目录下（对共享该目录的所有实例可见），同时在进程内叠加一把线程锁，
避免同一进程内多个线程各开一个 fd 去竞争 flock。

锁获取顺序固定为 "进程内锁 -> flock"，两层锁均不重入；manager 层
保证不会在持有同一会话锁时再次获取该会话锁。
"""

from __future__ import annotations

import fcntl
import os
import threading
from pathlib import Path

_LOCKS_DIR = "locks"
_QUOTA_LOCK_NAME = "quota.lock"


class CrossProcessLock:
    """进程内线程锁 + flock 跨进程锁的组合锁（不可重入）。"""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._thread_lock = threading.Lock()
        self._fd: int | None = None

    def acquire(self) -> None:
        # 先拿进程内锁，保证同进程的其他线程直接在这里排队
        self._thread_lock.acquire()
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
            except BaseException:
                os.close(fd)
                raise
            self._fd = fd
        except BaseException:
            self._thread_lock.release()
            raise

    def release(self) -> None:
        fd = self._fd
        if fd is None:
            raise RuntimeError("release called without acquire")
        self._fd = None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
            # 进程内锁最后释放：同进程的后继线程看到的磁盘状态一定已被
            # os.replace/fsync 发布（释放 flock 之前的写操作对后来者可见）
            self._thread_lock.release()

    def __enter__(self) -> "CrossProcessLock":
        self.acquire()
        return self

    def __exit__(self, *exc) -> None:
        self.release()


class LockManager:
    """管理暂存目录下会话锁与全局配额锁的锁文件。"""

    def __init__(self, root: Path):
        self.root = Path(root)
        self._guard = threading.Lock()
        self._locks: dict[str, CrossProcessLock] = {}

    def _get(self, key: str, path: Path) -> CrossProcessLock:
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = CrossProcessLock(path)
                self._locks[key] = lock
            return lock

    def _locks_dir(self) -> Path:
        return self.root / _LOCKS_DIR

    def session_lock(self, session_id: str) -> CrossProcessLock:
        return self._get(
            f"s:{session_id}", self._locks_dir() / "sessions" / f"{session_id}.lock"
        )

    def quota_lock(self) -> CrossProcessLock:
        return self._get("quota", self._locks_dir() / _QUOTA_LOCK_NAME)

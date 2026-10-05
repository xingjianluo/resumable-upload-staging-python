"""基于 fcntl.flock 的跨进程文件锁。

多进程/多实例共享同一个暂存目录时，进程内的 threading.Lock 无法互斥，
这里用文件锁把临界区扩展到整个机器范围（同一共享文件系统上语义相同）。

- 锁文件本身独立存放（``locks/`` 目录），与被保护的数据分离，
  会话被删除时锁文件仍可保留，不影响"先加锁再判定"的顺序；
- 锁是建议锁（advisory），所有访问方都经过本模块即能保证互斥；
- 进程崩溃时内核自动释放 flock，不会留下死锁。
"""

from __future__ import annotations

import fcntl
import os
import threading
from pathlib import Path
from typing import Self


class FileLock:
    """一个可重入的跨进程互斥锁（同一进程内按线程重入）。"""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._fd: int | None = None
        self._depth = 0
        self._owner: int | None = None
        self._guard = threading.Lock()

    def acquire(self) -> None:
        tid = threading.get_ident()
        with self._guard:
            if self._owner == tid:
                self._depth += 1
                return
        # 在 _guard 外阻塞等待文件锁，避免同进程其他线程空转
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX)
        with self._guard:
            # 等待期间可能被同线程重入？不会：同线程此时阻塞在 flock 上
            self._fd = fd
            self._owner = tid
            self._depth = 1

    def release(self) -> None:
        with self._guard:
            if self._owner != threading.get_ident() or self._depth == 0:
                raise RuntimeError("FileLock released by non-owner thread")
            self._depth -= 1
            if self._depth > 0:
                return
            fd = self._fd
            self._fd = None
            self._owner = None
        assert fd is not None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

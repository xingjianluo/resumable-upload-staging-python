"""暂存空间总量账本：跨进程共享的已占用字节数与上限校验。

账本文件 ``quota/ledger.json`` 与分片、元数据一样通过
"临时文件 + fsync + 原子 rename" 更新，所有读写都必须在 quota 全局
跨进程锁内进行。计费口径只统计 ``sessions/`` 下已确认的分片字节数；
``completed/`` 中的最终产物与 ``meta.json`` 等元数据不计入暂存配额。

容量约束在"写分片"这一分配点强制：写入前先记账（预留），写入/落盘
失败时回退；会话被清理（过期/失败/已回收）后其字节数返还账本，
后续上传可以重新使用这部分空间。
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .models import SessionStatus

_LEDGER_NAME = "ledger.json"
_QUOTA_DIR = "quota"
_SESSIONS_DIR = "sessions"
_METADATA_NAME = "meta.json"


class QuotaExceededError(RuntimeError):
    """内部异常：记账时容量不足（manager 层转译为 QuotaExceeded）。"""

    def __init__(self, used: int, limit: int, requested: int):
        super().__init__(f"quota exceeded: used={used} requested={requested} limit={limit}")
        self.used = used
        self.limit = limit
        self.requested = requested


class QuotaLedger:
    """暂存目录总占用的持久化账本。"""

    def __init__(self, root: Path, limit_bytes: int = 0):
        self.root = Path(root)
        self.limit_bytes = int(limit_bytes)

    def _path(self) -> Path:
        return self.root / _QUOTA_DIR / _LEDGER_NAME

    def load(self) -> int:
        """读取当前已占用字节数；账本不存在时按 0 处理。"""
        path = self._path()
        if not path.exists():
            return 0
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return max(0, int(data.get("used_bytes", 0)))

    def _store(self, used: int) -> None:
        path = self._path()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"used_bytes": max(0, int(used))}, ensure_ascii=False)
        fd, tmp_name = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp", dir=path.parent
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

    def charge(self, bytes_needed: int) -> int:
        """预留 bytes_needed 字节，返回预留后的总占用；不足则抛错且账本不变。"""
        used = self.load()
        if self.limit_bytes > 0 and used + bytes_needed > self.limit_bytes:
            raise QuotaExceededError(used, self.limit_bytes, bytes_needed)
        new_used = used + bytes_needed
        self._store(new_used)
        return new_used

    def refund(self, bytes_freed: int) -> int:
        """记账回退/释放字节数，返回释放后的总占用。"""
        used = max(0, self.load() - int(bytes_freed))
        self._store(used)
        return used

    def reconcile(self, sessions_root: Path | None = None) -> int:
        """按磁盘上的已确认分片重建账本（重启/崩溃后校准），返回校准后占用。"""
        root = sessions_root or (self.root / _SESSIONS_DIR)
        total = 0
        if root.exists():
            for session_dir in sorted(p for p in root.iterdir() if p.is_dir()):
                meta_path = session_dir / _METADATA_NAME
                if not meta_path.exists():
                    continue
                try:
                    with meta_path.open("r", encoding="utf-8") as f:
                        meta = json.load(f)
                except (OSError, json.JSONDecodeError):
                    # 元数据无法解析时不在此处删除，交由 recover 策略处理；
                    # 但要保守地把目录内实际分片字节计入，避免低估占用。
                    chunks_dir = session_dir / "chunks"
                    if chunks_dir.exists():
                        total += sum(
                            c.stat().st_size
                            for c in chunks_dir.iterdir()
                            if c.is_file()
                        )
                    continue
                # 只有仍在暂存（非 COMPLETED：completed 后分片目录会被回收）的
                # PENDING/EXPIRED/FAILED 会话占用暂存配额
                status = str(meta.get("status", SessionStatus.PENDING.value))
                if status == SessionStatus.COMPLETED.value:
                    continue
                for item in meta.get("chunks", []):
                    total += max(0, int(item.get("size", 0)))
        self._store(total)
        return total

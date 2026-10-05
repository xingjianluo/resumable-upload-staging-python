"""本地磁盘模拟的共享存储层。

目录布局::

    staging_dir/
        sessions/<session_id>/meta.json        # 会话元数据（持久化进度）
        sessions/<session_id>/chunks/<idx>     # 已确认分片
        sessions/<session_id>/assembled.tmp    # 提交时拼装中的临时文件
        completed/<session_id>-<filename>      # 拼装完成的最终文件

所有跨崩溃一致性依赖 "写临时文件 + fsync + 原子 rename(os.replace)" 模式。
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .config import StagingConfig
from .locks import FileLock
from .models import SessionState

_METADATA_NAME = "meta.json"
_CHUNKS_DIR = "chunks"
_SESSIONS_DIR = "sessions"
_COMPLETED_DIR = "completed"
_LOCKS_DIR = "locks"
_QUOTA_LOCK_NAME = "quota.lock"


def _safe_filename(filename: str) -> str:
    """把用户提供的文件名收敛为单层 basename，防止路径穿越。"""
    name = Path(filename).name
    name = name.replace(os.sep, "_").strip(". ")
    return name or "file"


class StagingStorage:
    """负责分片与元数据在磁盘上的持久化。"""

    def __init__(self, config: StagingConfig):
        self.config = config
        self.root = Path(config.staging_dir)

    # ---- 路径 ----
    def sessions_root(self) -> Path:
        return self.root / _SESSIONS_DIR

    def completed_root(self) -> Path:
        return self.root / _COMPLETED_DIR

    def session_dir(self, session_id: str) -> Path:
        return self.sessions_root() / session_id

    def chunk_path(self, session_id: str, index: int) -> Path:
        return self.session_dir(session_id) / _CHUNKS_DIR / str(index)

    def metadata_path(self, session_id: str) -> Path:
        return self.session_dir(session_id) / _METADATA_NAME

    def completed_path(self, session: SessionState) -> Path:
        safe = _safe_filename(session.filename)
        return self.completed_root() / f"{session.session_id}-{safe}"

    # ---- 跨进程锁 ----
    def session_lock(self, session_id: str) -> FileLock:
        """会话级跨进程锁：同一会话的写/提交/续期/回收互斥。"""
        return FileLock(self.root / _LOCKS_DIR / f"session-{session_id}.lock")

    def quota_lock(self) -> FileLock:
        """全局容量锁：用量统计与"检查+写入"必须在同一把锁内完成。"""
        return FileLock(self.root / _LOCKS_DIR / _QUOTA_LOCK_NAME)

    def discard_session_lock(self, session_id: str) -> None:
        """会话彻底删除后清理其锁文件（best-effort，避免锁文件无限增长）。"""
        (self.root / _LOCKS_DIR / f"session-{session_id}.lock").unlink(missing_ok=True)

    # ---- 容量 ----
    def usage_bytes(self) -> int:
        """暂存目录当前占用：已确认分片 + 已完成结果文件的字节数。

        元数据（meta.json）与锁文件属于固定小开销，不计入配额，
        这样配额语义与"上传数据量"一致，便于配置与推算。
        """
        total = 0
        sessions = self.sessions_root()
        if sessions.exists():
            for session_dir in sessions.iterdir():
                chunks_dir = session_dir / _CHUNKS_DIR
                try:
                    children = list(chunks_dir.iterdir())
                except FileNotFoundError:
                    # 与并发清理竞争：会话目录可能刚被整体删除
                    continue
                for child in children:
                    try:
                        if child.is_file():
                            total += child.stat().st_size
                    except FileNotFoundError:
                        continue
        completed = self.completed_root()
        if completed.exists():
            try:
                completed_children = list(completed.iterdir())
            except FileNotFoundError:
                completed_children = []
            for child in completed_children:
                try:
                    if child.is_file():
                        total += child.stat().st_size
                except FileNotFoundError:
                    continue
        return total

    # ---- 会话元数据 ----
    def create_session_layout(self, session: SessionState) -> None:
        """建立会话目录结构并写入第一版元数据。"""
        session_dir = self.session_dir(session.session_id)
        (session_dir / _CHUNKS_DIR).mkdir(parents=True, exist_ok=True)
        self.completed_root().mkdir(parents=True, exist_ok=True)
        self.save_metadata(session)

    def _atomic_write_bytes(self, target: Path, data: bytes) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=target.name + ".", suffix=".tmp", dir=target.parent)
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, target)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

    def save_metadata(self, session: SessionState) -> None:
        """原子写入元数据，保证崩溃后仍可读到一致版本。"""
        payload = json.dumps(session.to_metadata_dict(), ensure_ascii=False, indent=2)
        self._atomic_write_bytes(self.metadata_path(session.session_id), payload.encode("utf-8"))

    def load_metadata(self, session_id: str) -> SessionState | None:
        path = self.metadata_path(session_id)
        try:
            with path.open("r", encoding="utf-8") as f:
                return SessionState.from_metadata_dict(json.load(f))
        except FileNotFoundError:
            # 与并发清理竞争：exists 到 open 之间会话目录可能刚被删除
            return None

    def list_sessions(self) -> list[str]:
        root = self.sessions_root()
        if not root.exists():
            return []
        return sorted(p.name for p in root.iterdir() if p.is_dir())

    # ---- 分片 ----
    def write_chunk_atomic(self, session_id: str, index: int, data: bytes, checksum: str) -> None:
        """以临时文件 + 原子 rename 写入分片。

        checksum 仅用于日志/调试时把数据与校验值绑定，真正校验由 manager 完成。
        """
        del checksum
        self._atomic_write_bytes(self.chunk_path(session_id, index), data)

    def read_chunk(self, session_id: str, index: int) -> bytes:
        return self.chunk_path(session_id, index).read_bytes()

    def chunk_exists(self, session_id: str, index: int) -> bool:
        return self.chunk_path(session_id, index).exists()

    # ---- 提交 ----
    def assemble(self, session: SessionState) -> Path:
        """按序拼装分片为最终文件，返回最终文件路径。"""
        target = self.completed_path(session)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_path: Path
        fd, tmp_name = tempfile.mkstemp(prefix=f"{session.session_id}.", suffix=".part", dir=target.parent)
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as out:
                for index in range(session.total_chunks):
                    with self.chunk_path(session.session_id, index).open("rb") as src:
                        while True:
                            block = src.read(1024 * 1024)
                            if not block:
                                break
                            out.write(block)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp_path, target)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        return target

    # ---- 清理 ----
    def discard_chunks(self, session_id: str) -> None:
        """删除会话的全部分片（提交成功后释放空间；崩溃恢复时兜底）。"""
        import shutil

        shutil.rmtree(self.session_dir(session_id) / _CHUNKS_DIR, ignore_errors=True)

    def remove_session(self, session_id: str) -> None:
        import shutil

        shutil.rmtree(self.session_dir(session_id), ignore_errors=True)

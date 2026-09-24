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
from .models import SessionState

_METADATA_NAME = "meta.json"
_CHUNKS_DIR = "chunks"
_SESSIONS_DIR = "sessions"
_COMPLETED_DIR = "completed"


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
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as f:
            return SessionState.from_metadata_dict(json.load(f))

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
    def remove_session(self, session_id: str) -> None:
        import shutil

        shutil.rmtree(self.session_dir(session_id), ignore_errors=True)

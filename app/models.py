"""数据模型：会话元数据、分片状态、创建/提交请求与结果。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

SESSION_OPEN = "open"
SESSION_COMPLETED = "completed"
SESSION_ABORTED = "aborted"

_VALID_STATUSES = {SESSION_OPEN, SESSION_COMPLETED, SESSION_ABORTED}


@dataclass
class ChunkState:
    """单个分片的已确认状态。"""

    index: int
    size: int
    checksum: str
    committed_at: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "size": self.size,
            "checksum": self.checksum,
            "committed_at": self.committed_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChunkState":
        return cls(
            index=int(data["index"]),
            size=int(data["size"]),
            checksum=str(data["checksum"]),
            committed_at=float(data["committed_at"]),
        )


@dataclass
class SessionState:
    """会话元数据（持久化为 manifest.json）。"""

    session_id: str
    owner_token: str
    filename: str
    total_size: int
    chunk_size: int
    total_chunks: int
    chunk_checksums: list[str]
    checksum_algorithm: str
    final_checksum: str | None
    status: str
    created_at: float
    updated_at: float
    expires_at: float
    chunks: dict[int, ChunkState] = field(default_factory=dict)
    result_path: str | None = None

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(f"invalid session status: {self.status!r}")

    @property
    def received_indexes(self) -> list[int]:
        return sorted(self.chunks.keys())

    @property
    def missing_indexes(self) -> list[int]:
        return [i for i in range(self.total_chunks) if i not in self.chunks]

    def touch(self, now: float) -> None:
        """更新活动时间，但不改变过期时刻（TTL 自创建起算，语义确定）。"""
        self.updated_at = now

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "owner_token": self.owner_token,
            "filename": self.filename,
            "total_size": self.total_size,
            "chunk_size": self.chunk_size,
            "total_chunks": self.total_chunks,
            "chunk_checksums": list(self.chunk_checksums),
            "checksum_algorithm": self.checksum_algorithm,
            "final_checksum": self.final_checksum,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "chunks": {str(k): v.to_dict() for k, v in sorted(self.chunks.items())},
            "result_path": self.result_path,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SessionState":
        chunks = {
            int(k): ChunkState.from_dict(v) for k, v in data.get("chunks", {}).items()
        }
        return cls(
            session_id=str(data["session_id"]),
            owner_token=str(data["owner_token"]),
            filename=str(data["filename"]),
            total_size=int(data["total_size"]),
            chunk_size=int(data["chunk_size"]),
            total_chunks=int(data["total_chunks"]),
            chunk_checksums=list(data["chunk_checksums"]),
            checksum_algorithm=str(data.get("checksum_algorithm", "sha256")),
            final_checksum=data.get("final_checksum"),
            status=str(data["status"]),
            created_at=float(data["created_at"]),
            updated_at=float(data["updated_at"]),
            expires_at=float(data["expires_at"]),
            chunks=chunks,
            result_path=data.get("result_path"),
        )


@dataclass
class CreateSessionRequest:
    filename: str
    total_size: int
    chunk_size: int
    chunk_checksums: list[str]
    checksum_algorithm: str = "sha256"
    final_checksum: str | None = None
    ttl_seconds: int | None = None
    owner_token: str | None = None

    def __post_init__(self) -> None:
        if not self.filename:
            raise ValueError("filename is required")
        if self.total_size < 0:
            raise ValueError("total_size must be non-negative")
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if not self.chunk_checksums:
            raise ValueError("chunk_checksums must not be empty")
        if self.ttl_seconds is not None and self.ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")


@dataclass
class ChunkUploadResult:
    session_id: str
    index: int
    duplicate: bool
    size: int
    checksum: str
    reason: str


@dataclass
class CommitResult:
    session_id: str
    status: str
    result_path: str | None
    reason: str


def now_ts() -> float:
    """统一时间源，便于测试替换。"""
    return time.time()

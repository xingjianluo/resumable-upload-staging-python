"""会话与分片的数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class SessionStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass
class ChunkState:
    """单个已确认分片的状态。"""

    index: int
    size: int
    checksum: str
    received_at: float


@dataclass
class SessionState:
    """一个分片上传会话的完整状态。"""

    session_id: str
    owner_token: str
    filename: str
    total_chunks: int
    chunk_size: int
    total_size: int
    final_checksum: str
    checksum_algorithm: str
    created_at: float
    expires_at: float
    status: SessionStatus = SessionStatus.PENDING
    chunks: dict[int, ChunkState] = field(default_factory=dict)
    completed_at: float | None = None
    result_path: str | None = None
    fail_reason: str | None = None

    def to_metadata_dict(self) -> dict:
        """序列化为可落盘的 JSON 字典。"""
        return {
            "session_id": self.session_id,
            "owner_token": self.owner_token,
            "filename": self.filename,
            "total_chunks": self.total_chunks,
            "chunk_size": self.chunk_size,
            "total_size": self.total_size,
            "final_checksum": self.final_checksum,
            "checksum_algorithm": self.checksum_algorithm,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "status": self.status.value,
            "completed_at": self.completed_at,
            "result_path": self.result_path,
            "fail_reason": self.fail_reason,
            "chunks": [
                {
                    "index": c.index,
                    "size": c.size,
                    "checksum": c.checksum,
                    "received_at": c.received_at,
                }
                for c in sorted(self.chunks.values(), key=lambda c: c.index)
            ],
        }

    @classmethod
    def from_metadata_dict(cls, data: dict) -> SessionState:
        """从 JSON 字典恢复会话状态。"""
        chunks = {
            int(item["index"]): ChunkState(
                index=int(item["index"]),
                size=int(item["size"]),
                checksum=str(item["checksum"]),
                received_at=float(item["received_at"]),
            )
            for item in data.get("chunks", [])
        }
        return cls(
            session_id=str(data["session_id"]),
            owner_token=str(data["owner_token"]),
            filename=str(data["filename"]),
            total_chunks=int(data["total_chunks"]),
            chunk_size=int(data["chunk_size"]),
            total_size=int(data["total_size"]),
            final_checksum=str(data["final_checksum"]),
            checksum_algorithm=str(data.get("checksum_algorithm", "sha256")),
            created_at=float(data["created_at"]),
            expires_at=float(data["expires_at"]),
            status=SessionStatus(str(data["status"])),
            chunks=chunks,
            completed_at=data.get("completed_at"),
            result_path=data.get("result_path"),
            fail_reason=data.get("fail_reason"),
        )

    def received_indices(self) -> set[int]:
        """已确认分片序号集合。"""
        return set(self.chunks.keys())

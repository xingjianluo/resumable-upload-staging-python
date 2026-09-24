"""业务错误类型：每种失败原因对应独立异常，便于调用方区分判定依据。"""

from __future__ import annotations


class StagingError(Exception):
    """所有暂存错误的基类。"""

    error_code: str = "staging_error"
    http_status: int = 400


class SessionNotFound(StagingError):
    error_code = "session_not_found"
    http_status = 404


class SessionExpired(StagingError):
    error_code = "session_expired"
    http_status = 410


class UnauthorizedSession(StagingError):
    error_code = "unauthorized"
    http_status = 403


class SessionNotOpen(StagingError):
    """会话已提交/已中止，拒绝写入或提交。"""

    error_code = "session_not_open"
    http_status = 409


class ChunkIndexOutOfRange(StagingError):
    error_code = "chunk_index_out_of_range"
    http_status = 400


class ChunkSizeMismatch(StagingError):
    error_code = "chunk_size_mismatch"
    http_status = 400


class ChecksumMismatch(StagingError):
    error_code = "checksum_mismatch"
    http_status = 422


class MissingChunks(StagingError):
    error_code = "missing_chunks"
    http_status = 422


class FinalChecksumMismatch(StagingError):
    error_code = "final_checksum_mismatch"
    http_status = 422


class AlreadyCompleted(StagingError):
    """重复提交已完成会话：属幂等成功语义，路由层映射为已有结果。"""

    error_code = "already_completed"
    http_status = 409

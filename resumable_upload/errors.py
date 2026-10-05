"""分片上传相关的错误类型与拒绝原因。"""


class UploadError(Exception):
    """所有上传错误的基类，携带可区分的原因码。"""

    reason: str = "upload_error"

    def __init__(self, message: str = "", *, session_id: str | None = None):
        super().__init__(message or self.reason)
        self.message = message or self.reason
        self.session_id = session_id


class SessionNotFound(UploadError):
    reason = "session_not_found"


class SessionExpired(UploadError):
    reason = "session_expired"


class UnauthorizedSession(UploadError):
    reason = "unauthorized"


class SessionAlreadyFinalized(UploadError):
    reason = "session_finalized"


class ChunkOutOfRange(UploadError):
    reason = "chunk_out_of_range"


class ChunkTooLarge(UploadError):
    reason = "chunk_too_large"


class SessionFailed(UploadError):
    reason = "session_failed"


class ChecksumMismatch(UploadError):
    reason = "checksum_mismatch"


class MissingChunks(UploadError):
    reason = "missing_chunks"

    def __init__(self, message: str = "", *, missing: list[int] | None = None, session_id: str | None = None):
        super().__init__(message, session_id=session_id)
        self.missing = missing or []


class FinalChecksumMismatch(UploadError):
    reason = "final_checksum_mismatch"


class QuotaExceeded(UploadError):
    """暂存目录总容量不足：与会话过期、越权等拒绝原因可区分。"""

    reason = "quota_exceeded"

    def __init__(
        self,
        message: str = "",
        *,
        usage_bytes: int | None = None,
        limit_bytes: int | None = None,
        session_id: str | None = None,
    ):
        super().__init__(message, session_id=session_id)
        self.usage_bytes = usage_bytes
        self.limit_bytes = limit_bytes

"""HTTP 接口层：把 UploadManager 暴露为 FastAPI 路由。

鉴权约定：所有接口必须携带 ``X-Owner-Token`` 头标识上传者；
仅会话所有者可写入/查询/提交自己的会话。
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from .errors import (
    ChecksumMismatch,
    ChunkOutOfRange,
    ChunkTooLarge,
    FinalChecksumMismatch,
    MissingChunks,
    QuotaExceeded,
    SessionAlreadyFinalized,
    SessionExpired,
    SessionFailed,
    SessionNotFound,
    UnauthorizedSession,
    UploadError,
)
from .manager import UploadManager

logger = logging.getLogger("resumable_upload.api")

router = APIRouter(prefix="/uploads", tags=["uploads"])

# 各类拒绝原因对应的 HTTP 状态码
_STATUS_BY_REASON = {
    SessionNotFound.reason: 404,
    UnauthorizedSession.reason: 403,
    SessionExpired.reason: 410,
    SessionAlreadyFinalized.reason: 409,
    SessionFailed.reason: 409,
    ChunkOutOfRange.reason: 400,
    ChunkTooLarge.reason: 413,
    ChecksumMismatch.reason: 422,
    MissingChunks.reason: 409,
    FinalChecksumMismatch.reason: 422,
    QuotaExceeded.reason: 507,
}


class CreateSessionRequest(BaseModel):
    filename: str
    total_size: int
    chunk_size: int
    final_checksum: str
    checksum_algorithm: str = "sha256"


class CreateSessionResponse(BaseModel):
    session_id: str
    expires_at: float
    chunk_size: int
    total_chunks: int
    status: str


class RenewSessionRequest(BaseModel):
    # 顺延秒数；省略时按服务端配置的默认 TTL 顺延
    extend_seconds: float | None = None


def get_manager(request: Request) -> UploadManager:
    return request.app.state.upload_manager


def _raise_http(exc: UploadError) -> None:
    status = _STATUS_BY_REASON.get(exc.reason, 409)
    detail = {"reason": exc.reason, "message": exc.message}
    if isinstance(exc, MissingChunks):
        detail["missing_chunks"] = exc.missing
    if isinstance(exc, QuotaExceeded):
        detail["quota"] = {
            "used": exc.used,
            "limit": exc.limit,
            "requested": exc.requested,
        }
    logger.warning(
        "http reject reason=%s status=%d session=%s", exc.reason, status, exc.session_id
    )
    raise HTTPException(status_code=status, detail=detail)


@router.post("/sessions", response_model=CreateSessionResponse, status_code=201)
def create_session(
    body: CreateSessionRequest,
    request: Request,
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
) -> CreateSessionResponse:
    try:
        session = get_manager(request).create_session(
            filename=body.filename,
            total_size=body.total_size,
            chunk_size=body.chunk_size,
            final_checksum=body.final_checksum,
            owner_token=x_owner_token,
            checksum_algorithm=body.checksum_algorithm,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"reason": "invalid_request", "message": str(exc)})
    except UploadError as exc:
        _raise_http(exc)
    return CreateSessionResponse(
        session_id=session.session_id,
        expires_at=session.expires_at,
        chunk_size=session.chunk_size,
        total_chunks=session.total_chunks,
        status=session.status.value,
    )


@router.get("/sessions/{session_id}")
def get_progress(
    session_id: str,
    request: Request,
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
) -> dict:
    try:
        return get_manager(request).get_progress(session_id, x_owner_token)
    except UploadError as exc:
        _raise_http(exc)


@router.post("/sessions/{session_id}/renew")
def renew_session(
    session_id: str,
    body: RenewSessionRequest,
    request: Request,
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
) -> dict:
    try:
        session = get_manager(request).renew_session(
            session_id, x_owner_token, body.extend_seconds
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"reason": "invalid_request", "message": str(exc)})
    except UploadError as exc:
        _raise_http(exc)
    return {
        "session_id": session.session_id,
        "status": session.status.value,
        "expires_at": session.expires_at,
        "received_chunks": sorted(session.chunks.keys()),
    }


@router.put("/sessions/{session_id}/chunks/{index}")
async def put_chunk(
    session_id: str,
    index: int,
    request: Request,
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
    x_chunk_checksum: str = Header(..., alias="X-Chunk-Checksum"),
) -> dict:
    data = await request.body()
    try:
        state = get_manager(request).put_chunk(
            session_id, x_owner_token, index, data, x_chunk_checksum
        )
    except UploadError as exc:
        _raise_http(exc)
    return {
        "session_id": session_id,
        "chunk": index,
        "size": state.size,
        "checksum": state.checksum,
        "acknowledged": True,
    }


@router.post("/sessions/{session_id}/commit")
def commit(
    session_id: str,
    request: Request,
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
) -> dict:
    try:
        session = get_manager(request).commit(session_id, x_owner_token)
    except UploadError as exc:
        _raise_http(exc)
    return {
        "session_id": session.session_id,
        "status": session.status.value,
        "result_path": session.result_path,
        "completed_at": session.completed_at,
    }

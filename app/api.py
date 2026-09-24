"""FastAPI 路由：会话创建、分片上传、状态查询、提交/中止、恢复与清理。"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import Settings, get_settings
from app.errors import StagingError
from app.models import CreateSessionRequest
from app.session import SessionManager
from app.storage import DiskStore

logger = logging.getLogger("app.api")


class CreateSessionBody(BaseModel):
    filename: str
    total_size: int = Field(ge=0)
    chunk_size: int = Field(gt=0)
    chunk_checksums: list[str]
    checksum_algorithm: str = "sha256"
    final_checksum: str | None = None
    ttl_seconds: int | None = Field(default=None, gt=0)
    owner_token: str | None = None


class CommitBody(BaseModel):
    owner_token: str | None = None


def create_app(
    settings: Settings | None = None,
    manager: SessionManager | None = None,
    *,
    enable_cleanup: bool = False,
) -> FastAPI:
    settings = settings or get_settings()
    manager = manager or SessionManager(
        settings, DiskStore(settings.staging_dir)
    )
    app = FastAPI(title="resumable-upload-staging", version="0.1.0")
    app.state.settings = settings
    app.state.manager = manager
    install_exception_handlers(app)
    _install_routes(app)

    if enable_cleanup:
        @app.on_event("startup")
        async def _start_cleanup() -> None:  # pragma: no cover - 后台循环
            async def loop() -> None:
                while True:
                    await asyncio.sleep(settings.cleanup_interval_seconds)
                    try:
                        removed = await asyncio.to_thread(
                            manager.cleanup_expired
                        )
                        if removed:
                            logger.info("后台清理过期会话: %s", removed)
                    except Exception:  # noqa: BLE001
                        logger.exception("后台清理失败")

            app.state.cleanup_task = asyncio.create_task(loop())

        @app.on_event("shutdown")
        async def _stop_cleanup() -> None:  # pragma: no cover
            task = getattr(app.state, "cleanup_task", None)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    return app


def install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(StagingError)
    async def _handle(request: Request, exc: StagingError) -> JSONResponse:
        return _error_response(exc)

    @app.exception_handler(ValueError)
    async def _handle_value(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"error_code": "invalid_request", "message": str(exc)},
        )


def _error_response(exc: StagingError) -> JSONResponse:
    logger.warning(
        "请求被拒绝 error_code=%s http=%d message=%s",
        exc.error_code,
        exc.http_status,
        exc,
    )
    return JSONResponse(
        status_code=exc.http_status,
        content={"error_code": exc.error_code, "message": str(exc)},
    )


def _token(
    x_owner_token: str | None, body_token: str | None
) -> str:
    token = x_owner_token or body_token
    if not token:
        raise HTTPException(status_code=401, detail="owner token required")
    return token


def _install_routes(app: FastAPI) -> None:
    @app.post("/sessions", status_code=201)
    def create_session(body: CreateSessionBody):
        manager: SessionManager = app.state.manager
        req = CreateSessionRequest(
            filename=body.filename,
            total_size=body.total_size,
            chunk_size=body.chunk_size,
            chunk_checksums=body.chunk_checksums,
            checksum_algorithm=body.checksum_algorithm,
            final_checksum=body.final_checksum,
            ttl_seconds=body.ttl_seconds,
            owner_token=body.owner_token,
        )
        state = manager.create_session(req)
        return state.to_dict()

    @app.get("/sessions/{session_id}")
    def session_status(session_id: str):
        manager: SessionManager = app.state.manager
        state = manager.get_session(session_id)
        data = state.to_dict()
        data["missing_indexes"] = state.missing_indexes
        return data

    @app.put("/sessions/{session_id}/chunks/{index}")
    async def upload_chunk(
        session_id: str,
        index: int,
        request: Request,
        x_owner_token: str | None = Header(default=None),
        x_chunk_checksum: str | None = Header(default=None),
    ):
        token = _token(x_owner_token, None)
        payload = await request.body()
        manager: SessionManager = app.state.manager
        result = manager.upload_chunk(
            session_id=session_id,
            index=index,
            payload=payload,
            owner_token=token,
            checksum=x_chunk_checksum,
        )
        return {
            "session_id": result.session_id,
            "index": result.index,
            "duplicate": result.duplicate,
            "size": result.size,
            "checksum": result.checksum,
            "reason": result.reason,
        }

    @app.post("/sessions/{session_id}/commit")
    def commit_session(
        session_id: str,
        body: CommitBody | None = None,
        x_owner_token: str | None = Header(default=None),
    ):
        token = _token(x_owner_token, body.owner_token if body else None)
        manager: SessionManager = app.state.manager
        result = manager.commit_session(session_id, token)
        return {
            "session_id": result.session_id,
            "status": result.status,
            "result_path": result.result_path,
            "reason": result.reason,
        }

    @app.post("/sessions/{session_id}/abort")
    def abort_session(
        session_id: str,
        body: CommitBody | None = None,
        x_owner_token: str | None = Header(default=None),
    ):
        token = _token(x_owner_token, body.owner_token if body else None)
        manager: SessionManager = app.state.manager
        state = manager.abort_session(session_id, token)
        return state.to_dict()

    @app.post("/admin/recover")
    def recover():
        manager: SessionManager = app.state.manager
        recovered = manager.recover()
        return {"recovered": recovered, "count": len(recovered)}

    @app.post("/admin/cleanup")
    def cleanup(now: float | None = None):
        manager: SessionManager = app.state.manager
        removed = manager.cleanup_expired(now=now)
        return {"removed": removed, "count": len(removed)}

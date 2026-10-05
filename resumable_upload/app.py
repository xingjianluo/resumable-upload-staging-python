"""FastAPI 应用装配。"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api import router as uploads_router
from .config import StagingConfig
from .manager import UploadManager


def create_app(config: StagingConfig | None = None) -> FastAPI:
    config = config or StagingConfig()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        manager = UploadManager(config)
        manager.recover()
        manager.start_cleanup_worker()
        app.state.upload_manager = manager
        try:
            yield
        finally:
            manager.stop_cleanup_worker()

    app = FastAPI(title="resumable-upload-staging", lifespan=lifespan)
    app.include_router(uploads_router)
    return app


app = create_app()

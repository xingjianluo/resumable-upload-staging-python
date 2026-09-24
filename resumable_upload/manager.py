"""分片暂存的核心服务：会话生命周期、并发控制、提交与清理。

并发模型
--------
- 每个会话持有一把 :class:`threading.RLock`，同一会话的分片写入串行化，
  保证并发写同一分片不会互相覆盖、进度更新不会丢失更新；
- 不同会话使用不同的锁，天然相互隔离；
- 分片数据与元数据均通过 "临时文件 + fsync + 原子 rename" 落盘，
  崩溃/重启后已确认（已写入元数据）的进度不会丢失。

提交与清理
----------
- 提交与会话清理都在会话锁内完成状态迁移，重复提交幂等且只会产生一份结果文件；
- 过期清理只删除仍处于 PENDING 的会话；commit 会先对会话做过期判定，
  因此 commit 与清理并发时结论确定：要么过期、要么唯一一次成功提交。
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
import uuid
from pathlib import Path

from .config import StagingConfig
from .errors import (
    ChecksumMismatch,
    ChunkOutOfRange,
    ChunkTooLarge,
    FinalChecksumMismatch,
    MissingChunks,
    SessionAlreadyFinalized,
    SessionExpired,
    SessionFailed,
    SessionNotFound,
    UnauthorizedSession,
)
from .models import ChunkState, SessionState, SessionStatus
from .storage import StagingStorage

logger = logging.getLogger("resumable_upload")


class UploadManager:
    """对外提供创建会话、写入分片、查询进度、提交与清理能力。"""

    def __init__(self, config: StagingConfig | None = None):
        self.config = config or StagingConfig()
        self.storage = StagingStorage(self.config)
        # 每个会话一把锁，保护同一会话内的并发写与状态迁移
        self._session_locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()
        self._stop_event = threading.Event()
        self._cleanup_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # 锁与后台任务
    # ------------------------------------------------------------------
    def _lock_for(self, session_id: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._session_locks.get(session_id)
            if lock is None:
                lock = threading.RLock()
                self._session_locks[session_id] = lock
            return lock

    def start_cleanup_worker(self) -> None:
        if self._cleanup_thread is not None and self._cleanup_thread.is_alive():
            return
        self._stop_event.clear()
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop, name="staging-cleanup", daemon=True
        )
        self._cleanup_thread.start()
        logger.info("cleanup worker started interval=%.1fs", self.config.cleanup_interval_seconds)

    def stop_cleanup_worker(self) -> None:
        self._stop_event.set()
        thread = self._cleanup_thread
        if thread is not None and thread.is_alive() and threading.current_thread() is not thread:
            thread.join(timeout=5.0)
        self._cleanup_thread = None

    def _cleanup_loop(self) -> None:
        while not self._stop_event.wait(self.config.cleanup_interval_seconds):
            try:
                removed = self.cleanup_expired()
                if removed:
                    logger.info("background cleanup removed sessions=%s", removed)
            except Exception:  # pragma: no cover - 后台循环不能因单次异常退出
                logger.exception("cleanup iteration failed")

    # ------------------------------------------------------------------
    # 会话生命周期
    # ------------------------------------------------------------------
    def create_session(
        self,
        *,
        filename: str,
        total_size: int,
        chunk_size: int,
        final_checksum: str,
        owner_token: str,
        checksum_algorithm: str = "sha256",
    ) -> SessionState:
        if total_size < 0 or chunk_size <= 0:
            raise ValueError("total_size must be >= 0 and chunk_size must be > 0")
        total_chunks = (total_size + chunk_size - 1) // chunk_size if total_size else 0
        if total_size == 0:
            total_chunks = 0
        now = time.time()
        session = SessionState(
            session_id=uuid.uuid4().hex,
            owner_token=owner_token,
            filename=filename,
            total_chunks=total_chunks,
            chunk_size=chunk_size,
            total_size=total_size,
            final_checksum=final_checksum,
            checksum_algorithm=checksum_algorithm,
            created_at=now,
            expires_at=now + self.config.session_ttl_seconds,
        )
        # 先登记锁，再落盘，保证创建后立刻并发写也能串行化
        self._lock_for(session.session_id)
        self.storage.create_session_layout(session)
        logger.info(
            "session created session=%s owner=%s file=%s chunks=%d ttl=%.0fs",
            session.session_id, owner_token, filename, total_chunks,
            self.config.session_ttl_seconds,
        )
        return session

    def _get_session(self, session_id: str, owner_token: str | None = None) -> SessionState:
        """加载会话并依次校验存在性、归属与有效期。"""
        session = self.storage.load_metadata(session_id)
        if session is None:
            logger.warning("access rejected session=%s reason=session_not_found", session_id)
            raise SessionNotFound(session_id=session_id)
        if owner_token is not None and session.owner_token != owner_token:
            logger.warning(
                "access rejected session=%s reason=unauthorized provided_owner=%s",
                session_id, owner_token,
            )
            raise UnauthorizedSession(session_id=session_id)
        return session

    @staticmethod
    def _ensure_active(session: SessionState, now: float | None = None) -> None:
        """过期/终态判定，原因可区分。"""
        current = now if now is not None else time.time()
        if session.status == SessionStatus.COMPLETED:
            raise SessionAlreadyFinalized(session_id=session.session_id)
        if session.status == SessionStatus.FAILED:
            raise SessionFailed(
                f"session previously failed: {session.fail_reason}",
                session_id=session.session_id,
            )
        if current > session.expires_at:
            logger.warning(
                "access rejected session=%s reason=session_expired now=%.3f expires_at=%.3f",
                session.session_id, current, session.expires_at,
            )
            raise SessionExpired(session_id=session.session_id)

    def get_progress(self, session_id: str, owner_token: str) -> dict:
        session = self._get_session(session_id, owner_token)
        return {
            "session_id": session.session_id,
            "status": session.status.value,
            "total_chunks": session.total_chunks,
            "received_chunks": sorted(session.chunks.keys()),
            "missing_chunks": sorted(
                i for i in range(session.total_chunks) if i not in session.chunks
            ),
            "expires_at": session.expires_at,
            "result_path": session.result_path,
            "fail_reason": session.fail_reason,
        }

    # ------------------------------------------------------------------
    # 分片写入
    # ------------------------------------------------------------------
    def _checksum(self, algorithm: str, data: bytes) -> str:
        try:
            return hashlib.new(algorithm, data).hexdigest()
        except ValueError as exc:  # pragma: no cover - 算法在会话创建时已固定
            raise ValueError(f"unsupported checksum algorithm: {algorithm}") from exc

    def put_chunk(
        self,
        session_id: str,
        owner_token: str,
        index: int,
        data: bytes,
        checksum: str,
    ) -> ChunkState:
        self._get_session(session_id, owner_token)
        lock = self._lock_for(session_id)
        with lock:
            # 磁盘状态是权威来源，锁内重新加载，避免使用过期内存视图
            session = self.storage.load_metadata(session_id)
            if session is None:
                # 与过期清理并发时会话可能已被删除
                logger.warning("chunk rejected session=%s reason=session_not_found", session_id)
                raise SessionNotFound(session_id=session_id)
            self._ensure_active(session)

            if not 0 <= index < session.total_chunks:
                logger.warning(
                    "chunk rejected session=%s chunk=%d reason=chunk_out_of_range total=%d",
                    session_id, index, session.total_chunks,
                )
                raise ChunkOutOfRange(session_id=session_id)
            if len(data) > self.config.chunk_max_bytes:
                logger.warning(
                    "chunk rejected session=%s chunk=%d reason=chunk_too_large size=%d max=%d",
                    session_id, index, len(data), self.config.chunk_max_bytes,
                )
                raise ChunkTooLarge(session_id=session_id)

            actual = self._checksum(session.checksum_algorithm, data)
            if actual != checksum:
                logger.warning(
                    "chunk rejected session=%s chunk=%d reason=checksum_mismatch expected=%s actual=%s",
                    session_id, index, checksum, actual,
                )
                raise ChecksumMismatch(
                    f"chunk {index} checksum mismatch: expected {checksum}, got {actual}",
                    session_id=session_id,
                )

            existing = session.chunks.get(index)
            if existing is not None:
                if existing.checksum == checksum and existing.size == len(data):
                    logger.info(
                        "chunk idempotent-ack session=%s chunk=%d size=%d checksum=%s",
                        session_id, index, len(data), checksum,
                    )
                    return existing
                logger.warning(
                    "chunk rejected session=%s chunk=%d reason=checksum_conflict "
                    "confirmed_checksum=%s submitted_checksum=%s",
                    session_id, index, existing.checksum, checksum,
                )
                raise ChecksumMismatch(
                    f"chunk {index} already confirmed with different checksum",
                    session_id=session_id,
                )

            # 先原子写数据，再更新元数据；两步之间崩溃只可能留下孤儿分片，
            # 下次重传时会被原子覆盖，不会产生错误的"已确认"进度
            self.storage.write_chunk_atomic(session_id, index, data, checksum)
            state = ChunkState(
                index=index, size=len(data), checksum=checksum, received_at=time.time()
            )
            session.chunks[index] = state
            self.storage.save_metadata(session)
            logger.info(
                "chunk confirmed session=%s chunk=%d size=%d checksum=%s progress=%d/%d",
                session_id, index, len(data), checksum,
                len(session.chunks), session.total_chunks,
            )
            return state

    # ------------------------------------------------------------------
    # 提交
    # ------------------------------------------------------------------
    def commit(self, session_id: str, owner_token: str) -> SessionState:
        self._get_session(session_id, owner_token)
        lock = self._lock_for(session_id)
        with lock:
            session = self.storage.load_metadata(session_id)
            if session is None:
                logger.warning("commit rejected session=%s reason=session_not_found", session_id)
                raise SessionNotFound(session_id=session_id)

            # 幂等：重复提交同一已完成会话，直接返回同一结果
            if session.status == SessionStatus.COMPLETED:
                logger.info(
                    "commit idempotent-return session=%s result=%s",
                    session_id, session.result_path,
                )
                return session
            if session.status == SessionStatus.FAILED:
                raise SessionFailed(
                    f"session previously failed: {session.fail_reason}",
                    session_id=session_id,
                )
            self._ensure_active(session)

            missing = sorted(i for i in range(session.total_chunks) if i not in session.chunks)
            if missing:
                # 缺片不改终态：会话保持 PENDING，客户端补齐后可重新提交
                logger.warning(
                    "commit rejected session=%s reason=missing_chunks missing=%s",
                    session_id, missing,
                )
                raise MissingChunks(
                    f"missing chunks: {missing}", missing=missing, session_id=session_id
                )

            result_path = self.storage.assemble(session)

            hasher = hashlib.new(session.checksum_algorithm)
            with result_path.open("rb") as f:
                while True:
                    block = f.read(1024 * 1024)
                    if not block:
                        break
                    hasher.update(block)
            actual_final = hasher.hexdigest()
            if actual_final != session.final_checksum:
                # 完整性校验失败：删除拼装产物并把会话置为失败，禁止静默成功
                result_path.unlink(missing_ok=True)
                session.status = SessionStatus.FAILED
                session.fail_reason = FinalChecksumMismatch.reason
                self.storage.save_metadata(session)
                logger.warning(
                    "commit rejected session=%s reason=final_checksum_mismatch expected=%s actual=%s",
                    session_id, session.final_checksum, actual_final,
                )
                raise FinalChecksumMismatch(
                    f"expected {session.final_checksum}, got {actual_final}",
                    session_id=session_id,
                )

            session.status = SessionStatus.COMPLETED
            session.completed_at = time.time()
            session.result_path = str(result_path)
            self.storage.save_metadata(session)
            logger.info(
                "commit completed session=%s chunks=%d result=%s checksum=%s",
                session_id, session.total_chunks, result_path, actual_final,
            )
            return session

    # ------------------------------------------------------------------
    # 清理与恢复
    # ------------------------------------------------------------------
    def cleanup_expired(self, now: float | None = None) -> list[str]:
        """删除过期且仍未完成的会话；已完成会话保留。返回被清理的会话 id。"""
        current = now if now is not None else time.time()
        removed: list[str] = []
        for session_id in self.storage.list_sessions():
            lock = self._lock_for(session_id)
            with lock:
                session = self.storage.load_metadata(session_id)
                if session is None:
                    continue
                if session.status != SessionStatus.PENDING:
                    continue
                if current <= session.expires_at:
                    continue
                self.storage.remove_session(session_id)
                logger.info(
                    "session expired-removed session=%s expires_at=%.3f status=pending",
                    session_id, session.expires_at,
                )
                removed.append(session_id)
            # 已彻底删除后再摘除锁索引；删除本身在锁内，与 commit 互斥
            if session_id in removed:
                with self._locks_guard:
                    self._session_locks.pop(session_id, None)
        return removed

    def recover(self) -> int:
        """重启恢复：扫描暂存目录，重建锁索引并核对元数据与分片的一致性。

        返回识别到的会话数量。已确认进度完全以磁盘上的 meta.json 为准，
        因此重启后不会丢失；数据已写但元数据未更新的孤儿分片会被清除，
        等待客户端重传该分片。
        """
        recovered = 0
        for session_id in self.storage.list_sessions():
            self._lock_for(session_id)
            session = self.storage.load_metadata(session_id)
            if session is None:
                continue
            recovered += 1
            chunks_dir = self.storage.session_dir(session_id) / "chunks"
            if chunks_dir.exists():
                confirmed = set(session.chunks.keys())
                for child in chunks_dir.iterdir():
                    if not child.is_file():
                        continue
                    try:
                        idx = int(child.name)
                    except ValueError:
                        idx = -1
                    if idx not in confirmed:
                        child.unlink(missing_ok=True)
                        logger.info(
                            "recover orphan-chunk session=%s chunk=%s action=discard",
                            session_id, child.name,
                        )
            logger.info(
                "recover session=%s status=%s progress=%d/%d",
                session_id, session.status.value,
                len(session.chunks), session.total_chunks,
            )
        logger.info("recovery finished sessions=%d staging_dir=%s", recovered, self.storage.root)
        return recovered

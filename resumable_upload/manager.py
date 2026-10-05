"""分片暂存的核心服务：会话生命周期、跨进程并发控制、续期、容量约束、提交与清理。

并发模型
--------
- 每个会话持有一把跨进程组合锁（进程内线程锁 + 共享目录上的 flock），
  同一暂存目录被多个 worker 进程/多个实例共享时，同一会话的分片写入、
  续期、提交与回收仍然互斥，不会互相覆盖、丢失已确认进度或生成两份结果；
- 不同会话使用不同的锁文件，天然相互隔离；
- 分片数据与元数据均通过 "临时文件 + fsync + 原子 rename" 落盘，
  崩溃/重启后已确认（已写入元数据）的进度不会丢失。

容量约束
--------
- 暂存总量由共享目录下的配额账本记录，所有账本读写在全局配额锁内完成；
- 新建会话做容量预检，写分片时强制记账（预留 -> 写盘 -> 失败回退）；
- 提交成功或会话被回收后，占用的暂存字节返还账本，可被后续上传复用。

提交与清理
----------
- 提交、续期与会话清理都在会话锁内完成状态迁移，重复提交幂等且只会
  产生一份结果文件；
- 过期清理删除仍处于 PENDING（已过期）或 FAILED 的会话；commit/renew
  会先对会话做有效期判定，因此与清理并发时结论确定：
  要么过期/不存在，要么唯一一次成功。
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
    QuotaExceeded,
    SessionAlreadyFinalized,
    SessionExpired,
    SessionFailed,
    SessionNotFound,
    UnauthorizedSession,
)
from .locking import LockManager
from .models import ChunkState, SessionState, SessionStatus
from .quota import QuotaExceededError, QuotaLedger
from .storage import StagingStorage

logger = logging.getLogger("resumable_upload")


class UploadManager:
    """对外提供创建会话、写入分片、查询进度、续期、提交与清理能力。"""

    def __init__(self, config: StagingConfig | None = None):
        self.config = config or StagingConfig()
        self.storage = StagingStorage(self.config)
        # 跨进程组合锁：锁文件位于共享暂存目录，对所有实例可见
        self.locks = LockManager(self.storage.root)
        self.quota = QuotaLedger(self.storage.root, self.config.max_staging_bytes)
        self._stop_event = threading.Event()
        self._cleanup_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # 后台任务
    # ------------------------------------------------------------------
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
        # 容量预检：暂存空间已经被占满时，新建会话直接明确拒绝
        # （reason=quota_exceeded），与 session_expired / unauthorized 区分。
        # 不按声明大小提前预留：容量是在每个分片确认时逐片占用的，
        # 声明大于当前剩余量的会话仍可创建（可等其他会话提交/回收释放后再传），
        # 真正的上限强制发生在 put_chunk 的记账点。
        if self.config.max_staging_bytes > 0:
            with self.locks.quota_lock():
                used = self.quota.load()
            if used >= self.config.max_staging_bytes:
                logger.warning(
                    "session create rejected reason=quota_exceeded "
                    "used=%d requested=%d limit=%d",
                    used, total_size, self.config.max_staging_bytes,
                )
                raise QuotaExceeded(
                    f"staging quota exhausted: used={used} limit={self.config.max_staging_bytes}",
                    used=used,
                    limit=self.config.max_staging_bytes,
                    requested=total_size,
                )
        # 先建锁文件，再落盘，保证创建后立刻跨进程并发写也能串行化
        self.locks.session_lock(session.session_id)
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
        with self.locks.session_lock(session_id):
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

            # 容量强制：在全局配额锁内做检查并预留本分片字节；锁内只做账本的
            # 读改写，磁盘写放在配额锁外，避免长时间阻塞其他会话的配额检查。
            size = len(data)
            if self.config.max_staging_bytes > 0:
                try:
                    with self.locks.quota_lock():
                        used_after = self.quota.charge(size)
                except QuotaExceededError as exc:
                    logger.warning(
                        "chunk rejected session=%s chunk=%d reason=quota_exceeded "
                        "used=%d requested=%d limit=%d",
                        session_id, index, exc.used, size, exc.limit,
                    )
                    raise QuotaExceeded(
                        f"staging quota exceeded: used={exc.used} requested={size} "
                        f"limit={exc.limit}",
                        session_id=session_id,
                        used=exc.used,
                        limit=exc.limit,
                        requested=size,
                    ) from exc
            else:
                used_after = None
            try:
                # 先原子写数据，再更新元数据；两步之间崩溃只可能留下孤儿分片，
                # 下次重传时会被原子覆盖，recover 时按账本校准，不会产生错误的
                # "已确认"进度，也不会长期高估占用。
                self.storage.write_chunk_atomic(session_id, index, data, checksum)
                state = ChunkState(
                    index=index, size=size, checksum=checksum, received_at=time.time()
                )
                session.chunks[index] = state
                self.storage.save_metadata(session)
            except BaseException:
                # 写盘/元数据落盘失败：回退本次预留，避免容量被"幽灵占用"
                if self.config.max_staging_bytes > 0:
                    with self.locks.quota_lock():
                        self.quota.refund(size)
                raise
            logger.info(
                "chunk confirmed session=%s chunk=%d size=%d checksum=%s progress=%d/%d%s",
                session_id, index, size, checksum,
                len(session.chunks), session.total_chunks,
                f" quota_used={used_after}" if used_after is not None else "",
            )
            return state

    # ------------------------------------------------------------------
    # 提交
    # ------------------------------------------------------------------
    def commit(self, session_id: str, owner_token: str) -> SessionState:
        self._get_session(session_id, owner_token)
        with self.locks.session_lock(session_id):
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

            staged_bytes = self.storage.staged_bytes(session)
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
            # 先发布 COMPLETED 元数据，再回收暂存分片：COMPLETED 是权威结论，
            # 与任何实例上的回收/写入并发时，后者看到 COMPLETED 都不会再动数据。
            self.storage.save_metadata(session)
            if staged_bytes:
                self.storage.release_staged_chunks(session)
                # 分片已落盘的字节是计费口径；meta 中的 chunks 记录保留用于
                # 审计，实际磁盘占用已清零，在全局配额锁内返还账本。
                if self.config.max_staging_bytes > 0:
                    with self.locks.quota_lock():
                        freed = self.quota.refund(staged_bytes)
                    logger.info(
                        "quota released session=%s bytes=%d quota_used=%d",
                        session_id, staged_bytes, freed,
                    )
            logger.info(
                "commit completed session=%s chunks=%d result=%s checksum=%s",
                session_id, session.total_chunks, result_path, actual_final,
            )
            return session

    # ------------------------------------------------------------------
    # 续期 / 保活
    # ------------------------------------------------------------------
    def renew_session(
        self,
        session_id: str,
        owner_token: str,
        extend_seconds: float | None = None,
    ) -> SessionState:
        """把进行中会话的有效期向后顺延。

        - 仅 PENDING 且未过期的会话可续期；已完成/失败/过期分别按
          session_finalized / session_failed / session_expired 拒绝；
        - 续期只改 ``expires_at``，已确认分片、校验结论一律不变；
        - 新的有效期在会话锁内 "读盘 -> 判定 -> 写盘"，与回收互斥，
          二者撞车时结论唯一：续期成功则清理不会删它，清理先完成则续期
          得到 session_not_found/expired；
        - 延长后的过期时间持久化在 meta.json，重启不丢；
        - 受 ``max_session_ttl_seconds``（相对创建时刻）上限约束。
        """
        if extend_seconds is None:
            extend_seconds = self.config.session_ttl_seconds
        if extend_seconds <= 0:
            raise ValueError("extend_seconds must be > 0")
        self._get_session(session_id, owner_token)
        with self.locks.session_lock(session_id):
            session = self.storage.load_metadata(session_id)
            if session is None:
                logger.warning("renew rejected session=%s reason=session_not_found", session_id)
                raise SessionNotFound(session_id=session_id)
            if session.status == SessionStatus.COMPLETED:
                raise SessionAlreadyFinalized(session_id=session_id)
            if session.status == SessionStatus.FAILED:
                raise SessionFailed(
                    f"session previously failed: {session.fail_reason}",
                    session_id=session_id,
                )
            now = time.time()
            if now > session.expires_at:
                logger.warning(
                    "renew rejected session=%s reason=session_expired now=%.3f expires_at=%.3f",
                    session_id, now, session.expires_at,
                )
                raise SessionExpired(session_id=session_id)

            new_expires = now + extend_seconds
            cap = self.config.max_session_ttl_seconds
            if cap and cap > 0:
                upper = session.created_at + cap
                if new_expires > upper:
                    new_expires = upper
            # 续期只能顺延：封顶或较短 extend 都不能把有效期缩到当前值之前
            if new_expires < session.expires_at:
                new_expires = session.expires_at
            old_expires = session.expires_at
            session.expires_at = new_expires
            self.storage.save_metadata(session)
            logger.info(
                "session renewed session=%s old_expires_at=%.3f new_expires_at=%.3f "
                "progress=%d/%d",
                session_id, old_expires, new_expires,
                len(session.chunks), session.total_chunks,
            )
            return session

    # ------------------------------------------------------------------
    # 清理与恢复
    # ------------------------------------------------------------------
    def cleanup_expired(self, now: float | None = None) -> list[str]:
        """回收过期的 PENDING 会话与 FAILED 会话；已完成会话保留。

        返回被清理的会话 id。会话判定与目录删除在跨进程会话锁内完成，
        并在全局配额锁内把释放的字节返还账本。
        """
        current = now if now is not None else time.time()
        removed: list[str] = []
        for session_id in self.storage.list_sessions():
            with self.locks.session_lock(session_id):
                session = self.storage.load_metadata(session_id)
                if session is None:
                    continue
                if session.status == SessionStatus.COMPLETED:
                    continue
                # FAILED 会话不可再用，立即回收；PENDING 仅在过期后回收
                if session.status == SessionStatus.PENDING and current <= session.expires_at:
                    continue
                staged_bytes = self.storage.staged_bytes(session)
                self.storage.remove_session(session_id)
                if staged_bytes and self.config.max_staging_bytes > 0:
                    with self.locks.quota_lock():
                        self.quota.refund(staged_bytes)
                logger.info(
                    "session reaped session=%s status=%s expires_at=%.3f freed_bytes=%d",
                    session_id, session.status.value, session.expires_at, staged_bytes,
                )
                removed.append(session_id)
        return removed

    def recover(self) -> int:
        """重启恢复：扫描暂存目录，重建锁视图、校准配额账本并核对一致性。

        返回识别到的会话数量。已确认进度完全以磁盘上的 meta.json 为准，
        因此重启后不会丢失（包括续期后的 expires_at）；数据已写但元数据
        未更新的孤儿分片会被清除，等待客户端重传该分片。容量账本按
        磁盘实际已确认分片重新校准，崩溃窗口中"预留了但没写成"的字节
        不会再占用配额。
        """
        recovered = 0
        for session_id in self.storage.list_sessions():
            # 确保该会话的跨进程锁文件就绪
            self.locks.session_lock(session_id)
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
                "recover session=%s status=%s progress=%d/%d expires_at=%.3f",
                session_id, session.status.value,
                len(session.chunks), session.total_chunks, session.expires_at,
            )
        if self.config.max_staging_bytes > 0:
            with self.locks.quota_lock():
                used = self.quota.reconcile(self.storage.sessions_root())
            logger.info("quota reconciled quota_used=%d limit=%d", used, self.config.max_staging_bytes)
        logger.info("recovery finished sessions=%d staging_dir=%s", recovered, self.storage.root)
        return recovered

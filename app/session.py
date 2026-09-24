"""会话管理：创建、分片暂存写入、断点恢复、最终提交、过期清理。

一致性约定：
- 同一会话的所有变更在会话排他锁内完成（锁 -> 读 manifest -> 判定 ->
  落盘 -> 更新 manifest -> 解锁），进程内多线程与多进程均安全；
- 分片写入：先校验序号/大小/校验和，再原子落盘，最后才更新 manifest。
  manifest 是"已确认进度"的唯一事实来源，崩溃后只依据 manifest 恢复；
- 重复提交同一序号：内容一致则幂等返回，内容冲突则以
  checksum_mismatch 拒绝，绝不覆盖已确认分片；
- 最终提交：缺片 -> missing_chunks；整文件校验不符 ->
  final_checksum_mismatch；成功后成品文件已落盘且 manifest 标记
  completed，重复提交只返回同一份结果；
- 清理与提交互斥：过期 open 会话被删除；completed 会话保留。
"""

from __future__ import annotations

import logging
import os
import secrets
import uuid
from collections.abc import Callable

from app.config import Settings
from app.errors import (
    ChecksumMismatch,
    ChunkIndexOutOfRange,
    ChunkSizeMismatch,
    FinalChecksumMismatch,
    MissingChunks,
    SessionExpired,
    SessionNotFound,
    SessionNotOpen,
    UnauthorizedSession,
)
from app.models import (
    ChunkState,
    ChunkUploadResult,
    CommitResult,
    CreateSessionRequest,
    SessionState,
    SESSION_ABORTED,
    SESSION_COMPLETED,
    SESSION_OPEN,
    now_ts,
)
from app.storage import DiskStore

logger = logging.getLogger("app.session")


class SessionManager:
    def __init__(
        self,
        settings: Settings,
        store: DiskStore | None = None,
        time_func: Callable[[], float] = now_ts,
    ) -> None:
        self.settings = settings
        self.store = store or DiskStore(settings.staging_dir)
        self._time = time_func
        self.store.ensure_layout()

    # ---- 生命周期 -------------------------------------------------------

    def create_session(self, req: CreateSessionRequest) -> SessionState:
        session_id = uuid.uuid4().hex
        owner_token = req.owner_token or secrets.token_urlsafe(24)
        now = self._time()
        ttl = req.ttl_seconds or self.settings.session_ttl_seconds
        total_chunks = len(req.chunk_checksums)
        state = SessionState(
            session_id=session_id,
            owner_token=owner_token,
            filename=os.path.basename(req.filename),
            total_size=req.total_size,
            chunk_size=req.chunk_size,
            total_chunks=total_chunks,
            chunk_checksums=list(req.chunk_checksums),
            checksum_algorithm=req.checksum_algorithm,
            final_checksum=req.final_checksum,
            status=SESSION_OPEN,
            created_at=now,
            updated_at=now,
            expires_at=now + ttl,
        )
        os.makedirs(self.store.chunks_dir(session_id), exist_ok=True)
        self.store.write_manifest_raw(session_id, state.to_dict())
        logger.info(
            "session=%s 动作=创建 分片数=%d 总大小=%d TTL=%ds 判定依据=新建会话已持久化",
            session_id,
            total_chunks,
            req.total_size,
            ttl,
        )
        return state

    def get_session(self, session_id: str) -> SessionState:
        return self._load(session_id)

    def abort_session(self, session_id: str, owner_token: str) -> SessionState:
        with self.store.session_lock(
            session_id, self.settings.file_lock_timeout_seconds
        ):
            state = self._load_locked(session_id)
            self._authorize(state, owner_token)
            if state.status == SESSION_OPEN:
                state.status = SESSION_ABORTED
                state.touch(self._time())
                self._save(state)
                logger.info(
                    "session=%s 动作=中止 判定依据=属主主动中止且会话原为open",
                    session_id,
                )
            else:
                logger.info(
                    "session=%s 动作=中止(忽略) 当前状态=%s 判定依据=会话非open保持原状态",
                    session_id,
                    state.status,
                )
            return state

    # ---- 分片写入 -------------------------------------------------------

    def upload_chunk(
        self,
        session_id: str,
        index: int,
        payload: bytes,
        owner_token: str,
        checksum: str | None = None,
    ) -> ChunkUploadResult:
        with self.store.session_lock(
            session_id, self.settings.file_lock_timeout_seconds
        ):
            state = self._load_locked(session_id)
            self._authorize(state, owner_token)
            self._ensure_active(state, self._time())
            self._ensure_open(state)

            # 1) 序号范围
            if not 0 <= index < state.total_chunks:
                logger.warning(
                    "session=%s 分片=%d 判定=拒绝 依据=序号越界(允许0..%d)",
                    session_id,
                    index,
                    state.total_chunks - 1,
                )
                raise ChunkIndexOutOfRange(
                    f"chunk index {index} out of range [0,{state.total_chunks})"
                )

            # 2) 幂等：已确认分片只接受完全一致的重传
            existing = state.chunks.get(index)
            actual_checksum = self._compute_checksum(
                state.checksum_algorithm, payload
            )
            provided = checksum or actual_checksum
            if existing is not None:
                if provided == existing.checksum and actual_checksum == existing.checksum:
                    logger.info(
                        "session=%s 分片=%d 判定=幂等接受 依据=已确认分片校验和一致 checksum=%s",
                        session_id,
                        index,
                        existing.checksum,
                    )
                    return ChunkUploadResult(
                        session_id=session_id,
                        index=index,
                        duplicate=True,
                        size=existing.size,
                        checksum=existing.checksum,
                        reason="already_committed_checksum_match",
                    )
                logger.warning(
                    "session=%s 分片=%d 判定=拒绝 依据=重复提交内容与已确认分片不一致(已存=%s 新提交=%s)",
                    session_id,
                    index,
                    existing.checksum,
                    actual_checksum,
                )
                raise ChecksumMismatch(
                    f"chunk {index} re-upload conflicts with committed chunk"
                )

            # 3) 大小校验（最后一片允许不足 chunk_size）；先于校验和判定，
            #    使失败原因更具体
            expected_size = _expected_size(state, index)
            if len(payload) != expected_size:
                logger.warning(
                    "session=%s 分片=%d 判定=拒绝 依据=分片大小不符(期望=%d 实际=%d)",
                    session_id,
                    index,
                    expected_size,
                    len(payload),
                )
                raise ChunkSizeMismatch(
                    f"chunk {index} size {len(payload)} != expected {expected_size}"
                )

            # 4) 客户端声明校验和必须与实算一致
            if checksum is not None and checksum != actual_checksum:
                logger.warning(
                    "session=%s 分片=%d 判定=拒绝 依据=声明校验和与实算不一致(声明=%s 实算=%s)",
                    session_id,
                    index,
                    checksum,
                    actual_checksum,
                )
                raise ChecksumMismatch(
                    f"declared checksum mismatch for chunk {index}"
                )

            # 5) 必须与创建会话时登记的期望校验和一致
            expected_checksum = state.chunk_checksums[index]
            if actual_checksum != expected_checksum:
                logger.warning(
                    "session=%s 分片=%d 判定=拒绝 依据=与会话登记校验和不一致(期望=%s 实算=%s)",
                    session_id,
                    index,
                    expected_checksum,
                    actual_checksum,
                )
                raise ChecksumMismatch(
                    f"chunk {index} checksum does not match session manifest"
                )

            # 6) 原子落盘（带有限重试）后才确认进 manifest
            self.store.write_chunk_atomic(
                session_id,
                index,
                payload,
                retry_attempts=self.settings.flush_retry_attempts,
                retry_backoff=self.settings.flush_retry_backoff_seconds,
            )
            state.chunks[index] = ChunkState(
                index=index,
                size=len(payload),
                checksum=actual_checksum,
                committed_at=self._time(),
            )
            state.touch(self._time())
            self._save(state)
            logger.info(
                "session=%s 分片=%d 判定=已确认 进度=%d/%d checksum=%s 依据=校验与大小通过且原子落盘",
                session_id,
                index,
                len(state.chunks),
                state.total_chunks,
                actual_checksum,
            )
            return ChunkUploadResult(
                session_id=session_id,
                index=index,
                duplicate=False,
                size=len(payload),
                checksum=actual_checksum,
                reason="stored",
            )

    # ---- 提交 -----------------------------------------------------------

    def commit_session(self, session_id: str, owner_token: str) -> CommitResult:
        with self.store.session_lock(
            session_id, self.settings.file_lock_timeout_seconds
        ):
            state = self._load_locked(session_id)
            self._authorize(state, owner_token)

            # 已完成 -> 永远返回同一份结果（提交幂等）
            if state.status == SESSION_COMPLETED:
                logger.info(
                    "session=%s 动作=提交 判定=幂等返回 依据=会话已completed result=%s",
                    session_id,
                    state.result_path,
                )
                return CommitResult(
                    session_id=session_id,
                    status=SESSION_COMPLETED,
                    result_path=state.result_path,
                    reason="already_completed_same_result",
                )

            # 已中止 -> 明确拒绝，区别于其他失败
            if state.status == SESSION_ABORTED:
                logger.warning(
                    "session=%s 动作=提交 判定=拒绝 依据=会话已aborted", session_id
                )
                raise SessionNotOpen("session is aborted")

            # 过期 open 会话不得提交（与清理竞争时给出确定结论）
            self._ensure_active(state, self._time())

            # 缺片：不改变状态，客户端补齐后可重试
            missing = state.missing_indexes
            if missing:
                logger.warning(
                    "session=%s 动作=提交 判定=拒绝 依据=缺失分片 missing=%s 已收=%s",
                    session_id,
                    missing,
                    state.received_indexes,
                )
                raise MissingChunks(f"missing chunks: {missing}")

            # 顺序拼装成品（临时文件 + 原子改名）
            result_path = self.store.assemble_result(
                session_id, state.filename, list(range(state.total_chunks))
            )

            # 整文件校验
            if state.final_checksum is not None:
                import hashlib

                digest = hashlib.new(state.checksum_algorithm)
                with open(result_path, "rb") as fh:
                    for block in iter(lambda: fh.read(1024 * 1024), b""):
                        digest.update(block)
                actual_full = digest.hexdigest()
                if actual_full != state.final_checksum:
                    logger.warning(
                        "session=%s 动作=提交 判定=拒绝 依据=整文件校验和不一致(期望=%s 实算=%s)",
                        session_id,
                        state.final_checksum,
                        actual_full,
                    )
                    raise FinalChecksumMismatch("assembled file checksum mismatch")

            state.status = SESSION_COMPLETED
            state.result_path = result_path
            state.touch(self._time())
            self._save(state)
            logger.info(
                "session=%s 动作=提交 判定=完成 result=%s 依据=%d片齐全且整体校验通过",
                session_id,
                result_path,
                state.total_chunks,
            )
            return CommitResult(
                session_id=session_id,
                status=SESSION_COMPLETED,
                result_path=result_path,
                reason="completed",
            )

    # ---- 恢复与清理 ------------------------------------------------------

    def recover(self) -> list[str]:
        """服务重启后扫描磁盘，以 manifest 为准核对分片，返回可恢复会话。

        规则：
        - completed：结果保留，直接可用；
        - open：manifest 中登记但磁盘缺失的分片从进度中剔除（这些分片
          从未走完"落盘+确认"，重新上传即可）；状态写回；
        - aborted：保持中止态。
        """
        recovered: list[str] = []
        for session_id in self.store.list_sessions():
            with self.store.session_lock(
                session_id, self.settings.file_lock_timeout_seconds
            ):
                raw = self.store.read_manifest_raw(session_id)
                if raw is None:
                    continue
                state = SessionState.from_dict(raw)
                if state.status != SESSION_OPEN:
                    logger.info(
                        "session=%s 恢复扫描 状态=%s 判定=保持原状 依据=非open会话不恢复",
                        session_id,
                        state.status,
                    )
                    continue
                dangling = [
                    idx
                    for idx in list(state.chunks.keys())
                    if not self.store.chunk_present(session_id, idx)
                ]
                for idx in dangling:
                    del state.chunks[idx]
                if dangling:
                    self._save(state)
                recovered.append(session_id)
                logger.info(
                    "session=%s 恢复扫描 判定=可恢复 已确认=%d/%d 剔除未落盘分片=%s 依据=manifest为进度事实来源",
                    session_id,
                    len(state.chunks),
                    state.total_chunks,
                    dangling,
                )
        return recovered

    def cleanup_expired(self, *, now: float | None = None) -> list[str]:
        """删除过期 open 会话；completed 结果保留；aborted 一并清理。

        返回被清理的会话 id。与提交/上传共享会话锁，竞争时结果确定：
        要么提交先拿锁并把状态推进到 completed（清理跳过），要么清理先
        拿锁并删除会话（随后的提交得到 session_not_found）。
        """
        current = now if now is not None else self._time()
        removed: list[str] = []
        for session_id in self.store.list_sessions():
            with self.store.session_lock(
                session_id, self.settings.file_lock_timeout_seconds
            ):
                raw = self.store.read_manifest_raw(session_id)
                if raw is None:
                    continue
                state = SessionState.from_dict(raw)
                if state.status == SESSION_COMPLETED:
                    continue
                if (
                    state.status == SESSION_ABORTED
                    or state.expires_at <= current
                ):
                    self.store.remove_session(session_id)
                    removed.append(session_id)
                    logger.info(
                        "session=%s 清理 判定=删除 状态=%s 依据=%s",
                        session_id,
                        state.status,
                        "已中止" if state.status == SESSION_ABORTED
                        else f"已过期(expires_at={state.expires_at} <= now={current})",
                    )
        return removed

    # ---- 内部辅助 -------------------------------------------------------

    def _load(self, session_id: str) -> SessionState:
        raw = self.store.read_manifest_raw(session_id)
        if raw is None:
            raise SessionNotFound(f"session {session_id} not found")
        return SessionState.from_dict(raw)

    def _load_locked(self, session_id: str) -> SessionState:
        """调用方必须已持有会话锁。"""
        return self._load(session_id)

    def _save(self, state: SessionState) -> None:
        self.store.write_manifest_raw(session_id=state.session_id, data=state.to_dict())

    @staticmethod
    def _authorize(state: SessionState, owner_token: str) -> None:
        if not owner_token or not secrets.compare_digest(
            str(owner_token), state.owner_token
        ):
            raise UnauthorizedSession("owner token mismatch")

    def _ensure_active(self, state: SessionState, now: float) -> None:
        if state.status == SESSION_OPEN and state.expires_at <= now:
            raise SessionExpired(
                f"session {state.session_id} expired at {state.expires_at}"
            )

    @staticmethod
    def _ensure_open(state: SessionState) -> None:
        if state.status != SESSION_OPEN:
            raise SessionNotOpen(f"session is {state.status}")

    @staticmethod
    def _compute_checksum(algorithm: str, payload: bytes) -> str:
        import hashlib

        return hashlib.new(algorithm, payload).hexdigest()


def _expected_size(state: SessionState, index: int) -> int:
    """登记总大小时按总大小反推；否则除最后一片外均为 chunk_size。"""
    if state.total_size > 0:
        start = index * state.chunk_size
        return max(0, min(state.chunk_size, state.total_size - start))
    if index == state.total_chunks - 1:
        return state.chunk_size
    return state.chunk_size

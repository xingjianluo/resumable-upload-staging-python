# 分片暂存与断点续传设计说明

本地磁盘模拟"远端共享存储"，为大文件上传提供分片暂存、断点续传、完整性校验、
并发提交一致性与过期清理能力。

## 1. 目录布局（共享存储）

```
staging_dir/
├── sessions/<session_id>/
│   ├── meta.json            # 会话元数据：属主、总分片数、已确认分片、状态、过期时间
│   └── chunks/<index>       # 已确认的分片数据（文件名即分片序号）
└── completed/
    └── <session_id>-<文件名> # 提交成功后拼装出的最终文件（全局唯一，按会话隔离）
```

- 分片文件与 `meta.json` 都采用 **临时文件 + fsync + `os.replace` 原子改名** 写入，
  并发写同一文件不会出现半写内容，崩溃后不会读到撕裂状态。
- 最终文件先以 `.part` 临时文件拼装并 fsync，再原子改名到 `completed/`，
  因此 `completed/` 下要么没有文件、要么是一份完整文件。

## 2. 暂存生命周期

会话状态机：

```
            put_chunk(幂等累加)                 全部到齐 + 整文件校验一致
 PENDING ───────────────────────► PENDING ───────────────────────────────► COMPLETED
    │  ▲                              │
    │  └── 缺片提交：仅拒绝，不改终态    ├── 整文件校验不一致 / 会话失败 ──► FAILED（终态）
    │                                  └── 超过 expires_at ──► 过期写入/提交被拒
    └── 过期且仍为 PENDING：后台清理或手动 cleanup_expired() 物理删除
```

- **PENDING**：会话创建后的初始状态。分片可乱序、可并发、可重复提交。
- **COMPLETED**：提交成功。重复提交/查询直接幂等返回同一结果，不重复拼装；
  已完成的会话不会被过期清理删除。
- **FAILED**：仅在整文件校验不一致等不可静默成功的情况下进入；`fail_reason` 落盘。
- **过期**：`now > expires_at` 的 PENDING 会话，写入与提交一律拒绝（`session_expired`），
  并由清理线程物理回收暂存空间。

### 分片确认（ack）语义

1. 校验 `index ∈ [0, total_chunks)`（否则 `chunk_out_of_range`）、大小上限（`chunk_too_large`）；
2. 用会话约定算法（默认 sha256）重算分片校验和，不一致拒绝（`checksum_mismatch`），**不落盘**；
3. 已确认过的同序号分片：
   - 校验和与大小一致 → **幂等**返回原确认记录（同一会话只有一份 ack）；
   - 校验和不一致 → 以"已确认事实"为准拒绝（`checksum_mismatch`/冲突），原数据不被覆盖；
4. 新分片：先原子写 `chunks/<index>`，再把 ChunkState 追加进 `meta.json` 原子落盘。
   只有进入 `meta.json` 的分片才算"已确认"。

## 3. 并发语义

- 每个 `session_id` 持有一把进程内 `RLock`，同一会话的写入与提交串行化：
  - 同一会话并发写同一分片 → 串行生效，重复写幂等，不互相覆盖、不丢失进度；
  - 同一会话并发提交 → 只有一次真正拼装，其余等待后幂等返回，**只产生一份结果文件**。
- 不同会话使用不同的锁与独立目录，写入完全隔离。
- 所有状态判断均以"锁内重新从磁盘加载的 `meta.json`"为权威，拒绝使用过期内存视图。
- 鉴权：每个会话记录 `owner_token`，写入/查询/提交必须携带相同的
  `X-Owner-Token`，否则以 `unauthorized`（HTTP 403）拒绝。

### 提交与清理并发的确定性

提交和清理都在同一把会话锁内做"过期判定 + 状态迁移"，因此二者并发时结论唯一：

- 清理先拿到锁且会话已过期 → 会话目录被删除，提交方得到 `session_expired`/`session_not_found`；
- 提交先拿到锁 → 要么过期拒绝，要么原子完成 COMPLETED；清理线程随后看到的是
  已完成会话，不再删除。

不会出现"清理删了一半、提交又写成功"或两份结果文件的情况。

## 4. 恢复语义（重启 / 崩溃）

- 进度真相只存在磁盘（`meta.json` + 分片文件），内存中没有唯一状态。
  服务重启后 `recover()` 扫描 `sessions/` 重建会话索引，**已确认分片不丢失**，
  客户端可凭原 `session_id` 继续上传缺失分片后提交。
- 崩溃窗口处理：分片数据已写入但 `meta.json` 更新前崩溃，会留下"孤儿分片"。
  恢复时发现某分片文件不在元数据的已确认集合中，会将其删除，等待客户端重传——
  避免把未经完整确认的数据误计入进度。
- 提交在拼装完成、整文件校验通过后才把状态改为 COMPLETED 并落盘；
  若整文件校验不一致，删除拼装产物并将会话置为 FAILED，绝不静默成功。

## 5. 完整性校验

- 分片级：每个分片请求头 `X-Chunk-Checksum` 携带该分片 sha256，服务端重算比对。
- 整文件级：创建会话时声明 `final_checksum`；提交时对拼装结果流式重算 sha256 比对。
  不一致返回 `final_checksum_mismatch`（HTTP 422），失败原因与缺片（`missing_chunks`，
  HTTP 409）可明确区分。

## 6. 配置项（`resumable_upload.config.StagingConfig`）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `staging_dir` | `staging/` | 暂存根目录（本地磁盘模拟共享存储） |
| `session_ttl_seconds` | `3600` | 会话有效期；过期的写入/提交被拒并可被清理 |
| `cleanup_interval_seconds` | `300` | 后台过期清理线程轮询周期 |
| `chunk_max_bytes` | `16 MiB` | 单分片大小上限，超限以 `chunk_too_large` 拒绝 |

## 7. HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /uploads/sessions` | 创建会话（body 含文件名、总大小、分片大小、整文件校验和；头 `X-Owner-Token`） |
| `GET  /uploads/sessions/{id}` | 查询进度：已收/缺失分片、状态、结果路径、失败原因 |
| `PUT  /uploads/sessions/{id}/chunks/{index}` | 上传/重传分片（原始 body；头 `X-Chunk-Checksum`、`X-Owner-Token`） |
| `POST /uploads/sessions/{id}/commit` | 最终提交，幂等 |

错误响应体：`{"detail": {"reason": <可区分原因码>, "message": ...}}`，
主要原因码与状态码：

| reason | HTTP | 场景 |
| --- | --- | --- |
| `session_not_found` | 404 | 会话不存在或已被清理 |
| `unauthorized` | 403 | owner token 不匹配 |
| `session_expired` | 410 | 会话超过有效期 |
| `session_finalized` | 409 | 对已完成会话的非法状态操作 |
| `session_failed` | 409 | 会话此前已因完整性问题失败 |
| `chunk_out_of_range` | 400 | 分片序号越界 |
| `chunk_too_large` | 413 | 分片超过大小上限 |
| `checksum_mismatch` | 422 | 分片校验和不一致 / 与已确认分片冲突 |
| `missing_chunks` | 409 | 提交时存在缺失分片（响应附 `missing_chunks`） |
| `final_checksum_mismatch` | 422 | 拼装结果与整文件校验和不一致 |

## 8. 验证方法

```bash
uv sync --group dev
uv run pytest                          # 全部单测（21 个）
uv run pytest -o log_cli=true          # 同时打印会话标识/分片序号/判定依据日志
uv run uvicorn main:app --reload       # 起服务，见 http://127.0.0.1:8000/docs
```

测试覆盖：

- `test_concurrent_writes_same_chunk_are_idempotent`：8 线程并发重复写全部分片，
  验证无覆盖、无丢失、重复 ack 幂等、提交内容正确；
- `test_concurrent_writes_different_sessions_are_isolated`：两个会话并发互斥写入，
  结果互不串扰；
- `test_duplicate_commit_produces_single_result`：5 线程并发提交，仅生成一份结果文件；
- `test_commit_and_cleanup_race_has_deterministic_outcome`：20 轮提交/清理竞态，
  结论只能是"唯一成功"或"过期拒绝"；
- `test_progress_survives_restart` / `test_resume_after_restart_over_http`：
  模拟中断与重启，已确认进度保留并可断点续传；
- `test_orphan_chunk_after_crash_is_discarded_on_recover`：崩溃窗口孤儿分片不被误确认；
- `test_unauthorized_writes_are_rejected` / `test_http_error_reasons_are_distinguishable`：
  越权拒绝与各类失败原因码可区分。

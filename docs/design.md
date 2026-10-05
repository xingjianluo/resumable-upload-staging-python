# 分片暂存与断点续传设计说明

本地磁盘模拟"远端共享存储"，为大文件上传提供分片暂存、断点续传、完整性校验、
并发提交一致性、多进程/多实例共享、会话续期保活、暂存容量约束、过期清理与
重启恢复能力。

## 1. 目录布局（共享存储）

```
staging_dir/
├── sessions/<session_id>/
│   ├── meta.json            # 会话元数据：属主、总分片数、已确认分片、状态、过期时间
│   └── chunks/<index>       # 已确认的分片数据（文件名即分片序号）
├── completed/
│   └── <session_id>-<文件名> # 提交成功后拼装出的最终文件（全局唯一，按会话隔离）
├── locks/
│   ├── sessions/<session_id>.lock  # 每个会话一把跨进程 flock 锁
│   └── quota.lock                  # 全局配额账本跨进程锁
└── quota/
    └── ledger.json          # 暂存总占用账本（已确认分片字节数），原子更新
```

- 分片文件、`meta.json` 与 `ledger.json` 都采用
  **临时文件 + fsync + `os.replace` 原子改名** 写入，并发写同一文件不会出现
  半写内容，崩溃后不会读到撕裂状态。
- 最终文件先以 `.part` 临时文件拼装并 fsync，再原子改名到 `completed/`，
  因此 `completed/` 下要么没有文件、要么是一份完整文件。

## 2. 多实例 / 多进程共享部署

多个 worker 进程、甚至多个实例（容器/主机）只要把 `staging_dir` 指向**同一个
共享目录**（网络盘、共享块存储等保证 flock 与原子 rename 语义的文件系统），
就可以对等对外服务，无需主节点：

- **跨进程互斥**：每个会话一把组合锁——进程内是线程锁，进程间是该会话锁文件
  上的排他 `fcntl.flock`。同一会话的分片写入、续期、提交、回收在所有进程间
  串行化；不同会话用不同锁文件，互不阻塞。
- **权威状态在磁盘**：所有判定都在锁内"重新加载 `meta.json` → 判定 → 原子
  写回"，不依赖任何进程内存视图，因此任意实例读到的已确认进度一致。
- **重启/换实例续传**：客户端拿原 `session_id` 向任意实例补传缺失分片即可；
  新实例 `recover()` 后以 `meta.json` 重建进度视图（含续期后的过期时间），
  并按磁盘实际占用重新校准配额账本。
- **单份结果**：跨进程并发提交同样被会话锁串行，只有一次真正拼装，其余
  幂等返回同一结果文件。

> 文件系统要求：支持 `fcntl.flock`（或等效的字节范围锁）与同目录
> `os.replace` 原子改名；例如 NFS v3/v4（需锁服务）、本地盘、共享块存储。
> 部署时把所有实例的 `StagingConfig.staging_dir` 配成同一挂载路径即可。

## 3. 暂存生命周期

会话状态机：

```
            put_chunk(幂等累加)                 全部到齐 + 整文件校验一致
 PENDING ───────────────────────► PENDING ───────────────────────────────► COMPLETED
    │  ▲                              │ │
    │  └── 缺片提交：仅拒绝，不改终态    │ ├── 整文件校验不一致 / 会话失败 ──► FAILED（终态，可被回收）
    │                                  │ └── 超过 expires_at ──► 过期写入/提交/续期被拒
    │                                  └── renew_session()：仅顺延 expires_at，不改任何进度
    └── 过期且仍为 PENDING，或 FAILED：后台清理 / cleanup_expired() 物理删除并返还配额
```

- **PENDING**：会话创建后的初始状态。分片可乱序、可并发、可重复提交。
- **COMPLETED**：提交成功。重复提交/查询直接幂等返回同一结果，不重复拼装；
  已完成的会话不会被过期清理删除；其暂存分片在提交成功后即被回收，
  最终文件保留在 `completed/`。
- **FAILED**：仅在整文件校验不一致等不可静默成功的情况下进入；`fail_reason`
  落盘。FAILED 会话不可再用，下一轮清理即物理回收并释放配额。
- **过期**：`now > expires_at` 的 PENDING 会话，写入、提交、续期一律拒绝
  （`session_expired`），并由清理线程物理回收暂存空间。

### 会话续期 / 保活

`renew_session(session_id, owner, extend_seconds)`：

1. 仅 **PENDING 且未过期** 的会话可续期；COMPLETED / FAILED / 过期分别以
   `session_finalized` / `session_failed` / `session_expired` 拒绝，属主不匹配
   仍以 `unauthorized` 拒绝；
2. 续期**只改 `expires_at`**，已确认分片、校验结论、状态全部保持不变；
3. 续期在跨进程会话锁内"读盘 → 判定 → 原子写盘"，与回收严格互斥，
   二者撞车时结论唯一：续期先成功则后续清理不会删它；回收先完成则续期得到
   明确的过期/不存在结论，不会出现"又被删又续期成功"；
4. 新的 `expires_at` 持久化在 `meta.json`，**重启后不丢**；
5. 顺延只会向后（不会缩短当前有效期），且不超过
   `created_at + max_session_ttl_seconds` 的总时长上限。

### 分片确认（ack）语义

1. 校验 `index ∈ [0, total_chunks)`（否则 `chunk_out_of_range`）、大小上限（`chunk_too_large`）；
2. 用会话约定算法（默认 sha256）重算分片校验和，不一致拒绝（`checksum_mismatch`），**不落盘、不占配额**；
3. 已确认过的同序号分片：
   - 校验和与大小一致 → **幂等**返回原确认记录（同一会话只有一份 ack，不重复计费）；
   - 校验和不一致 → 以"已确认事实"为准拒绝（`checksum_mismatch`/冲突），原数据不被覆盖；
4. 新分片：在全局配额锁内**先记账预留**本分片字节，再原子写 `chunks/<index>`、
   把 ChunkState 追加进 `meta.json` 原子落盘；写盘失败则回退预留。
   只有进入 `meta.json` 的分片才算"已确认"。

## 4. 暂存容量约束与回收

- `max_staging_bytes` 配置整个暂存目录允许占用的**暂存字节上限**
  （`0` 表示不限制）。计费口径只统计 `sessions/` 下**已确认分片**字节数；
  `completed/` 最终产物与 `meta.json`、锁文件等元数据不计入暂存配额。
- 两个强制点，拒绝原因统一为 **`quota_exceeded`（HTTP 507）**，
  与会话过期（410）、越权（403）等明确区分，响应体还带
  `{used, limit, requested}`：
  1. **新建会话预检**：当前暂存已被占满时直接拒绝；声明大小本身不提前预留，
     容量按分片确认逐片占用（允许"先建会话、等空间释放后再传"）；
  2. **写分片强制**：全局配额锁内检查 `已占用 + 本片字节 > 上限` 即拒绝，
     因此多进程/多实例并发写入时占用也不会明显超过上限（计费临界区在
     flock 内串行）。被拒时不写盘、不记账，已有分片完好。
- **空间释放与复用**：
  - 提交成功 → COMPLETED 元数据发布后删除暂存分片并按字节返还账本；
  - 过期 PENDING 或 FAILED 会话被清理 → 物理删除目录并返还账本；
  - 返还出来的空间立刻可被后续上传的分片使用。
- **回收不误伤**：回收只对"已过期的 PENDING"与"FAILED"会话动手，
  且判定与删除都在该会话的跨进程锁内；仍在续期、仍活跃的会话绝不会被当成
  垃圾清掉，COMPLETED 会话永久保留。
- **崩溃后校准**：`recover()` 清除"数据写了但 meta 没记"的孤儿分片后，
  按 `meta.json` 的已确认分片重建账本，预留了却没写成的字节不会再占额。

## 5. 并发语义

- 每个 `session_id` 持有一把跨进程组合锁，同一会话的写入、续期与提交串行化：
  - 同一会话并发写同一分片（即使来自不同进程/实例）→ 串行生效，重复写幂等，
    不互相覆盖、不丢失进度；
  - 同一会话并发提交 → 只有一次真正拼装，其余等待后幂等返回，**只产生一份结果文件**。
- 不同会话使用不同的锁文件与独立目录，写入完全隔离。
- 配额账本的读改写在独立的全局配额锁内完成，与具体会话锁不嵌套形成环
  （固定加锁顺序：会话锁 → 配额锁）。
- 鉴权：每个会话记录 `owner_token`，写入/查询/续期/提交必须携带相同的
  `X-Owner-Token`，否则以 `unauthorized`（HTTP 403）拒绝。

### 提交/续期与清理并发的确定性

提交、续期和清理都在同一把会话锁内做"有效期判定 + 状态迁移"，因此并发时
结论唯一：

- 清理先拿到锁且会话已过期（或已 FAILED）→ 会话目录被删除并返还配额，
  提交方/续期方得到 `session_expired`/`session_not_found`；
- 提交/续期先拿到锁 → 要么过期拒绝，要么原子完成 COMPLETED / 顺延有效期；
  清理线程随后看到的是已完成会话或未到期会话，不再删除。

不会出现"清理删了一半、操作又写成功"或两份结果文件的情况。

## 6. 恢复语义（重启 / 崩溃）

- 进度真相只存在磁盘（`meta.json` + 分片文件），内存中没有唯一状态。
  服务重启后 `recover()` 扫描 `sessions/` 重建会话锁视图，**已确认分片与
  续期后的过期时间都不丢失**，客户端可凭原 `session_id` 向任意实例继续上传
  缺失分片后提交。
- 崩溃窗口处理：分片数据已写入但 `meta.json` 更新前崩溃，会留下"孤儿分片"。
  恢复时发现某分片文件不在元数据的已确认集合中，会将其删除，等待客户端重传
  ——避免把未经完整确认的数据误计入进度；随后按元数据重建配额账本。
- 提交在拼装完成、整文件校验通过后才把状态改为 COMPLETED 并落盘；
  若整文件校验不一致，删除拼装产物并将会话置为 FAILED，绝不静默成功。

## 7. 完整性校验

- 分片级：每个分片请求头 `X-Chunk-Checksum` 携带该分片 sha256，服务端重算比对。
- 整文件级：创建会话时声明 `final_checksum`；提交时对拼装结果流式重算 sha256 比对。
  不一致返回 `final_checksum_mismatch`（HTTP 422），失败原因与缺片（`missing_chunks`，
  HTTP 409）可明确区分。

## 8. 配置项（`resumable_upload.config.StagingConfig`）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `staging_dir` | `staging/` | 暂存根目录；多实例部署时所有实例指向同一共享目录 |
| `session_ttl_seconds` | `3600` | 会话有效期；过期的写入/提交/续期被拒并可被清理 |
| `cleanup_interval_seconds` | `300` | 后台过期清理线程轮询周期 |
| `chunk_max_bytes` | `16 MiB` | 单分片大小上限，超限以 `chunk_too_large` 拒绝 |
| `max_staging_bytes` | `0`（不限） | 暂存目录已确认分片总占用上限；超限以 `quota_exceeded`(507) 拒绝 |
| `max_session_ttl_seconds` | `7 天` | 续期后允许达到的有效期上限（相对创建时刻）；`0` 为不限 |

## 9. HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /uploads/sessions` | 创建会话（body 含文件名、总大小、分片大小、整文件校验和；头 `X-Owner-Token`） |
| `GET  /uploads/sessions/{id}` | 查询进度：已收/缺失分片、状态、结果路径、失败原因、过期时间 |
| `PUT  /uploads/sessions/{id}/chunks/{index}` | 上传/重传分片（原始 body；头 `X-Chunk-Checksum`、`X-Owner-Token`） |
| `POST /uploads/sessions/{id}/commit` | 最终提交，幂等 |
| `POST /uploads/sessions/{id}/renew` | 续期保活（body `{"extend_seconds": 可选}`，省略按默认 TTL 顺延） |

错误响应体：`{"detail": {"reason": <可区分原因码>, "message": ...}}`，
主要原因码与状态码：

| reason | HTTP | 场景 |
| --- | --- | --- |
| `session_not_found` | 404 | 会话不存在或已被清理 |
| `unauthorized` | 403 | owner token 不匹配 |
| `session_expired` | 410 | 会话超过有效期（写入/提交/续期） |
| `session_finalized` | 409 | 对已完成会话的非法状态操作（含续期） |
| `session_failed` | 409 | 会话此前已因完整性问题失败（含续期） |
| `chunk_out_of_range` | 400 | 分片序号越界 |
| `chunk_too_large` | 413 | 分片超过大小上限 |
| `checksum_mismatch` | 422 | 分片校验和不一致 / 与已确认分片冲突 |
| `missing_chunks` | 409 | 提交时存在缺失分片（响应附 `missing_chunks`） |
| `final_checksum_mismatch` | 422 | 拼装结果与整文件校验和不一致 |
| `quota_exceeded` | 507 | 暂存容量不足（响应附 `quota:{used,limit,requested}`） |

## 10. 验证方法

```bash
uv sync --group dev
uv run pytest                          # 全部单测；默认即打印会话/分片/判定日志
uv run uvicorn main:app --reload       # 起服务，见 http://127.0.0.1:8000/docs
```

默认执行 `pytest` 即通过 `log_cli` 输出 INFO 日志，可直接看出涉及的会话 id、
分片序号与判定结论（如 `chunk confirmed session=.. chunk=2 progress=3/4`、
`chunk rejected .. reason=quota_exceeded`、`session renewed .. new_expires_at=..`、
`session reaped .. freed_bytes=..`），无需再加参数。

测试覆盖（`tests/`）：

- `test_concurrent_writes_same_chunk_are_idempotent`：8 线程并发重复写全部分片，
  验证无覆盖、无丢失、重复 ack 幂等、提交内容正确；
- `test_concurrent_writes_different_sessions_are_isolated`：两个会话并发互斥写入，
  结果互不串扰；
- `test_duplicate_commit_produces_single_result`：5 线程并发提交，仅生成一份结果文件；
- `test_commit_and_cleanup_race_has_deterministic_outcome`：20 轮提交/清理竞态，
  结论只能是"唯一成功"或"过期拒绝"；
- `tests/test_multiprocess.py`：用 spawn 起**真实多进程**指向同一暂存目录，
  - `test_multiple_processes_write_same_session_without_corruption`：
    4 进程重叠写同一会话分片，无覆盖、无丢进度、只出一份正确结果；
  - `test_progress_consistent_in_fresh_process_after_restart`：
    全新进程 recover 后进度一致并补传提交；
  - `test_cross_process_commit_and_cleanup_race_is_unique`：
    20 轮跨进程提交/回收竞态，结论唯一、结果文件唯一；
  - `test_renew_vs_cleanup_race_in_separate_processes`：
    续期进程与回收进程竞争，续期成功则保留且再清理不删，回收成功则续期得明确拒绝；
  - `test_concurrent_processes_respect_quota_cap`：
    4 进程并发写，账本占用不超过上限，只有容量内的分片被确认；
- `tests/test_renewal.py`：续期顺延且进度不变、持续续期不被回收、停止续期后
  按时拒绝并回收、终态/越权/过期续期被拒、续期结果重启不丢、TTL 上限、
  30 轮线程级续期/回收竞态；
- `tests/test_quota.py`：容量打满创建/写分片被拒且原因可区分、已确认分片不被
  破坏、过期/失败/提交成功释放空间后可复用、并发不超限、重启按盘校准账本；
- `test_progress_survives_restart` / `test_resume_after_restart_over_http`：
  模拟中断与重启，已确认进度保留并可断点续传；
- `test_orphan_chunk_after_crash_is_discarded_on_recover`：崩溃窗口孤儿分片不被误确认；
- `test_unauthorized_writes_are_rejected` / `test_http_error_reasons_are_distinguishable`：
  越权拒绝与各类失败原因码可区分。

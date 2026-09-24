# 大文件分片暂存与断点续传服务（本地磁盘模拟共享存储）

基于 FastAPI 实现的分片上传暂存服务。以**本地磁盘 + 文件锁**模拟远端共享
存储，使并发分片写入、会话中断恢复、并发提交与过期清理的最终状态可预期。

## 1. 能力概览

- 分片会话创建与暂存：客户端预先登记总分片数与每片校验和；服务端逐片
  校验后原子落盘。
- 同一会话并发写入互不覆盖/丢失；不同会话目录与锁完全隔离。
- 写入鉴权：每会话签发 `owner_token`，未携带或不匹配一律拒绝；过期会话、
  已中止/已完成会话写入均被拒绝，且**每种失败原因有独立错误码**。
- 断点续传：已确认进度持久化在 `manifest.json`，服务重启后不丢失；
  可通过状态接口查询缺失分片并只补传缺失部分。
- 分片幂等：重复提交已确认分片，内容一致 → 幂等成功；内容冲突 → 拒绝，
  绝不覆盖已确认数据。
- 最终提交：缺片、整文件校验不一致均无法静默成功，失败原因可区分；
  同一会话重复提交只返回同一份成品文件。
- 过期清理与提交互斥：无论谁先执行，最终状态唯一确定（见第 4 节）。

## 2. 暂存生命周期

```
                 POST /sessions
  [不存在] ───────────────────────▶ open（暂存中，可写/可查/可中止）
                                      │
                 逐片 PUT .../chunks  │（已确认进度随时持久化）
                                      │
                 POST .../commit      │
                 ┌────────────────────┼─────────────────────┐
                 ▼                    ▼                     ▼
           缺片/校验失败          全部成功                POST .../abort
        （保持 open，可补齐      completed               aborted
          后重新提交）        （成品落盘，结果保留）     （拒绝后续写入/提交）
                                      │
            open 超过 TTL 或 aborted：cleanup 时删除整个会话目录
            completed：默认永久保留（清理器跳过）
```

### 磁盘布局

```
<STAGING_DIR>/
├── sessions/<session_id>/
│   ├── manifest.json         # 会话元数据 + 已确认分片进度（事实来源）
│   └── chunks/00000000.part  # 分片文件（原子 rename 落盘）
├── completed/<session_id>/<filename>  # 提交成功后的成品文件
├── locks/<session_id>.lock   # fcntl 排他锁文件（不随会话删除）
└── tmp/                      # 预留临时目录
```

## 3. 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /sessions` | 创建会话，body：`filename,total_size,chunk_size,chunk_checksums[,checksum_algorithm,final_checksum,ttl_seconds,owner_token]`；返回含 `session_id`、`owner_token` |
| `PUT /sessions/{id}/chunks/{index}` | 上传分片（原始字节为 body）；请求头 `X-Owner-Token` 必填，`X-Chunk-Checksum` 可选 |
| `GET /sessions/{id}` | 查询状态，含 `status`、已收分片与 `missing_indexes` |
| `POST /sessions/{id}/commit` | 最终提交（body/header 提供 token），顺序拼装并做整文件校验 |
| `POST /sessions/{id}/abort` | 主动中止会话 |
| `POST /admin/recover` | 扫描磁盘做重启恢复核对，返回可恢复会话 id |
| `POST /admin/cleanup?now=<ts>` | 触发过期清理（`now` 便于测试注入） |

### 错误码（判定依据）

| error_code | HTTP | 含义 |
| --- | --- | --- |
| `unauthorized` | 403 | 未携带/错误的 owner_token |
| `session_not_found` | 404 | 会话不存在或已被清理 |
| `session_expired` | 410 | 会话超过 TTL |
| `session_not_open` | 409 | 会话已 completed/aborted，拒绝写入或提交 |
| `chunk_index_out_of_range` | 400 | 分片序号越界 |
| `chunk_size_mismatch` | 400 | 分片大小与登记不符 |
| `checksum_mismatch` | 422 | 分片校验和不一致（含冲突重传） |
| `missing_chunks` | 422 | 提交时存在缺失分片（会话保持 open，可补齐重试） |
| `final_checksum_mismatch` | 422 | 拼装后整文件校验和与 `final_checksum` 不符 |

幂等语义：重复上传已确认且内容一致的分片返回
`reason=already_committed_checksum_match`（`duplicate=true`）；重复提交已
完成会话返回原 `result_path`（`reason=already_completed_same_result`）。

## 4. 并发与恢复语义

### 一致性机制

1. **每会话一把排他锁**（`fcntl.flock`，锁文件独立存放）。同一会话的
   上传/提交/中止/清理/恢复全程串行：`加锁 → 读 manifest → 判定 →
   落盘 → 更新 manifest → 解锁`；不同会话使用不同锁文件，天然并发。
2. **原子落盘**：分片与 manifest 均写同目录临时文件 → `fsync` →
   `os.replace` 原子改名 → fsync 父目录。崩溃时只会看到旧版或新版，
   不存在半截 manifest / 分片。
3. **先落盘后确认**：只有分片原子写入成功后才更新 manifest。manifest
   是“已确认进度”的唯一事实来源。
4. **重启恢复**（`POST /admin/recover`）：open 会话中 manifest 已登记但
   磁盘缺失的分片视为从未确认，从进度中剔除后写回 manifest，客户端重新
   上传这些分片即可；completed 会话结果直接可用，重复提交仍幂等。

### 并发提交 × 过期清理

两者竞争同一把会话锁，结果只有两种且均可判定：

- **提交先获锁**：会话在锁内推进到 `completed`，清理随后跳过
  （completed 永久保留）；
- **清理先获锁**：open 会话已过期，整个会话目录被删除，随后的提交得到
  `session_not_found`（清理发生前的过期提交得 `session_expired`）。

并发重复提交时，锁保证只有一次真正拼装，所有调用拿到同一个
`result_path`。

## 5. 配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `STAGING_DIR` | `staging_data` | 暂存根目录 |
| `SESSION_TTL_SECONDS` | `3600` | 会话默认有效期（秒，自创建起算） |
| `CLEANUP_INTERVAL_SECONDS` | `300` | 后台清理扫描间隔（仅 `enable_cleanup=True` 启动时生效） |
| `LOCK_TIMEOUT_SECONDS` | `10.0` | 获取会话锁的等待上限 |
| `FLUSH_RETRY_ATTEMPTS` | `3` | 分片落盘重试次数 |
| `FLUSH_RETRY_BACKOFF_SECONDS` | `0.05` | 落盘重试退避基数 |

## 6. 运行与验证

```bash
uv sync                                  # 安装依赖（含 pytest/httpx）
uv run pytest                            # 全部 32 项单测，日志打印
                                         # 会话标识/分片序号/判定依据
uv run uvicorn main:app --reload         # 启动服务
```

快速手工验证：

```bash
curl -X POST localhost:8000/sessions -H 'Content-Type: application/json' -d '{
  "filename":"demo.bin","total_size":11,"chunk_size":4,
  "chunk_checksums":["<每片sha256...>"],
  "final_checksum":"<整文件sha256>"}'
# 用返回的 session_id / owner_token 逐片上传：
curl -X PUT localhost:8000/sessions/$SID/chunks/0 \
  -H "X-Owner-Token: $TOKEN" --data-binary @chunk0
curl localhost:8000/sessions/$SID          # 查看 missing_indexes
curl -X POST localhost:8000/sessions/$SID/commit \
  -H 'Content-Type: application/json' -d "{\"owner_token\":\"$TOKEN\"}"
```

测试覆盖（`tests/`）：

- `test_session_lifecycle.py`：完整生命周期、幂等、冲突重传、缺片/整文件
  校验失败、越权、过期、完成后写入；
- `test_concurrency.py`：同会话并发写不同分片无丢失/无覆盖、并发重复提交
  幂等、会话间隔离、并发提交仅一份结果、提交×清理竞争确定性、清理保留
  规则；
- `test_recovery.py`：重启后进度保留与断点补齐、重启后幂等、未落盘分片
  恢复剔除、completed 结果跨重启可用；
- `test_api.py`：各错误码的 HTTP 映射、幂等接口行为、HTTP 并发提交。

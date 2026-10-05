# resumable-upload-staging-python

面向大文件的分片暂存与断点续传服务：以本地磁盘模拟远端共享存储，支持并发分片
写入、多进程/多实例共享同一暂存目录、会话隔离与鉴权、幂等重传、完整性校验、
会话续期保活、暂存容量上限与回收、并发提交去重、过期清理与重启恢复。

## 快速开始

```bash
uv sync --group dev
uv run uvicorn main:app --reload        # 打开 http://127.0.0.1:8000/docs
uv run pytest                            # 运行全部测试（默认即打印会话/分片/判定日志）
```

## 使用流程

```bash
# 1. 创建会话（owner 通过 X-Owner-Token 标识）
curl -s -X POST localhost:8000/uploads/sessions \
  -H 'X-Owner-Token: alice' -H 'Content-Type: application/json' \
  -d '{"filename":"big.bin","total_size":12345,"chunk_size":1048576,
       "final_checksum":"<整文件 sha256>"}'

# 2. 并发/乱序/重复上传分片（幂等）
curl -s -X PUT localhost:8000/uploads/sessions/<sid>/chunks/0 \
  -H 'X-Owner-Token: alice' -H 'X-Chunk-Checksum: <分片 sha256>' \
  --data-binary @chunk0

# 3. 查询进度（已收/缺失分片）
curl -s localhost:8000/uploads/sessions/<sid> -H 'X-Owner-Token: alice'

# 3.5 大文件传很久时，随时给会话续期保活（只顺延有效期，进度不变）
curl -s -X POST localhost:8000/uploads/sessions/<sid>/renew \
  -H 'X-Owner-Token: alice' -H 'Content-Type: application/json' \
  -d '{"extend_seconds":3600}'

# 4. 最终提交（重复提交幂等，只生成一份结果）
curl -s -X POST localhost:8000/uploads/sessions/<sid>/commit \
  -H 'X-Owner-Token: alice'
```

中断后使用原 `session_id` 继续上传缺失分片即可；服务重启后已确认进度（含续期后
的有效期）不丢失。容量不足返回 HTTP 507 `quota_exceeded`（响应体带
`used/limit/requested`），与会话过期（410）、越权（403）明确区分。

## 多实例共享部署

把所有 worker / 实例的 `staging_dir` 指向**同一个共享目录**（需支持 `flock` 与
同目录原子 rename，如共享块存储/NFS）即可对等扩容，无需主节点：

```python
StagingConfig(
    staging_dir="/mnt/shared/staging",   # 所有实例共用
    session_ttl_seconds=3600,
    max_staging_bytes=500 * 1024 ** 3,   # 整个暂存目录已确认分片总占用上限
    max_session_ttl_seconds=7 * 24 * 3600,
)
```

- 每个会话在共享目录有一把 flock 锁文件，跨进程的同会话写/续期/提交/回收互斥；
- 配额账本 `quota/ledger.json` 在全局配额锁内原子更新，并发写入不会明显超上限；
- 过期 PENDING 与 FAILED 会话由任意实例的后台清理线程回收并释放空间，
  仍在续期/活跃的会话与 COMPLETED 会话不会被误删；提交成功后暂存分片立即
  释放、空间可被后续上传复用。

## 代码结构

- `resumable_upload/models.py`：会话/分片状态模型与 JSON 序列化
- `resumable_upload/storage.py`：磁盘存储层（原子写、拼装、暂存分片回收、清理）
- `resumable_upload/locking.py`：跨进程组合锁（进程内线程锁 + 共享目录 flock）
- `resumable_upload/quota.py`：暂存容量账本（原子记账、预留/回退、重启校准）
- `resumable_upload/manager.py`：核心服务（会话锁、确认语义、续期、配额强制、提交、清理、恢复）
- `resumable_upload/api.py` / `app.py`：HTTP 接口与应用装配
- `tests/`：含真实多进程共享目录（`test_multiprocess.py`）、续期竞态
  （`test_renewal.py`）、容量打满与空间复用（`test_quota.py`）等测试
- `docs/design.md`：多实例部署、生命周期、并发/续期/容量/恢复语义、配置项与验证方法

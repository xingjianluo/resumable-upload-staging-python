# resumable-upload-staging-python

面向大文件的分片暂存与断点续传服务：以本地磁盘模拟远端共享存储，支持并发分片写入、
会话隔离与鉴权、幂等重传、完整性校验、并发提交去重、过期清理与重启恢复，
并支持多进程/多实例共享部署、会话续期保活与暂存总量约束。

## 快速开始

```bash
uv sync --group dev
uv run uvicorn main:app --reload        # 打开 http://127.0.0.1:8000/docs
uv run pytest                            # 运行全部测试（默认输出会话/分片/判定日志）
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

# 3.5 大文件传得久：定期续期保活，只延长不缩短
curl -s -X POST localhost:8000/uploads/sessions/<sid>/renew \
  -H 'X-Owner-Token: alice' -H 'Content-Type: application/json' \
  -d '{"extend_seconds": 3600}'

# 4. 最终提交（重复提交幂等，只生成一份结果）
curl -s -X POST localhost:8000/uploads/sessions/<sid>/commit \
  -H 'X-Owner-Token: alice'
```

中断后使用原 `session_id` 继续上传缺失分片即可；服务重启后已确认进度不丢失。

## 多实例共享部署

多个进程/实例只需把 `StagingConfig.staging_dir` 指向**同一个目录**（共享文件系统），
各自创建 `UploadManager` 并运行后台清理线程即可：

- 同一会话的写分片、提交、续期、过期回收由"进程内锁 + 会话级跨进程文件锁
  （fcntl.flock）"双重串行化：分片不互相覆盖、进度不丢、提交只产生一份结果；
- 所有进度真相在磁盘 `meta.json`，任一实例重启后各实例看到的已确认进度一致，
  客户端拿原会话找任意实例补传即可；
- 容量检查与写入在全局配额锁内完成，多实例并发不会明显超过 `max_staging_bytes`。

## 容量与回收

- `max_staging_bytes`：暂存目录总量上限（默认不限）。计入配额的是已确认分片 +
  已完成结果文件；空间不足时新建会话/写分片以 `quota_exceeded`（HTTP 507）拒绝，
  与会话过期（410）、越权（403）可区分，且不会损坏已确认数据。
- 过期 PENDING/FAILED 会话由清理线程回收，释放的空间立即可被后续上传复用；
  提交成功后分片副本即删除，只保留结果文件；持续续期的会话不会被回收。

## 代码结构

- `resumable_upload/models.py`：会话/分片状态模型与 JSON 序列化
- `resumable_upload/storage.py`：磁盘存储层（原子写、拼装、清理、用量统计）
- `resumable_upload/locks.py`：基于 fcntl.flock 的跨进程文件锁
- `resumable_upload/manager.py`：核心服务（会话锁、确认语义、续期、配额、提交、清理、恢复）
- `resumable_upload/api.py` / `app.py`：HTTP 接口与应用装配
- `tests/`：并发、多进程、续期、容量、恢复、幂等、越权、提交一致性测试
- `docs/design.md`：暂存生命周期、并发与恢复语义、续期规则、容量配置与验证方法

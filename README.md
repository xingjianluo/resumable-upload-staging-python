# resumable-upload-staging-python

面向大文件的分片暂存与断点续传服务：以本地磁盘模拟远端共享存储，支持并发分片写入、
会话隔离与鉴权、幂等重传、完整性校验、并发提交去重、过期清理与重启恢复。

## 快速开始

```bash
uv sync --group dev
uv run uvicorn main:app --reload        # 打开 http://127.0.0.1:8000/docs
uv run pytest                            # 运行全部测试
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

# 4. 最终提交（重复提交幂等，只生成一份结果）
curl -s -X POST localhost:8000/uploads/sessions/<sid>/commit \
  -H 'X-Owner-Token: alice'
```

中断后使用原 `session_id` 继续上传缺失分片即可；服务重启后已确认进度不丢失。

## 代码结构

- `resumable_upload/models.py`：会话/分片状态模型与 JSON 序列化
- `resumable_upload/storage.py`：磁盘存储层（原子写、拼装、清理）
- `resumable_upload/manager.py`：核心服务（会话锁、确认语义、提交、清理、恢复）
- `resumable_upload/api.py` / `app.py`：HTTP 接口与应用装配
- `tests/`：并发、恢复、幂等、越权、提交一致性测试
- `docs/design.md`：暂存生命周期、并发与恢复语义、配置项与验证方法

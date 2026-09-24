"""HTTP API 层测试：错误码/状态码映射、幂等语义、越权拒绝、提交结论。"""

from __future__ import annotations

import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("tests.api")

CHUNK_SIZE = 8


def _create(client, payload: bytes, ttl: int = 3600):
    chunks = [payload[i : i + CHUNK_SIZE] for i in range(0, len(payload), CHUNK_SIZE)]
    resp = client.post(
        "/sessions",
        json={
            "filename": "api.bin",
            "total_size": len(payload),
            "chunk_size": CHUNK_SIZE,
            "chunk_checksums": [hashlib.sha256(c).hexdigest() for c in chunks],
            "final_checksum": hashlib.sha256(payload).hexdigest(),
            "ttl_seconds": ttl,
        },
    )
    assert resp.status_code == 201
    body = resp.json()
    return body["session_id"], body["owner_token"], chunks


def _upload(client, sid, token, index, data):
    return client.put(
        f"/sessions/{sid}/chunks/{index}",
        content=data,
        headers={"X-Owner-Token": token},
    )


def test_create_upload_commit_flow(client):
    payload = b"0123456789abcdef0123456789"
    sid, token, chunks = _create(client, payload)
    for i, chunk in enumerate(chunks):
        resp = _upload(client, sid, token, i, chunk)
        assert resp.status_code == 200
        assert resp.json()["duplicate"] is False
        logger.info("会话=%s 分片=%d 判定=HTTP200已确认", sid, i)

    commit = client.post(f"/sessions/{sid}/commit", json={"owner_token": token})
    assert commit.status_code == 200
    assert commit.json()["status"] == "completed"
    logger.info("会话=%s 判定=提交成功 reason=%s", sid, commit.json()["reason"])


def test_missing_token_rejected_401(client):
    payload = b"0123456789abcdef"
    sid, token, chunks = _create(client, payload)
    resp = client.put(f"/sessions/{sid}/chunks/0", content=chunks[0])
    assert resp.status_code == 401
    logger.info("会话=%s 分片=0 判定=缺token拒绝401", sid)


def test_wrong_token_rejected_403(client):
    payload = b"0123456789abcdef"
    sid, token, chunks = _create(client, payload)
    resp = _upload(client, sid, "wrong-token", 0, chunks[0])
    assert resp.status_code == 403
    assert resp.json()["error_code"] == "unauthorized"
    logger.info("会话=%s 分片=0 判定=越权拒绝403 依据=unauthorized", sid)


def test_unknown_session_404(client):
    resp = client.get("/sessions/does-not-exist")
    assert resp.status_code == 404
    assert resp.json()["error_code"] == "session_not_found"
    logger.info("会话=does-not-exist 判定=404 依据=session_not_found")


def test_duplicate_upload_idempotent_200(client):
    payload = b"0123456789abcdef"
    sid, token, chunks = _create(client, payload)
    assert _upload(client, sid, token, 0, chunks[0]).status_code == 200
    resp = _upload(client, sid, token, 0, chunks[0])
    assert resp.status_code == 200
    assert resp.json()["duplicate"] is True
    assert resp.json()["reason"] == "already_committed_checksum_match"
    logger.info("会话=%s 分片=0 判定=重复提交幂等200 依据=%s", sid, resp.json()["reason"])


def test_conflicting_reupload_422(client):
    payload = b"0123456789abcdef"
    sid, token, chunks = _create(client, payload)
    _upload(client, sid, token, 0, chunks[0])
    resp = _upload(client, sid, token, 0, b"XXXXXXXX")
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "checksum_mismatch"
    logger.info("会话=%s 分片=0 判定=冲突重传422 依据=checksum_mismatch", sid)


def test_commit_with_missing_chunks_422(client):
    payload = b"0123456789abcdef0123456789"  # 4 片
    sid, token, chunks = _create(client, payload)
    _upload(client, sid, token, 0, chunks[0])
    resp = client.post(f"/sessions/{sid}/commit", json={"owner_token": token})
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "missing_chunks"
    logger.info("会话=%s 判定=缺片提交422 依据=missing_chunks", sid)


def test_expired_session_410(client):
    payload = b"0123456789abcdef"
    sid, token, chunks = _create(client, payload, ttl=1)
    import time

    time.sleep(1.05)
    resp = _upload(client, sid, token, 0, chunks[0])
    assert resp.status_code == 410
    assert resp.json()["error_code"] == "session_expired"
    logger.info("会话=%s 分片=0 判定=过期拒绝410 依据=session_expired", sid)


def test_write_after_commit_409(client):
    payload = b"0123456789abcdef"
    sid, token, chunks = _create(client, payload)
    for i, chunk in enumerate(chunks):
        _upload(client, sid, token, i, chunk)
    client.post(f"/sessions/{sid}/commit", json={"owner_token": token})
    resp = _upload(client, sid, token, 0, chunks[0])
    assert resp.status_code == 409
    assert resp.json()["error_code"] == "session_not_open"
    logger.info("会话=%s 分片=0 判定=完成后写入409 依据=session_not_open", sid)


def test_concurrent_http_commits_single_result(client):
    payload = b"0123456789abcdef0123456789"
    sid, token, chunks = _create(client, payload)
    for i, chunk in enumerate(chunks):
        _upload(client, sid, token, i, chunk)

    def do_commit():
        return client.post(f"/sessions/{sid}/commit", json={"owner_token": token})

    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(lambda _: do_commit(), range(6)))

    assert all(r.status_code == 200 for r in responses)
    paths = {r.json()["result_path"] for r in responses}
    assert len(paths) == 1
    logger.info(
        "会话=%s 判定=HTTP并发提交结果唯一 依据=result_path=%s", sid, paths.pop()
    )


def test_status_endpoint_reports_missing(client):
    payload = b"0123456789abcdef0123456789"
    sid, token, chunks = _create(client, payload)
    _upload(client, sid, token, 0, chunks[0])
    resp = client.get(f"/sessions/{sid}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["missing_indexes"] == [1, 2, 3]
    logger.info("会话=%s 判定=状态接口报告缺失分片%s", sid, body["missing_indexes"])

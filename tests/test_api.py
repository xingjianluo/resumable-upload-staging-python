"""HTTP 接口层单测：原因码映射、鉴权头、端到端断点续传。"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from resumable_upload.app import create_app
from resumable_upload.config import StagingConfig

from .conftest import make_payload


@pytest.fixture
def client(staging_dir: Path):
    config = StagingConfig(
        staging_dir=staging_dir, session_ttl_seconds=3600,
        cleanup_interval_seconds=3600, chunk_max_bytes=4 * 1024 * 1024,
    )
    app = create_app(config)
    with TestClient(app) as c:
        yield c


@pytest.fixture
def quota_client(staging_dir: Path):
    config = StagingConfig(
        staging_dir=staging_dir, session_ttl_seconds=3600,
        cleanup_interval_seconds=3600, chunk_max_bytes=4 * 1024 * 1024,
        max_staging_bytes=256,
    )
    app = create_app(config)
    with TestClient(app) as c:
        yield c


def _create(client, token, total_size, chunk_size, final_sum, filename="f.bin"):
    resp = client.post(
        "/uploads/sessions",
        json={
            "filename": filename, "total_size": total_size,
            "chunk_size": chunk_size, "final_checksum": final_sum,
        },
        headers={"X-Owner-Token": token},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def _put_chunk(client, session_id, token, index, part):
    return client.put(
        f"/uploads/sessions/{session_id}/chunks/{index}",
        content=part,
        headers={
            "X-Owner-Token": token,
            "X-Chunk-Checksum": hashlib.sha256(part).hexdigest(),
        },
    )


def test_full_upload_lifecycle_over_http(client):
    data, chunks, final_sum = make_payload(700, 256)
    created = _create(client, "alice", 700, 256, final_sum)
    sid = created["session_id"]
    assert created["total_chunks"] == 3

    for i, part in enumerate(chunks):
        resp = _put_chunk(client, sid, "alice", i, part)
        assert resp.status_code == 200, resp.text
        assert resp.json()["acknowledged"] is True

    progress = client.get(f"/uploads/sessions/{sid}", headers={"X-Owner-Token": "alice"})
    assert progress.json()["missing_chunks"] == []

    commit = client.post(f"/uploads/sessions/{sid}/commit", headers={"X-Owner-Token": "alice"})
    assert commit.status_code == 200, commit.text
    body = commit.json()
    assert body["status"] == "completed"
    assert Path(body["result_path"]).read_bytes() == data

    # 重复提交幂等：结论一致、结果文件一致
    again = client.post(f"/uploads/sessions/{sid}/commit", headers={"X-Owner-Token": "alice"})
    assert again.status_code == 200
    assert again.json()["result_path"] == body["result_path"]


def test_resume_after_restart_over_http(client, staging_dir: Path):
    data, chunks, final_sum = make_payload(700, 256)
    created = _create(client, "alice", 700, 256, final_sum)
    sid = created["session_id"]
    # 中断前仅传分片 0、2
    for i in (0, 2):
        assert _put_chunk(client, sid, "alice", i, chunks[i]).status_code == 200

    # 重启：重建 app（lifespan 内自动 recover）
    config = StagingConfig(staging_dir=staging_dir, session_ttl_seconds=3600,
                           cleanup_interval_seconds=3600)
    with TestClient(create_app(config)) as client2:
        progress = client2.get(f"/uploads/sessions/{sid}", headers={"X-Owner-Token": "alice"})
        assert progress.json()["received_chunks"] == [0, 2]
        # 已确认分片重传幂等
        assert _put_chunk(client2, sid, "alice", 0, chunks[0]).status_code == 200
        for i in (1, 2):
            assert _put_chunk(client2, sid, "alice", i, chunks[i]).status_code == 200
        commit = client2.post(f"/uploads/sessions/{sid}/commit",
                              headers={"X-Owner-Token": "alice"})
        assert commit.status_code == 200
        assert Path(commit.json()["result_path"]).read_bytes() == data


def test_http_error_reasons_are_distinguishable(client):
    data, chunks, final_sum = make_payload(512, 256)
    created = _create(client, "alice", 512, 256, final_sum)
    sid = created["session_id"]

    # 越权 -> 403 unauthorized
    resp = client.put(
        f"/uploads/sessions/{sid}/chunks/0", content=chunks[0],
        headers={"X-Owner-Token": "bob", "X-Chunk-Checksum": "0" * 64},
    )
    assert resp.status_code == 403
    assert resp.json()["detail"]["reason"] == "unauthorized"

    # 分片校验不一致 -> 422 checksum_mismatch
    resp = client.put(
        f"/uploads/sessions/{sid}/chunks/0", content=chunks[0],
        headers={"X-Owner-Token": "alice", "X-Chunk-Checksum": "0" * 64},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"]["reason"] == "checksum_mismatch"

    # 缺片提交 -> 409 missing_chunks，且 missing_chunks 明确
    assert _put_chunk(client, sid, "alice", 0, chunks[0]).status_code == 200
    resp = client.post(f"/uploads/sessions/{sid}/commit",
                       headers={"X-Owner-Token": "alice"})
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "missing_chunks"
    assert resp.json()["detail"]["missing_chunks"] == [1]

    # 不存在的会话 -> 404
    resp = client.get("/uploads/sessions/deadbeef", headers={"X-Owner-Token": "alice"})
    assert resp.status_code == 404
    assert resp.json()["detail"]["reason"] == "session_not_found"


def test_final_checksum_mismatch_fails_commit(client):
    data, chunks, _ = make_payload(512, 256)
    created = _create(client, "alice", 512, 256, "f" * 64)
    sid = created["session_id"]
    for i, part in enumerate(chunks):
        assert _put_chunk(client, sid, "alice", i, part).status_code == 200
    resp = client.post(f"/uploads/sessions/{sid}/commit",
                       headers={"X-Owner-Token": "alice"})
    assert resp.status_code == 422
    assert resp.json()["detail"]["reason"] == "final_checksum_mismatch"


def test_renew_session_over_http(client):
    data, chunks, final_sum = make_payload(512, 256)
    created = _create(client, "alice", 512, 256, final_sum)
    sid = created["session_id"]
    assert _put_chunk(client, sid, "alice", 0, chunks[0]).status_code == 200

    resp = client.post(
        f"/uploads/sessions/{sid}/renew",
        json={"extend_seconds": 7200},
        headers={"X-Owner-Token": "alice"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "pending"
    assert body["expires_at"] > created["expires_at"]
    # 已确认分片不变
    assert body["received_chunks"] == [0]

    # 省略 extend_seconds 时按默认 TTL 顺延，也应成功
    default_renew = client.post(
        f"/uploads/sessions/{sid}/renew", json={},
        headers={"X-Owner-Token": "alice"},
    )
    assert default_renew.status_code == 200

    # 越权续期 -> 403
    forbidden = client.post(
        f"/uploads/sessions/{sid}/renew", json={"extend_seconds": 60},
        headers={"X-Owner-Token": "bob"},
    )
    assert forbidden.status_code == 403
    assert forbidden.json()["detail"]["reason"] == "unauthorized"

    # 非法 extend_seconds -> 400 invalid_request
    bad = client.post(
        f"/uploads/sessions/{sid}/renew", json={"extend_seconds": -1},
        headers={"X-Owner-Token": "alice"},
    )
    assert bad.status_code == 400
    assert bad.json()["detail"]["reason"] == "invalid_request"


def test_quota_exceeded_over_http_returns_507(quota_client):
    data, chunks, final_sum = make_payload(512, 256)
    created = _create(quota_client, "alice", 512, 256, final_sum)
    sid = created["session_id"]
    assert _put_chunk(quota_client, sid, "alice", 0, chunks[0]).status_code == 200
    # 第二个分片将使暂存占用达到 512 > 256 上限
    resp = _put_chunk(quota_client, sid, "alice", 1, chunks[1])
    assert resp.status_code == 507, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == "quota_exceeded"
    assert detail["quota"]["limit"] == 256
    assert detail["quota"]["requested"] == 256
    # 已确认分片未被破坏
    progress = quota_client.get(
        f"/uploads/sessions/{sid}", headers={"X-Owner-Token": "alice"}
    ).json()
    assert progress["received_chunks"] == [0]


def test_quota_full_blocks_new_session_creation_over_http(quota_client):
    data, chunks, final_sum = make_payload(256, 256)
    first = _create(quota_client, "alice", 256, 256, final_sum)
    assert _put_chunk(quota_client, first["session_id"], "alice", 0, chunks[0]).status_code == 200
    # 暂存已满，再建会话被明确拒绝为 507 quota_exceeded，而非其他原因
    resp = quota_client.post(
        "/uploads/sessions",
        json={"filename": "x", "total_size": 10, "chunk_size": 10,
              "final_checksum": final_sum},
        headers={"X-Owner-Token": "bob"},
    )
    assert resp.status_code == 507
    assert resp.json()["detail"]["reason"] == "quota_exceeded"

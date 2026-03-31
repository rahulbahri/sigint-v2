"""
tests/test_upload.py — CSV upload and workspace isolation tests.
"""
import io
import pytest
from httpx import AsyncClient

CSV_CONTENT = b"""date,revenue,cogs,opex,customers
2025-01-01,100000,40000,30000,50
2025-02-01,110000,42000,31000,55
2025-03-01,120000,44000,32000,60
"""


@pytest.mark.anyio
async def test_upload_no_auth_behavior(client: AsyncClient):
    """
    POST /api/upload without auth — _get_workspace returns '' so workspace_id is ''.
    The upload itself may succeed (200) with an empty workspace_id (M7 bug),
    OR the endpoint may return 200 regardless.  We just assert it does NOT return 401.
    """
    files = {"file": ("test.csv", io.BytesIO(CSV_CONTENT), "text/csv")}
    resp = await client.post("/api/upload", files=files)
    # The endpoint uses _get_workspace (not _require_workspace) so it will succeed
    # with workspace_id="" — test documents the current (unguarded) behaviour.
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_upload_with_auth(client: AsyncClient, auth_headers):
    """POST /api/upload with auth + valid CSV → 200, returns upload_id."""
    files = {"file": ("test_alice.csv", io.BytesIO(CSV_CONTENT), "text/csv")}
    resp = await client.post("/api/upload", files=files, headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "upload_id" in data
    assert data["upload_id"] is not None
    assert data["rows_processed"] == 3
    assert data["months_detected"] == 3


@pytest.mark.anyio
async def test_list_uploads_with_auth(client: AsyncClient, auth_headers):
    """GET /api/uploads with auth → 200, returns alice's uploads (at least one)."""
    # Seed an upload first
    files = {"file": ("list_test.csv", io.BytesIO(CSV_CONTENT), "text/csv")}
    await client.post("/api/upload", files=files, headers=auth_headers)

    resp = await client.get("/api/uploads", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 1
    # Every returned entry must have the expected shape
    for entry in data:
        assert "id" in entry
        assert "filename" in entry


@pytest.mark.anyio
async def test_workspace_isolation(client: AsyncClient, auth_headers, other_auth_headers):
    """GET /api/uploads for bob should not include alice's uploads."""
    # Ensure alice has at least one upload
    files = {"file": ("isolation_test.csv", io.BytesIO(CSV_CONTENT), "text/csv")}
    alice_upload = await client.post("/api/upload", files=files, headers=auth_headers)
    assert alice_upload.status_code == 200

    # Bob checks his uploads — his workspace is completely separate
    bob_resp = await client.get("/api/uploads", headers=other_auth_headers)
    assert bob_resp.status_code == 200
    bob_data = bob_resp.json()

    alice_id = alice_upload.json()["upload_id"]
    bob_ids = [entry["id"] for entry in bob_data]
    assert alice_id not in bob_ids, "Bob must NOT see Alice's upload"


@pytest.mark.anyio
async def test_delete_upload_no_auth(client: AsyncClient, auth_headers):
    """DELETE /api/uploads/{id} without auth → 401."""
    # Create an upload as alice first
    files = {"file": ("to_delete.csv", io.BytesIO(CSV_CONTENT), "text/csv")}
    create_resp = await client.post("/api/upload", files=files, headers=auth_headers)
    upload_id = create_resp.json()["upload_id"]

    # Attempt deletion with no auth — _get_workspace returns '' which triggers 401
    resp = await client.delete(f"/api/uploads/{upload_id}")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_delete_upload_wrong_workspace(client: AsyncClient, auth_headers, other_auth_headers):
    """DELETE /api/uploads/{id} with wrong workspace → 404."""
    # Alice creates an upload
    files = {"file": ("wrong_ws.csv", io.BytesIO(CSV_CONTENT), "text/csv")}
    create_resp = await client.post("/api/upload", files=files, headers=auth_headers)
    upload_id = create_resp.json()["upload_id"]

    # Bob tries to delete Alice's upload — should get 404 (not found in his workspace)
    resp = await client.delete(f"/api/uploads/{upload_id}", headers=other_auth_headers)
    assert resp.status_code == 404

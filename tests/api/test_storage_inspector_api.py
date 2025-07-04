import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_storage_inspector_endpoints():
    with TestClient(app) as client:
        # fetch an available node id
        nodes_resp = client.get("/cluster/nodes")
        assert nodes_resp.status_code == 200
        node_id = nodes_resp.json()["nodes"][0]["node_id"]

        # perform a write so that WAL and memtable have data
        client.post("/put/inspector_key", params={"value": "1"})

        # WAL contents
    resp = client.get(f"/nodes/{node_id}/wal")
    assert resp.status_code in {200, 503}
    if resp.status_code == 200:
        data = resp.json()
        assert "entries" in data
        assert isinstance(data["entries"], list)
        if data["entries"]:
            entry = data["entries"][0]
            assert "type" in entry and "key" in entry and "vector_clock" in entry

        # Memtable contents
    resp = client.get(f"/nodes/{node_id}/memtable")
    assert resp.status_code in {200, 503}
    if resp.status_code == 200:
        data = resp.json()
        assert "entries" in data
        assert isinstance(data["entries"], list)
        if data["entries"]:
            entry = data["entries"][0]
            assert "key" in entry and "vector_clock" in entry

        # SSTable metadata
    resp = client.get(f"/nodes/{node_id}/sstables")
    assert resp.status_code in {200, 503}
    if resp.status_code == 200:
        data = resp.json()
        assert "tables" in data
        assert isinstance(data["tables"], list)
        if data["tables"]:
            table = data["tables"][0]
            assert "id" in table and "level" in table and "item_count" in table


def test_storage_inspector_pagination():
    with TestClient(app) as client:
        nodes_resp = client.get("/cluster/nodes")
        assert nodes_resp.status_code == 200
        node_id = nodes_resp.json()["nodes"][0]["node_id"]

        for i in range(5):
            client.post(f"/put/inspector_page{i}", params={"value": str(i)})

        resp_all = client.get(f"/nodes/{node_id}/wal")
        assert resp_all.status_code in {200, 503}
        if resp_all.status_code == 200:
            entries = resp_all.json().get("entries", [])
            resp = client.get(
                f"/nodes/{node_id}/wal",
                params={"offset": 1, "limit": 2},
            )
            assert resp.status_code == 200
            assert resp.json().get("entries") == entries[1:3]

        resp_all = client.get(f"/nodes/{node_id}/memtable")
        assert resp_all.status_code in {200, 503}
        if resp_all.status_code == 200:
            entries = resp_all.json().get("entries", [])
            resp = client.get(
                f"/nodes/{node_id}/memtable",
                params={"offset": 1, "limit": 2},
            )
            assert resp.status_code == 200
            assert resp.json().get("entries") == entries[1:3]

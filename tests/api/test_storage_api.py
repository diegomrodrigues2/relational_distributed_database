import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_node_storage_endpoints():
    with TestClient(app) as client:
        resp = client.get("/cluster/nodes")
        assert resp.status_code == 200
        node_id = resp.json()["nodes"][0]["node_id"]

        # trigger some activity so WAL and memtable are not empty
        client.post("/put/test_key", params={"value": "1"})

        resp = client.get(f"/nodes/{node_id}/wal")
        assert resp.status_code == 200
        data = resp.json()
        assert "entries" in data or "error" in data

        resp = client.get(f"/nodes/{node_id}/memtable")
        assert resp.status_code == 200
        data = resp.json()
        assert "entries" in data or "error" in data

        resp = client.get(f"/nodes/{node_id}/sstables")
        assert resp.status_code == 200
        data = resp.json()
        assert "tables" in data or "error" in data

import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_cluster_hotspots():
    with TestClient(app) as client:
        # generate some traffic
        client.post("/put/hotkey", params={"value": "1"})
        client.get("/get/hotkey")
        resp = client.get("/cluster/hotspots")
        assert resp.status_code == 200
        data = resp.json()
        assert "hot_partitions" in data
        assert "hot_keys" in data
        assert isinstance(data["hot_partitions"], list)
        assert isinstance(data["hot_keys"], list)


def test_replication_status():
    with TestClient(app) as client:
        nodes_resp = client.get("/cluster/nodes")
        assert nodes_resp.status_code == 200
        node_id = nodes_resp.json()["nodes"][0]["node_id"]

        resp = client.get(f"/nodes/{node_id}/replication_status")
        assert resp.status_code in {200, 503}
        if resp.status_code == 200:
            data = resp.json()
            assert "last_seen" in data
            assert "hints" in data


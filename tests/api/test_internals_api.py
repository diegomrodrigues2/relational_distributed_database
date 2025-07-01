import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_cluster_hotspots_endpoint():
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


def test_node_replication_status_endpoint():
    with TestClient(app) as client:
        resp = client.get("/nodes/node_0/replication_status")
        assert resp.status_code == 200
        data = resp.json()
        assert "last_seen" in data
        assert "hints" in data

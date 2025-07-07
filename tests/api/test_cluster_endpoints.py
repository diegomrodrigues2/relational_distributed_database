import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_cluster_nodes():
    with TestClient(app) as client:
        resp = client.get("/cluster/nodes")
        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data
        assert len(data["nodes"]) == 3
        for node in data["nodes"]:
            assert "node_id" in node
            assert "port" in node


def test_cluster_partitions():
    with TestClient(app) as client:
        resp = client.get("/cluster/partitions")
        assert resp.status_code == 200
        data = resp.json()
        assert "partitions" in data
        assert len(data["partitions"]) > 0
        ids = [p["id"] for p in data["partitions"]]
        assert ids == sorted(ids)
        for p in data["partitions"]:
            assert "key_range" in p


def test_time_series_metrics():
    with TestClient(app) as client:
        resp = client.get("/cluster/metrics/time_series")
        assert resp.status_code == 200
        data = resp.json()
        assert "latency_ms" in data
        assert isinstance(data["latency_ms"], list)
        assert "throughput" in data


def test_cluster_config():
    with TestClient(app) as client:
        resp = client.get("/cluster/config")
        assert resp.status_code == 200
        data = resp.json()
        assert data["replication_factor"] >= 1
        assert data["num_partitions"] >= 1

import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_dashboard_nodes():
    with TestClient(app) as client:
        resp = client.get("/cluster/nodes")
        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data
        assert isinstance(data["nodes"], list)
        assert len(data["nodes"]) > 0


def test_dashboard_partitions():
    with TestClient(app) as client:
        resp = client.get("/cluster/partitions")
        assert resp.status_code == 200
        data = resp.json()
        assert "partitions" in data
        assert isinstance(data["partitions"], list)


def test_dashboard_time_series():
    with TestClient(app) as client:
        resp = client.get("/cluster/metrics/time_series")
        assert resp.status_code == 200
        data = resp.json()
        assert "latency_ms" in data
        assert "throughput" in data


def test_dashboard_config():
    with TestClient(app) as client:
        resp = client.get("/cluster/config")
        assert resp.status_code == 200
        data = resp.json()
        assert "replication_factor" in data
        assert "num_partitions" in data

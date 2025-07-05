import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_event_log_endpoint_returns_events():
    with TestClient(app) as client:
        resp = client.get("/cluster/events")
        assert resp.status_code == 200
        data = resp.json()
        assert "events" in data
        assert isinstance(data["events"], list)
        assert any("NodeCluster created" in e for e in data["events"])


def test_node_event_log_endpoint_returns_events():
    with TestClient(app) as client:
        resp = client.get("/cluster/nodes/node_0/events")
        assert resp.status_code == 200
        data = resp.json()
        assert "events" in data
        assert isinstance(data["events"], list)

import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_check_hot_partitions_splits():
    with TestClient(app) as client:
        cluster = app.state.cluster
        client.post("/cluster/actions/reset_metrics")
        for _ in range(5):
            client.post("/put/hot_a", params={"value": "v"})
            client.post("/put/hot_b", params={"value": "v"})
        before = cluster.num_partitions
        resp = client.post("/cluster/actions/check_hot_partitions")
        assert resp.status_code == 200
        assert cluster.num_partitions == before + 1


def test_reset_metrics_clears_ops():
    with TestClient(app) as client:
        cluster = app.state.cluster
        for _ in range(3):
            client.post("/put/key", params={"value": "v"})
        assert any(cluster.partition_ops)
        resp = client.post("/cluster/actions/reset_metrics")
        assert resp.status_code == 200
        assert all(cnt == 0 for cnt in cluster.partition_ops)


def test_mark_hot_key_enables_salting():
    with TestClient(app) as client:
        cluster = app.state.cluster
        resp = client.post(
            "/cluster/actions/mark_hot_key",
            json={"key": "hk", "buckets": 2, "migrate": False},
        )
        assert resp.status_code == 200
        assert "hk" in cluster.salted_keys

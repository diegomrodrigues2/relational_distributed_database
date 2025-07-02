import os
import sys
import time
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_check_hot_partitions_splits_partition():
    with TestClient(app) as client:
        cluster = app.state.cluster
        from database.clustering.partitioning import HashPartitioner

        # convert existing cluster to hash partitioning for easier splitting
        cluster.partitioner = HashPartitioner(cluster.num_partitions, cluster.nodes)
        cluster.partition_strategy = "hash"
        cluster.ring = None
        cluster.key_ranges = None
        cluster.update_partition_map()
        cluster.reset_metrics()
        time.sleep(0.2)
        initial = cluster.num_partitions
        for i in range(5):
            client.post("/put/b", params={"value": str(i)})
            client.post("/put/ae", params={"value": str(i)})
        resp = client.post(
            "/cluster/actions/check_hot_partitions",
            params={"threshold": 1.0, "min_keys": 2},
        )
        assert resp.status_code == 200
        assert cluster.num_partitions > initial


def test_reset_metrics_clears_partition_ops():
    with TestClient(app) as client:
        cluster = app.state.cluster
        for i in range(3):
            client.post("/put/x", params={"value": str(i)})
        assert any(o > 0 for o in cluster.partition_ops)
        resp = client.post("/cluster/actions/reset_metrics")
        assert resp.status_code == 200
        assert all(o == 0 for o in cluster.partition_ops)


def test_mark_hot_key_endpoint():
    with TestClient(app) as client:
        cluster = app.state.cluster
        resp = client.post(
            "/cluster/actions/mark_hot_key",
            params={"key": "hk", "buckets": 2, "migrate": False},
        )
        assert resp.status_code == 200
        assert "hk" in cluster.salted_keys

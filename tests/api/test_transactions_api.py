import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from api.main import app


def test_cluster_transactions_endpoint():
    with TestClient(app) as client:
        cluster = app.state.cluster
        tx_id, _ = cluster.nodes[0].client.begin_transaction()
        try:
            resp = client.get("/cluster/transactions")
            assert resp.status_code == 200
            data = resp.json()
            assert "transactions" in data
            entries = {t["node"]: t["tx_ids"] for t in data["transactions"]}
            assert tx_id in entries.get(cluster.nodes[0].node_id, [])
        finally:
            cluster.nodes[0].client.abort_transaction(tx_id)

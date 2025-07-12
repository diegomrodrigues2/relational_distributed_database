import json
import sys
import os
import uuid
import time

# Ensure project root is on the import path just like the tests do
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.main import app
from database.replication import NodeCluster
from examples.service_runner import start_frontend
from examples.data_generators import generate_hash_items
from database.clustering.partitioning import compose_key
from database.sql.query_coordinator import QueryCoordinator
import tempfile


def main() -> None:
    app.router.on_startup.clear()
    ranges = [("a", "m"), ("m", "z")]
    cluster_name = f"registry_cluster_{uuid.uuid4().hex[:6]}"
    cluster = NodeCluster(
        base_path=os.path.join(tempfile.gettempdir(), cluster_name),
        num_nodes=2,
        key_ranges=ranges,
        start_router=True,
        use_registry=True,
    )

    print("Partitions:")
    for pid, owner in sorted(cluster.get_partition_map().items()):
        rng = cluster.partitions[pid][0]
        print(f"  P{pid} {rng} -> {owner}")

    # --- Relational setup ---
    ddl = "CREATE TABLE items (id STRING PRIMARY KEY, value STRING)"
    cluster.nodes[0].client.execute_ddl(ddl)
    time.sleep(0.5)

    for key, value in generate_hash_items(5):
        row = {"id": key, "value": value}
        record_key = compose_key("items", key, None)
        cluster.router_client.put(record_key, json.dumps(row))
        pid = cluster.get_partition_id(key)
        owner = cluster.get_partition_map().get(pid)
        rng = cluster.partitions[pid][0]
        print(
            f"Router stored row {row} in partition {pid} {rng} on {owner}"
        )

    qc = QueryCoordinator(cluster.nodes)
    rows = qc.execute("SELECT * FROM items")
    print("Query results:")
    for r in rows:
        print(r)
    app.state.cluster = cluster
    front_proc = start_frontend()
    print("API running at http://localhost:8000")
    try:
        import uvicorn
        uvicorn.run("api.main:app", host="0.0.0.0", port=8000)
    finally:
        front_proc.terminate()
        cluster.shutdown()


if __name__ == "__main__":
    main()

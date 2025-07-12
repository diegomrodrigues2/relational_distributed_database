import json
import sys
import os
import tempfile
import time
import uuid

# Ensure project root is on the import path just like the tests do
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.main import app
from database.replication import NodeCluster
from examples.service_runner import start_frontend
from database.clustering.partitioning import compose_key
from database.sql.query_coordinator import QueryCoordinator
from examples.data_generators import COLORS


def main() -> None:
    app.router.on_startup.clear()
    cluster_name = f"index_cluster_{uuid.uuid4().hex[:6]}"
    cluster = NodeCluster(
        base_path=os.path.join(tempfile.gettempdir(), cluster_name),
        num_nodes=3,
        index_fields=["color"],
    )

    print("Partition map:")
    for pid, owner in sorted(cluster.get_partition_map().items()):
        print(f"  P{pid}: {owner}")

    # --- Relational setup ---
    ddl = "CREATE TABLE widgets (id INT PRIMARY KEY, color STRING)"
    cluster.nodes[0].client.execute_ddl(ddl)
    time.sleep(0.5)

    for i in range(1, 6):
        color = COLORS[(i - 1) % len(COLORS)]
        row = {"id": i, "color": color}
        key = compose_key("widgets", str(i), None)
        cluster.put(0, key, json.dumps(row))
        pid = cluster.get_partition_id(str(i))
        owner = cluster.get_partition_map().get(pid)
        idx_owner = cluster.get_index_owner("color", color)
        print(
            f"Stored row {row} in partition {pid} on {owner}; index on {idx_owner}"
        )

    qc = QueryCoordinator(cluster.nodes)
    rows = qc.execute("SELECT * FROM widgets")
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

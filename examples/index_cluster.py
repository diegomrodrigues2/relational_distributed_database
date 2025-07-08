import json
import sys
import os
import tempfile

# Ensure project root is on the import path just like the tests do
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.main import app
from database.replication import NodeCluster
from examples.service_runner import start_frontend
from examples.data_generators import generate_index_items


def main() -> None:
    app.router.on_startup.clear()
    cluster = NodeCluster(
        base_path=os.path.join(tempfile.gettempdir(), "index_cluster"),
        num_nodes=3,
        index_fields=["color"],
    )

    print("Partition map:")
    for pid, owner in sorted(cluster.get_partition_map().items()):
        print(f"  P{pid}: {owner}")

    for key, value in generate_index_items(50):
        cluster.put(0, key, value)
        color = json.loads(value)["color"]
        pid = cluster.get_partition_id(key)
        owner = cluster.get_partition_map().get(pid)
        idx_owner = cluster.get_index_owner("color", color)
        print(
            f"Stored {key} color={color} in partition {pid} on {owner}; index on {idx_owner}"
        )
    app.state.cluster = cluster
    front_proc = start_frontend()
    print("API running at http://localhost:8000")
    try:
        import uvicorn
        uvicorn.run("api.main:app", port=8000)
    finally:
        front_proc.terminate()
        cluster.shutdown()


if __name__ == "__main__":
    main()

import json
import sys
import os
import tempfile
import uuid

# Ensure project root is on the import path just like the tests do
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.main import app
from database.replication import NodeCluster
from examples.service_runner import start_frontend
from examples.data_generators import generate_range_items


def main() -> None:
    app.router.on_startup.clear()
    ranges = [("a", "m"), ("m", "z")]
    cluster_name = f"range_cluster_{uuid.uuid4().hex[:6]}"
    cluster = NodeCluster(
        base_path=os.path.join(tempfile.gettempdir(), cluster_name),
        num_nodes=3,
        key_ranges=ranges,
    )

    print("Partitions:")
    for pid, owner in sorted(cluster.get_partition_map().items()):
        rng = cluster.partitions[pid][0]
        print(f"  P{pid} {rng} -> {owner}")

    for key, value in generate_range_items(50):
        cluster.put(0, key, value)
        pk = key.split("|", 1)[0]
        pid = cluster.get_partition_id(pk)
        owner = cluster.get_partition_map().get(pid)
        rng = cluster.partitions[pid][0]
        print(f"Stored {key} in partition {pid} {rng} on {owner}")
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

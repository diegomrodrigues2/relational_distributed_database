import os
import sys
import tempfile
import uuid

# Ensure project root is on the import path just like the tests do
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.main import app
from database.replication import NodeCluster
from examples.service_runner import start_frontend
from examples.data_generators import generate_session_data


def main() -> None:
    app.router.on_startup.clear()
    cluster_name = f"session_cluster_{uuid.uuid4().hex[:6]}"
    cluster = NodeCluster(
        base_path=os.path.join(tempfile.gettempdir(), cluster_name),
        num_nodes=3,
        partition_strategy="hash",
        replication_factor=2,
        partitions_per_node=32,
        consistency_mode="lww",
    )

    print("Partition map:")
    for pid, owner in sorted(cluster.get_partition_map().items()):
        print(f"  P{pid}: {owner}")

    NUM_SESSIONS = 10000
    for sid, value in generate_session_data(NUM_SESSIONS):
        cluster.put(0, sid, value)
        pid = cluster.get_partition_id(sid)
        owner = cluster.get_partition_map().get(pid)
        print(f"Stored {sid} in partition {pid} on {owner}")

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

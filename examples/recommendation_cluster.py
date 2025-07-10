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
from examples.data_generators import generate_recommendation_data


def main() -> None:
    app.router.on_startup.clear()
    cluster_name = f"recommendation_cluster_{uuid.uuid4().hex[:6]}"
    cluster = NodeCluster(
        base_path=os.path.join(tempfile.gettempdir(), cluster_name),
        num_nodes=3,
        index_fields=["preference"],
    )

    print("Partition map:")
    for pid, owner in sorted(cluster.get_partition_map().items()):
        print(f"  P{pid}: {owner}")

    for key, value in generate_recommendation_data(10000):
        cluster.put(0, key, value)
        pref = json.loads(value)["preference"]
        pid = cluster.get_partition_id(key)
        owner = cluster.get_partition_map().get(pid)
        idx_owner = cluster.get_index_owner("preference", pref)
        print(
            f"Stored {key} pref={pref} in partition {pid} on {owner}; index on {idx_owner}"
        )

    sports_users = cluster.secondary_query("preference", "sports")
    if sports_users:
        print(f"Users who prefer sports: {', '.join(sports_users)}")

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

import os
import sys
import time

# Ensure project root is on the import path just like the tests do
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.main import app
from database.replication import NodeCluster
from examples.service_runner import start_frontend


def main() -> None:
    app.router.on_startup.clear()
    cluster = NodeCluster(
        base_path="/tmp/hash_cluster",
        num_nodes=3,
        partition_strategy="hash",
        consistency_mode="lww",
    )
    cluster.put(0, "k1", "v1")
    cluster.put(0, "k2", "v2")
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

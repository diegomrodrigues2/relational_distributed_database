import os
import time

from api.main import app
from database.replication import NodeCluster
from database.clustering.partitioning import compose_key
from .service_runner import start_frontend


def main() -> None:
    app.router.on_startup.clear()
    ranges = [("a", "m"), ("m", "z")]
    cluster = NodeCluster(base_path="/tmp/range_cluster", num_nodes=3, key_ranges=ranges)
    cluster.put(0, compose_key("a", "1"), "v1")
    cluster.put(0, compose_key("b", "2"), "v2")
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

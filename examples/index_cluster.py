import os
import json
import time

from api.main import app
from database.replication import NodeCluster
from .service_runner import start_frontend, ngrok


def main(tunnel: bool = False):
    app.router.on_startup.clear()
    cluster = NodeCluster(base_path="/tmp/index_cluster", num_nodes=3, index_fields=["color"])
    cluster.put(0, "p1", json.dumps({"color": "red"}))
    cluster.put(0, "p2", json.dumps({"color": "blue"}))
    app.state.cluster = cluster
    front_proc = start_frontend(tunnel)
    print("API running at http://localhost:8000")
    try:
        import uvicorn
        uvicorn.run("api.main:app", port=8000)
    finally:
        front_proc.terminate()
        cluster.shutdown()
        if tunnel and ngrok:
            ngrok.kill()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tunnel",
        action="store_true",
        help="Expose API and UI using ngrok",
    )
    args = parser.parse_args()
    main(tunnel=args.tunnel)

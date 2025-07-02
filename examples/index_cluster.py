import os
import json
import time

from api.main import app
from database.replication import NodeCluster
from .service_runner import start_services, ngrok


def main(tunnel: bool = False):
    app.router.on_startup.clear()
    cluster = NodeCluster(base_path="/tmp/index_cluster", num_nodes=3, index_fields=["color"])
    cluster.put(0, "p1", json.dumps({"color": "red"}))
    cluster.put(0, "p2", json.dumps({"color": "blue"}))
    app.state.cluster = cluster
    api_proc, front_proc = start_services(tunnel)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        api_proc.terminate()
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

import os
import sys
import time

# Ensure project root is on the import path just like the tests do
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.main import app
from database.replication import NodeCluster
from .service_runner import start_services, ngrok


def main(tunnel: bool = False):
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

import os
import time

from api.main import app
from database.replication import NodeCluster
from database.clustering.partitioning import compose_key
from .service_runner import start_services, ngrok


def main(tunnel: bool = False):
    app.router.on_startup.clear()
    ranges = [("a", "m"), ("m", "z")]
    cluster = NodeCluster(base_path="/tmp/range_cluster", num_nodes=3, key_ranges=ranges)
    cluster.put(0, compose_key("a", "1"), "v1")
    cluster.put(0, compose_key("b", "2"), "v2")
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

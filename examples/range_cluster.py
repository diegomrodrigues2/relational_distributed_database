import os
import subprocess
import time

from api.main import app
from database.replication import NodeCluster
from database.clustering.partitioning import compose_key

try:
    from pyngrok import ngrok  # type: ignore
except Exception:
    ngrok = None


def start_services(tunnel: bool = False):
    api_proc = subprocess.Popen([
        "uvicorn",
        "api.main:app",
        "--port",
        "8000",
    ])
    frontend_proc = subprocess.Popen([
        "npm",
        "run",
        "dev",
    ], cwd=os.path.join(os.path.dirname(__file__), "..", "app"))
    if tunnel and ngrok:
        api_url = ngrok.connect(8000, bind_tls=True).public_url
        ui_url = ngrok.connect(5173, bind_tls=True).public_url
    else:
        api_url = "http://localhost:8000"
        ui_url = "http://localhost:5173"
    print(f"API running at {api_url}")
    print(f"Frontend running at {ui_url}")
    return api_proc, frontend_proc


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

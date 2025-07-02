import os
import sys
import subprocess
import time
import socket

# Ensure project root is on the import path just like the tests do
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.main import app
from database.replication import NodeCluster

try:
    from pyngrok import ngrok  # type: ignore
except Exception:
    ngrok = None


def wait_port(port: int, host: str = "127.0.0.1", timeout: float = 30.0) -> bool:
    """Wait until a TCP port is open."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), 1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


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
    wait_port(8000)
    ui_ready = wait_port(5173)

    if tunnel and ngrok:
        api_url = ngrok.connect(8000, bind_tls=True).public_url
        ui_url = (
            ngrok.connect(5173, bind_tls=True).public_url if ui_ready else None
        )
    else:
        api_url = "http://localhost:8000"
        ui_url = "http://localhost:5173" if ui_ready else None

    print(f"API running at {api_url}")
    if ui_url:
        print(f"Frontend running at {ui_url}")
    else:
        print("Frontend failed to start on port 5173")
    return api_proc, frontend_proc


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

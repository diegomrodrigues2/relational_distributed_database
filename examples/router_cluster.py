import os
import subprocess
import time

from api.main import app
from database.replication import NodeCluster


def start_services():
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
    print("API running at http://localhost:8000")
    print("Frontend running at http://localhost:5173")
    return api_proc, frontend_proc


def main():
    app.router.on_startup.clear()
    cluster = NodeCluster(base_path="/tmp/router_cluster", num_nodes=2, start_router=True)
    cluster.router_client.put("r1", "v1")
    cluster.router_client.put("r2", "v2")
    app.state.cluster = cluster
    api_proc, front_proc = start_services()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        api_proc.terminate()
        front_proc.terminate()
        cluster.shutdown()


if __name__ == "__main__":
    main()

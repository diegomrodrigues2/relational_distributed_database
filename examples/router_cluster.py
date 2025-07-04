from api.main import app
from database.replication import NodeCluster
from .service_runner import start_frontend
from .data_generators import generate_hash_items


def main() -> None:
    app.router.on_startup.clear()
    cluster = NodeCluster(
        base_path="/tmp/router_cluster",
        num_nodes=2,
        start_router=True,
    )
    for key, value in generate_hash_items(10):
        cluster.router_client.put(key, value)
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

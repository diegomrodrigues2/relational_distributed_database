from api.main import app
from database.replication import NodeCluster
from .service_runner import start_frontend
from .data_generators import generate_range_items


def main() -> None:
    app.router.on_startup.clear()
    ranges = [("a", "m"), ("m", "z")]
    cluster = NodeCluster(
        base_path="/tmp/range_cluster",
        num_nodes=3,
        key_ranges=ranges,
    )
    for key, value in generate_range_items(20):
        cluster.put(0, key, value)
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

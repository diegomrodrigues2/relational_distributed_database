import json
import sys
import os
import tempfile
import uuid
import time

# Ensure project root is on the import path just like the tests do
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.main import app
from database.replication import NodeCluster
from examples.service_runner import start_frontend
from examples.data_generators import generate_product_catalog, generate_cart_items
from database.clustering.partitioning import compose_key
from database.sql.query_coordinator import QueryCoordinator


def main() -> None:
    app.router.on_startup.clear()
    cluster_name = f"ecommerce_cluster_{uuid.uuid4().hex[:6]}"
    cluster = NodeCluster(
        base_path=os.path.join(tempfile.gettempdir(), cluster_name),
        num_nodes=3,
        partition_strategy="hash",
        replication_factor=2,
        partitions_per_node=32,
        host="127.0.0.1",
    )

    print("Partition map:")
    for pid, owner in sorted(cluster.get_partition_map().items()):
        print(f"  P{pid}: {owner}")

    # --- Relational setup ---
    ddl_catalog = "CREATE TABLE catalog (id STRING PRIMARY KEY, name STRING, price FLOAT)"
    ddl_carts = "CREATE TABLE carts (id STRING PRIMARY KEY, items STRING)"
    cluster.nodes[0].client.execute_ddl(ddl_catalog)
    cluster.nodes[0].client.execute_ddl(ddl_carts)
    time.sleep(0.5)

    print("Loading catalog...")
    for key, value in generate_product_catalog(5):
        attrs = json.loads(value)
        row = {"id": key, "name": attrs["name"], "price": attrs["price"]}
        record_key = compose_key("catalog", key, None)
        cluster.put(0, record_key, json.dumps(row))
        pid = cluster.get_partition_id(key)
        owner = cluster.get_partition_map().get(pid)
        print(f"Catalog product {row} in partition {pid} on {owner}")

    print("Loading carts...")
    for key, value in generate_cart_items(5):
        row = {"id": key, "items": value}
        record_key = compose_key("carts", key, None)
        cluster.put(0, record_key, json.dumps(row))
        pid = cluster.get_partition_id(key)
        owner = cluster.get_partition_map().get(pid)
        print(f"Cart {row} in partition {pid} on {owner}")

    qc = QueryCoordinator(cluster.nodes)
    rows = qc.execute("SELECT * FROM catalog")
    print("Catalog rows:")
    for r in rows:
        print(r)

    app.state.cluster = cluster
    front_proc = start_frontend()
    print("API running at http://localhost:8000")
    try:
        import uvicorn
        uvicorn.run("api.main:app", host="0.0.0.0", port=8000)
    finally:
        front_proc.terminate()
        cluster.shutdown()


if __name__ == "__main__":
    main()

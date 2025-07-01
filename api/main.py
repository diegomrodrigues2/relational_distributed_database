from fastapi import FastAPI
from database.replication import NodeCluster
from database.replication.replica import replication_pb2
import time

app = FastAPI()


@app.on_event("startup")
def startup_event() -> None:
    """Initialize the cluster when the API starts."""
    app.state.cluster_start = time.time()
    app.state.cluster = NodeCluster(base_path="/tmp/api_cluster", num_nodes=3)


@app.on_event("shutdown")
def shutdown_event() -> None:
    """Shutdown the cluster when the API stops."""
    cluster = getattr(app.state, "cluster", None)
    if cluster is not None:
        cluster.shutdown()

@app.get("/get/{key}")
def get_value(key: str):
    """Retrieve a value from the cluster."""
    value = app.state.cluster.get(0, key)
    return {"value": value}

@app.post("/put/{key}")
def put_value(key: str, value: str):
    """Store ``value`` in the cluster under ``key``."""
    app.state.cluster.put(0, key, value)
    return {"status": "ok"}


@app.get("/cluster/nodes")
def list_nodes() -> dict:
    """Return node information aggregated from GetNodeInfo."""
    cluster = app.state.cluster
    nodes = []
    for n in cluster.nodes:
        info = {
            "node_id": n.node_id,
            "host": n.host,
            "port": n.port,
        }
        try:
            req = replication_pb2.NodeInfoRequest(node_id=n.node_id)
            resp = n.client.stub.GetNodeInfo(req)
            info.update(
                {
                    "status": resp.status,
                    "cpu": resp.cpu,
                    "memory": resp.memory,
                    "disk": resp.disk,
                    "uptime": resp.uptime,
                    "replication_log_size": resp.replication_log_size,
                    "hints_count": resp.hints_count,
                }
            )
        except Exception:
            pass
        nodes.append(info)
    return {"nodes": nodes}


@app.get("/cluster/partitions")
def list_partitions() -> dict:
    """Return partition map with operation and item count stats."""
    cluster = app.state.cluster
    mapping = cluster.get_partition_map()
    stats = cluster.get_partition_stats()
    counts = cluster.get_partition_item_counts()
    parts = []
    for pid, owner in mapping.items():
        parts.append(
            {
                "id": pid,
                "node": owner,
                "ops": stats.get(pid, 0),
                "items": counts.get(pid, 0),
            }
        )
    return {"partitions": sorted(parts, key=lambda x: x["id"])}


@app.get("/cluster/hotspots")
def cluster_hotspots() -> dict:
    """Return hot partitions and keys based on access frequency."""
    cluster = app.state.cluster
    hot_ids = cluster.get_hot_partitions()
    avg = (
        sum(cluster.partition_ops) / len(cluster.partition_ops)
        if cluster.partition_ops
        else 0
    )
    hot_parts = [
        {
            "id": pid,
            "operation_count": cluster.partition_ops[pid],
            "average_ops": avg,
        }
        for pid in hot_ids
    ]
    hot_keys = [
        {"key": k, "frequency": cluster.key_freq.get(k, 0)}
        for k in cluster.get_hot_keys()
    ]
    return {"hot_partitions": hot_parts, "hot_keys": hot_keys}


@app.get("/cluster/metrics/time_series")
def time_series_metrics() -> dict:
    """Return simple latency/throughput samples and log sizes."""
    cluster = app.state.cluster
    latencies = []
    replog = 0
    hints = 0
    for n in cluster.nodes:
        start = time.time()
        try:
            n.client.ping(n.node_id)
            latencies.append((time.time() - start) * 1000)
        except Exception:
            latencies.append(None)
        try:
            req = replication_pb2.NodeInfoRequest(node_id=n.node_id)
            resp = n.client.stub.GetNodeInfo(req)
            replog += resp.replication_log_size
            hints += resp.hints_count
        except Exception:
            pass
    total_ops = sum(cluster.get_partition_stats().values())
    elapsed = max(time.time() - getattr(app.state, "cluster_start", time.time()), 1)
    throughput = total_ops / elapsed
    return {
        "latency_ms": [l for l in latencies if l is not None],
        "throughput": throughput,
        "replication_log_size": replog,
        "hints_count": hints,
    }


@app.get("/cluster/config")
def cluster_config() -> dict:
    """Return cluster configuration values."""
    cluster = app.state.cluster
    return {
        "replication_factor": cluster.replication_factor,
        "write_quorum": cluster.write_quorum,
        "read_quorum": cluster.read_quorum,
        "partition_strategy": cluster.partition_strategy,
        "consistency_mode": cluster.consistency_mode,
        "partitions_per_node": cluster.partitions_per_node,
        "num_partitions": cluster.num_partitions,
    }


@app.get("/nodes/{node_id}/replication_status")
def node_replication_status(node_id: str) -> dict:
    """Return replication status information for ``node_id``."""
    cluster = app.state.cluster
    node = cluster.nodes_by_id.get(node_id)
    if node is None:
        return {"error": "node not found"}
    try:
        req = replication_pb2.NodeInfoRequest(node_id=node_id)
        resp = node.client.stub.GetReplicationStatus(req)
        return {
            "last_seen": dict(resp.last_seen),
            "hints": dict(resp.hints),
        }
    except Exception:
        return {"error": "unreachable"}


@app.get("/health")
def health() -> dict:
    """Return basic cluster information."""
    cluster = app.state.cluster
    healthy = 0
    for n in cluster.nodes:
        try:
            n.client.ping(n.node_id)
            healthy += 1
        except Exception:
            continue
    return {"nodes": len(cluster.nodes), "healthy": healthy}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=False)

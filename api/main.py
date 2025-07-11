from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from database.replication import NodeCluster
from database.replication.replica import replication_pb2
from concurrent.futures import ThreadPoolExecutor
from database.sql.query_coordinator import QueryCoordinator
from database.sql.parser import parse_sql
from database.sql.planner import QueryPlanner
from database.sql.metadata import CatalogManager
from database.lsm.lsm_db import SimpleLSMDB
import time
import os
import tempfile

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Record(BaseModel):
    partitionKey: str
    clusteringKey: str | None = None
    value: str


@app.on_event("startup")
def startup_event() -> None:
    """Initialize the cluster when the API starts."""
    app.state.cluster_start = time.time()
    app.state.cluster = NodeCluster(
        base_path=os.path.join(tempfile.gettempdir(), "api_cluster"), num_nodes=3
    )


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
    ranges = cluster.get_partition_ranges()
    stats = cluster.get_partition_stats()
    counts = cluster.get_partition_item_counts()
    parts = []
    for pid, owner in mapping.items():
        parts.append(
            {
                "id": pid,
                "node": owner,
                "key_range": ranges.get(pid, ("", "")),
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
    # simple cache to avoid recomputation when the dashboard refreshes rapidly
    cache = getattr(app.state, "_time_series_cache", None)
    now = time.time()
    if cache and now - cache.get("ts", 0) < 1:
        return cache["data"]

    cluster = app.state.cluster

    def collect(n):
        start = time.time()
        latency = None
        try:
            n.client.ping(n.node_id)
            latency = (time.time() - start) * 1000
        except Exception:
            pass
        rep = 0
        hint = 0
        try:
            req = replication_pb2.NodeInfoRequest(node_id=n.node_id)
            resp = n.client.stub.GetNodeInfo(req)
            rep = resp.replication_log_size
            hint = resp.hints_count
        except Exception:
            pass
        return latency, rep, hint

    with ThreadPoolExecutor(max_workers=len(cluster.nodes)) as ex:
        results = list(ex.map(collect, cluster.nodes))

    latencies = [lat for lat, _, _ in results if lat is not None]
    replog = sum(rep for _, rep, _ in results)
    hints = sum(h for _, _, h in results)
    total_ops = sum(cluster.get_partition_stats().values())
    elapsed = max(time.time() - getattr(app.state, "cluster_start", time.time()), 1)
    throughput = total_ops / elapsed
    data = {
        "latency_ms": latencies,
        "throughput": throughput,
        "replication_log_size": replog,
        "hints_count": hints,
    }
    app.state._time_series_cache = {"ts": now, "data": data}
    return data


@app.get("/cluster/events")
def cluster_events(offset: int = 0, limit: int | None = None) -> dict:
    """Return recent event log entries."""
    cluster = app.state.cluster
    cluster.event_logger.sync()
    events = cluster.event_logger.get_events(offset=offset, limit=limit)
    return {"events": events}


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


@app.get("/cluster/transactions")
def cluster_transactions() -> dict:
    """Return active transactions for each node in the cluster."""
    cluster = app.state.cluster
    results = []
    for n in cluster.nodes:
        try:
            tx_ids = n.client.list_transactions()
        except Exception:
            tx_ids = []
        results.append({"node": n.node_id, "tx_ids": tx_ids})
    return {"transactions": results}


@app.post("/cluster/transactions/{node}/{tx_id}/abort")
def abort_transaction(node: str, tx_id: str) -> dict:
    """Abort transaction ``tx_id`` on ``node``."""
    cluster = app.state.cluster
    n = cluster.nodes_by_id.get(node)
    if n is None:
        raise HTTPException(status_code=404, detail="node not found")
    try:
        n.client.abort_transaction(tx_id)
    except Exception:
        raise HTTPException(status_code=503, detail="unreachable")
    return {"status": "ok"}


@app.post("/cluster/actions/add_node")
def add_node() -> dict:
    """Add a new node to the cluster and return its id."""
    node = app.state.cluster.add_node()
    return {"status": "ok", "node_id": node.node_id}


@app.delete("/cluster/actions/remove_node/{node_id}")
def remove_node(node_id: str) -> dict:
    """Remove ``node_id`` from the cluster."""
    app.state.cluster.remove_node(node_id)
    return {"status": "ok"}


@app.post("/cluster/actions/check_hot_partitions")
def check_hot_partitions(threshold: float = 2.0, min_keys: int = 2) -> dict:
    """Check for hot partitions and split them if needed."""
    cluster = app.state.cluster
    cluster.check_hot_partitions(threshold=threshold, min_keys=min_keys)
    return {"status": "ok"}


@app.post("/cluster/actions/reset_metrics")
def reset_metrics() -> dict:
    """Reset partition and key frequency metrics."""
    app.state.cluster.reset_metrics()
    return {"status": "ok"}


@app.post("/cluster/actions/mark_hot_key")
def mark_hot_key(key: str, buckets: int, migrate: bool = False) -> dict:
    """Enable salting for ``key`` using ``buckets`` variants."""
    cluster = app.state.cluster
    cluster.mark_hot_key(key, buckets=buckets, migrate=migrate)
    return {"status": "ok"}


@app.post("/cluster/actions/split_partition")
def split_partition(pid: int, split_key: str | None = None) -> dict:
    """Split the partition ``pid`` at ``split_key`` if provided."""
    cluster = app.state.cluster
    try:
        cluster.split_partition(pid, split_key)
        return {"status": "ok"}
    except Exception as exc:
        return {"error": str(exc)}


@app.post("/cluster/actions/merge_partitions")
def merge_partitions(pid1: int, pid2: int) -> dict:
    """Merge adjacent partitions ``pid1`` and ``pid2``."""
    cluster = app.state.cluster
    try:
        cluster.merge_partitions(pid1, pid2)
        return {"status": "ok"}
    except Exception as exc:
        return {"error": str(exc)}


@app.post("/cluster/actions/rebalance")
def rebalance() -> dict:
    """Re-send the current partition map to all nodes."""
    cluster = app.state.cluster
    cluster.update_partition_map(manual=True)
    return {"status": "ok"}


@app.post("/nodes/{node_id}/stop")
def stop_node(node_id: str) -> dict:
    """Stop the node identified by ``node_id``."""
    app.state.cluster.stop_node(node_id)
    return {"status": "ok"}


@app.post("/nodes/{node_id}/start")
def start_node(node_id: str) -> dict:
    """Start the node identified by ``node_id``."""
    app.state.cluster.start_node(node_id)
    return {"status": "ok"}


@app.get("/nodes/{node_id}/replication_status")
def node_replication_status(node_id: str) -> dict:
    """Return replication status information for ``node_id``."""
    cluster = app.state.cluster
    node = cluster.nodes_by_id.get(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail="node not found")
    try:
        req = replication_pb2.NodeInfoRequest(node_id=node_id)
        resp = node.client.stub.GetReplicationStatus(req)
        return {
            "last_seen": dict(resp.last_seen),
            "hints": dict(resp.hints),
        }
    except Exception:
        raise HTTPException(status_code=503, detail="unreachable")


@app.get("/nodes/{node_id}/wal")
def node_wal(node_id: str, offset: int = 0, limit: int | None = None) -> dict:
    """Return Write Ahead Log entries stored on ``node_id`` with optional pagination."""
    cluster = app.state.cluster
    node = cluster.nodes_by_id.get(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail="node not found")
    try:
        entries = node.client.get_wal_entries()
        results = [
            {
                "type": e[0],
                "key": e[1],
                "value": e[2],
                "vector_clock": e[3],
            }
            for e in entries
        ]
        if offset < 0:
            offset = 0
        end = offset + limit if limit is not None else None
        return {"entries": results[offset:end]}
    except Exception:
        raise HTTPException(status_code=503, detail="unreachable")


@app.get("/nodes/{node_id}/memtable")
def node_memtable(node_id: str, offset: int = 0, limit: int | None = None) -> dict:
    """Return current MemTable contents for ``node_id`` with optional pagination."""
    cluster = app.state.cluster
    node = cluster.nodes_by_id.get(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail="node not found")
    try:
        entries = node.client.get_memtable_entries()
        results = [
            {
                "key": e[0],
                "value": e[1],
                "vector_clock": e[2],
            }
            for e in entries
        ]
        if offset < 0:
            offset = 0
        end = offset + limit if limit is not None else None
        return {"entries": results[offset:end]}
    except Exception:
        raise HTTPException(status_code=503, detail="unreachable")


@app.get("/nodes/{node_id}/sstables")
def node_sstables(node_id: str) -> dict:
    """Return metadata for SSTables stored by ``node_id``."""
    cluster = app.state.cluster
    node = cluster.nodes_by_id.get(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail="node not found")
    try:
        tables = node.client.get_sstables()
        results = [
            {
                "id": t[0],
                "level": t[1],
                "size": t[2],
                "item_count": t[3],
                "key_range": [t[4], t[5]],
            }
            for t in tables
        ]
        return {"tables": results}
    except Exception:
        raise HTTPException(status_code=503, detail="unreachable")


@app.get("/nodes/{node_id}/events")
def node_events(node_id: str, offset: int = 0, limit: int | None = None) -> dict:
    """Return recent event log entries for ``node_id``."""
    cluster = app.state.cluster
    logger = cluster.node_loggers.get(node_id)
    if logger is None:
        raise HTTPException(status_code=404, detail="node not found")
    logger.sync()
    events = logger.get_events(offset=offset, limit=limit)
    return {"events": events}


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


@app.get("/data/records")
def list_records_endpoint(
    offset: int = 0, limit: int | None = 100, query: str | None = None
) -> dict:
    """Return records stored in the cluster with optional pagination.

    ``limit`` defaults to 100 to avoid loading the entire dataset when many
    records exist.
    """
    cluster = app.state.cluster
    records = [
        {
            "partition_key": pk,
            "clustering_key": ck,
            "value": val,
        }
        for pk, ck, val in cluster.list_records(offset=offset, limit=limit, query=query)
    ]
    return {"records": records}


@app.post("/data/records")
def create_record(record: Record) -> dict:
    """Insert ``record`` into the cluster."""
    cluster = app.state.cluster
    cluster.put(0, record.partitionKey, record.clusteringKey, record.value)
    return {"status": "ok"}


@app.put("/data/records/{partition_key}/{clustering_key}")
def update_record(partition_key: str, clustering_key: str, value: str) -> dict:
    """Update an existing record."""
    cluster = app.state.cluster
    cluster.put(0, partition_key, clustering_key, value)
    return {"status": "ok"}


@app.delete("/data/records/{partition_key}/{clustering_key}")
def delete_record(partition_key: str, clustering_key: str) -> dict:
    """Delete a record from the cluster."""
    cluster = app.state.cluster
    cluster.delete(0, partition_key, clustering_key)
    return {"status": "ok"}


@app.get("/data/records/scan_range")
def scan_range(
    partition_key: str, start_ck: str, end_ck: str
) -> dict:
    """Return items for ``partition_key`` between ``start_ck`` and ``end_ck``."""
    cluster = app.state.cluster
    items = [
        {"clustering_key": ck, "value": val}
        for ck, val in cluster.get_range(partition_key, start_ck, end_ck)
    ]
    return {"items": items}


@app.get("/data/query_index")
def query_index(field: str, value: str) -> dict:
    """Return keys matching ``field``/``value`` from secondary indexes."""
    cluster = app.state.cluster
    keys = cluster.secondary_query(field, value)
    return {"keys": keys}


@app.post("/sql/query")
def sql_query(payload: dict) -> dict:
    """Execute a SELECT query and return rows."""
    sql = payload.get("sql", "")
    coordinator = QueryCoordinator(app.state.cluster.nodes)
    try:
        rows = coordinator.execute(sql)
    except Exception as exc:  # pragma: no cover - unexpected errors
        raise HTTPException(status_code=400, detail=str(exc))
    columns = list(rows[0].keys()) if rows else []
    return {
        "columns": [{"name": c, "type": "text"} for c in columns],
        "rows": rows,
    }


@app.post("/sql/explain")
def sql_explain(payload: dict) -> dict:
    """Return the planned execution tree for the given SQL."""
    sql = payload.get("sql", "")
    cluster = app.state.cluster
    node = cluster.nodes[0]
    db_path = os.path.join(cluster.base_path, node.node_id)

    class DummyNode:
        def __init__(self, db):
            self.db = db
            self.replication_log = {}

        def next_op_id(self):
            return "api:1"

        def save_replication_log(self):
            pass

        def replicate(self, *args, **kwargs):
            pass

    db = SimpleLSMDB(db_path=db_path)
    dummy = DummyNode(db)
    catalog = CatalogManager(dummy)
    planner = QueryPlanner(db, catalog, index_manager=object())
    try:
        query = parse_sql(sql)
        plan = planner.create_plan(query)
        result = plan.to_dict()
    except Exception as exc:  # pragma: no cover - unexpected errors
        db.close()
        raise HTTPException(status_code=400, detail=str(exc))
    db.close()
    return result


@app.post("/sql/execute")
def sql_execute(payload: dict) -> dict:
    """Execute a non-SELECT SQL statement."""
    sql = payload.get("sql", "")
    if sql.strip().upper().startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Use /sql/query for SELECT")
    try:
        app.state.cluster.nodes[0].client.execute_ddl(sql)
    except Exception as exc:  # pragma: no cover - runtime errors
        raise HTTPException(status_code=400, detail=str(exc))
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=False)

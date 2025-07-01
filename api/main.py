from fastapi import FastAPI
from database.replication import NodeCluster

app = FastAPI()


@app.on_event("startup")
def startup_event() -> None:
    """Initialize the cluster when the API starts."""
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


@app.get("/health")
def health() -> dict:
    """Return basic cluster information."""
    cluster = app.state.cluster
    return {"nodes": len(cluster.nodes)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=False)

"""Clustering and partitioning utilities."""

# Re-export commonly used functions without importing modules to avoid
# circular dependencies during package initialization.
from importlib import import_module

def __getattr__(name):
    if name in {
        "compose_key",
        "RangePartitioner",
        "HashPartitioner",
        "ConsistentHashPartitioner",
        "HashRing",
        "IndexManager",
        "GlobalIndexManager",
        "run_metadata_service",
        "run_router",
    }:
        module_map = {
            "compose_key": "partitioning",
            "RangePartitioner": "partitioning",
            "HashPartitioner": "partitioning",
            "ConsistentHashPartitioner": "partitioning",
            "HashRing": "hash_ring",
            "IndexManager": "index_manager",
            "GlobalIndexManager": "global_index_manager",
            "run_metadata_service": "metadata_service",
            "run_router": "router_server",
        }
        mod = import_module(f"{__name__}.{module_map[name]}")
        return getattr(mod, name)
    raise AttributeError(name)

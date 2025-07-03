import time
import os
import grpc
from . import replication_pb2, replication_pb2_grpc, router_pb2_grpc

class GRPCReplicaClient:
    """Simple gRPC client for replica nodes."""
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.channel = None
        self.stub = None
        self.heartbeat_stub = None
        self._ensure_channel()
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=self._reset_channel)

    def _ensure_channel(self):
        if self.channel is None:
            self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
            self.stub = replication_pb2_grpc.ReplicaStub(self.channel)
            self.heartbeat_stub = replication_pb2_grpc.HeartbeatServiceStub(self.channel)

    def _reset_channel(self):
        if self.channel is not None:
            try:
                self.channel.close()
            except Exception:
                pass
        self.channel = None
        self.stub = None
        self.heartbeat_stub = None

    def put(
        self,
        key,
        value,
        timestamp=None,
        node_id="",
        op_id="",
        vector=None,
        hinted_for="",
        tx_id: str = "",
    ):
        self._ensure_channel()
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        if vector is None:
            vv = replication_pb2.VersionVector(items={})
        elif isinstance(vector, replication_pb2.VersionVector):
            vv = vector
        else:
            vv = replication_pb2.VersionVector(items=dict(vector))
        request = replication_pb2.KeyValue(
            key=key,
            value=value,
            timestamp=timestamp,
            node_id=node_id,
            op_id=op_id,
            vector=vv,
            hinted_for=hinted_for,
            tx_id=tx_id,
        )
        self.stub.Put(request)

    def delete(
        self,
        key,
        timestamp=None,
        node_id="",
        op_id="",
        vector=None,
        hinted_for="",
        tx_id: str = "",
    ):
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        if vector is None:
            vv = replication_pb2.VersionVector(items={})
        elif isinstance(vector, replication_pb2.VersionVector):
            vv = vector
        else:
            vv = replication_pb2.VersionVector(items=dict(vector))
        request = replication_pb2.KeyRequest(
            key=key,
            timestamp=timestamp,
            node_id=node_id,
            op_id=op_id,
            vector=vv,
            hinted_for=hinted_for,
            tx_id=tx_id,
        )
        self._ensure_channel()
        self.stub.Delete(request)

    def increment(self, key, amount):
        """Perform atomic increment on the given key."""
        self._ensure_channel()
        req = replication_pb2.IncrementRequest(key=key, amount=int(amount))
        self.stub.Increment(req)

    def get(self, key, *, tx_id: str = ""):
        self._ensure_channel()
        request = replication_pb2.KeyRequest(
            key=key, timestamp=0, node_id="", tx_id=tx_id
        )
        response = self.stub.Get(request)
        results = []
        for item in response.values:
            val = item.value if item.value else None
            vec = dict(item.vector.items)
            results.append((val, item.timestamp, vec))
        return results

    def get_for_update(self, key, tx_id: str):
        self._ensure_channel()
        request = replication_pb2.KeyRequest(
            key=key, timestamp=0, node_id="", tx_id=tx_id
        )
        response = self.stub.GetForUpdate(request)
        results = []
        for item in response.values:
            val = item.value if item.value else None
            vec = dict(item.vector.items)
            results.append((val, item.timestamp, vec))
        return results

    def get_for_update(self, key, tx_id: str):
        self._ensure_channel()
        request = replication_pb2.KeyRequest(
            key=key, timestamp=0, node_id="", tx_id=tx_id
        )
        response = self.stub.GetForUpdate(request)
        results = []
        for item in response.values:
            val = item.value if item.value else None
            vec = dict(item.vector.items)
            results.append((val, item.timestamp, vec))
        return results

    def scan_range(self, partition_key, start_ck, end_ck):
        self._ensure_channel()
        req = replication_pb2.RangeRequest(
            partition_key=partition_key,
            start_ck=start_ck,
            end_ck=end_ck,
        )
        resp = self.stub.ScanRange(req)
        results = []
        for it in resp.items:
            results.append(
                (
                    it.clustering_key,
                    it.value,
                    it.timestamp,
                    dict(it.vector.items),
                )
            )
        return results

    def list_by_index(self, field: str, value) -> list[str]:
        self._ensure_channel()
        req = replication_pb2.IndexQuery(field=field, value=str(value))
        resp = self.stub.ListByIndex(req)
        return list(resp.keys)

    def begin_transaction(self) -> str:
        self._ensure_channel()
        resp = self.stub.BeginTransaction(replication_pb2.Empty())
        return resp.id

    def commit_transaction(self, tx_id: str) -> None:
        self._ensure_channel()
        req = replication_pb2.TransactionControl(tx_id=tx_id)
        self.stub.CommitTransaction(req)

    def abort_transaction(self, tx_id: str) -> None:
        self._ensure_channel()
        req = replication_pb2.TransactionControl(tx_id=tx_id)
        self.stub.AbortTransaction(req)

    def fetch_updates(self, last_seen: dict, ops=None, segment_hashes=None, trees=None):
        self._ensure_channel()
        """Fetch updates from peer optionally sending our pending ops, hashes and trees."""
        vv = replication_pb2.VersionVector(items=last_seen)
        ops = ops or []
        hashes = segment_hashes or {}
        trees = trees or []
        for op in ops:
            if not op.vector.items:
                op.vector.MergeFrom(vv)
        req = replication_pb2.FetchRequest(vector=vv, ops=ops, segment_hashes=hashes, trees=trees)
        return self.stub.FetchUpdates(req)

    def update_partition_map(self, mapping: dict[int, str] | None):
        self._ensure_channel()
        """Send a new partition map to the replica."""
        req = replication_pb2.PartitionMap(items=mapping or {})
        self.stub.UpdatePartitionMap(req)

    def update_hash_ring(self, entries):
        self._ensure_channel()
        items = [replication_pb2.HashRingEntry(hash=str(h), node_id=nid) for h, nid in (entries or [])]
        req = replication_pb2.HashRing(items=items)
        self.stub.UpdateHashRing(req)

    def get_wal_entries(self) -> list[tuple[str, str, str | None, dict]]:
        self._ensure_channel()
        """Return WAL operations currently stored on the remote node."""
        req = replication_pb2.NodeInfoRequest()
        resp = self.stub.GetWalEntries(req)
        results = []
        for entry in resp.entries:
            results.append(
                (
                    entry.type,
                    entry.key,
                    entry.value if entry.value else None,
                    dict(entry.vector.items),
                )
            )
        return results

    def get_memtable_entries(self) -> list[tuple[str, str | None, dict]]:
        self._ensure_channel()
        """Return key/value pairs stored in the remote MemTable."""
        req = replication_pb2.NodeInfoRequest()
        resp = self.stub.GetMemtableEntries(req)
        results = []
        for entry in resp.entries:
            results.append(
                (
                    entry.key,
                    entry.value if entry.value else None,
                    dict(entry.vector.items),
                )
            )
        return results

    def get_sstables(self) -> list[tuple[str, int, int, int, str, str]]:
        self._ensure_channel()
        """Return metadata describing SSTables stored on disk."""
        req = replication_pb2.NodeInfoRequest()
        resp = self.stub.GetSSTables(req)
        results = []
        for table in resp.tables:
            results.append(
                (
                    table.id,
                    table.level,
                    table.size,
                    table.item_count,
                    table.start_key,
                    table.end_key,
                )
            )
        return results

    def get_sstable_content(self, sstable_id: str, node_id: str = "") -> list[tuple[str, str | None, dict]]:
        self._ensure_channel()
        """Return all key/value pairs stored in the given SSTable."""
        req = replication_pb2.SSTableContentRequest(node_id=node_id, sstable_id=sstable_id)
        resp = self.stub.GetSSTableContent(req)
        results = []
        for entry in resp.entries:
            results.append(
                (
                    entry.key,
                    entry.value if entry.value else None,
                    dict(entry.vector.items),
                )
            )
        return results

    def ping(self, node_id: str = ""):
        self._ensure_channel()
        """Send a heartbeat ping to the remote peer."""
        req = replication_pb2.Heartbeat(node_id=node_id)
        self.heartbeat_stub.Ping(req)

    def close(self):
        self.channel.close()

    def __getstate__(self):
        return {"host": self.host, "port": self.port}

    def __setstate__(self, state):
        self.host = state["host"]
        self.port = state["port"]
        self.channel = None
        self.stub = None
        self.heartbeat_stub = None
        self._ensure_channel()


class GRPCRouterClient:
    """gRPC client for the router service mirroring ``GRPCReplicaClient``."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.channel = None
        self.stub = None
        self._ensure_channel()
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=self._reset_channel)

    def _ensure_channel(self):
        if self.channel is None:
            self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
            self.stub = router_pb2_grpc.RouterStub(self.channel)

    def _reset_channel(self):
        if self.channel is not None:
            try:
                self.channel.close()
            except Exception:
                pass
        self.channel = None
        self.stub = None

    def put(
        self,
        key,
        value,
        timestamp=None,
        node_id="",
        op_id="",
        vector=None,
        hinted_for="",
        tx_id: str = "",
    ):
        self._ensure_channel()
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        if vector is None:
            vv = replication_pb2.VersionVector(items={})
        elif isinstance(vector, replication_pb2.VersionVector):
            vv = vector
        else:
            vv = replication_pb2.VersionVector(items=dict(vector))
        request = replication_pb2.KeyValue(
            key=key,
            value=value,
            timestamp=timestamp,
            node_id=node_id,
            op_id=op_id,
            vector=vv,
            hinted_for=hinted_for,
            tx_id=tx_id,
        )
        self.stub.Put(request)

    def delete(
        self,
        key,
        timestamp=None,
        node_id="",
        op_id="",
        vector=None,
        hinted_for="",
        tx_id: str = "",
    ):
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        if vector is None:
            vv = replication_pb2.VersionVector(items={})
        elif isinstance(vector, replication_pb2.VersionVector):
            vv = vector
        else:
            vv = replication_pb2.VersionVector(items=dict(vector))
        request = replication_pb2.KeyRequest(
            key=key,
            timestamp=timestamp,
            node_id=node_id,
            op_id=op_id,
            vector=vv,
            hinted_for=hinted_for,
            tx_id=tx_id,
        )
        self._ensure_channel()
        self.stub.Delete(request)

    def get(self, key, *, tx_id: str = ""):
        self._ensure_channel()
        request = replication_pb2.KeyRequest(
            key=key, timestamp=0, node_id="", tx_id=tx_id
        )
        response = self.stub.Get(request)
        results = []
        for item in response.values:
            val = item.value if item.value else None
            vec = dict(item.vector.items)
            results.append((val, item.timestamp, vec))
        return results

    def scan_range(self, partition_key, start_ck, end_ck):
        self._ensure_channel()
        req = replication_pb2.RangeRequest(
            partition_key=partition_key,
            start_ck=start_ck,
            end_ck=end_ck,
        )
        resp = self.stub.ScanRange(req)
        results = []
        for it in resp.items:
            results.append(
                (
                    it.clustering_key,
                    it.value,
                    it.timestamp,
                    dict(it.vector.items),
                )
            )
        return results

    def close(self):
        self.channel.close()

    def __getstate__(self):
        return {"host": self.host, "port": self.port}

    def __setstate__(self, state):
        self.host = state["host"]
        self.port = state["port"]
        self.channel = None
        self.stub = None
        self._ensure_channel()

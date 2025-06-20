import time
import grpc
from . import replication_pb2, replication_pb2_grpc

class GRPCReplicaClient:
    """Simple gRPC client for replica nodes."""
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = replication_pb2_grpc.ReplicaStub(self.channel)
        self.heartbeat_stub = replication_pb2_grpc.HeartbeatServiceStub(self.channel)

    def put(
        self,
        key,
        value,
        timestamp=None,
        node_id="",
        op_id="",
        vector=None,
        hinted_for="",
    ):
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
        )
        self.stub.Put(request)

    def delete(self, key, timestamp=None, node_id="", op_id="", vector=None, hinted_for=""):
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
        )
        self.stub.Delete(request)

    def get(self, key):
        request = replication_pb2.KeyRequest(key=key, timestamp=0, node_id="")
        response = self.stub.Get(request)
        results = []
        for item in response.values:
            val = item.value if item.value else None
            vec = dict(item.vector.items)
            results.append((val, item.timestamp, vec))
        return results

    def scan_range(self, partition_key, start_ck, end_ck):
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
        req = replication_pb2.IndexQuery(field=field, value=str(value))
        resp = self.stub.ListByIndex(req)
        return list(resp.keys)

    def fetch_updates(self, last_seen: dict, ops=None, segment_hashes=None, trees=None):
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
        """Send a new partition map to the replica."""
        req = replication_pb2.PartitionMap(items=mapping or {})
        self.stub.UpdatePartitionMap(req)

    def ping(self, node_id: str = ""):
        """Send a heartbeat ping to the remote peer."""
        req = replication_pb2.Heartbeat(node_id=node_id)
        self.heartbeat_stub.Ping(req)

    def close(self):
        self.channel.close()

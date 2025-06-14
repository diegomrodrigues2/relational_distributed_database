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

    def put(self, key, value, timestamp=None, node_id="", op_id="", vector=None):
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
        )
        self.stub.Put(request)

    def delete(self, key, timestamp=None, node_id="", op_id="", vector=None):
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
        )
        self.stub.Delete(request)

    def get(self, key):
        request = replication_pb2.KeyRequest(key=key, timestamp=0, node_id="")
        response = self.stub.Get(request)
        value = response.value if response.value else None
        return value, response.timestamp

    def fetch_updates(self, last_seen: dict, ops=None, segment_hashes=None):
        """Fetch updates from peer optionally sending our pending ops and hashes."""
        vv = replication_pb2.VersionVector(items=last_seen)
        ops = ops or []
        hashes = segment_hashes or {}
        for op in ops:
            if not op.vector.items:
                op.vector.MergeFrom(vv)
        req = replication_pb2.FetchRequest(vector=vv, ops=ops, segment_hashes=hashes)
        return self.stub.FetchUpdates(req)

    def close(self):
        self.channel.close()

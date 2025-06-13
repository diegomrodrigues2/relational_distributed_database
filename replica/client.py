import time
import grpc
from . import replication_pb2, replication_pb2_grpc

class GRPCReplicaClient:
    """Simple gRPC client for replica nodes."""
    def __init__(self, host: str, port: int):
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = replication_pb2_grpc.ReplicaStub(self.channel)

    def put(self, key, value, timestamp=None, node_id="", op_id=""):
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        request = replication_pb2.KeyValue(
            key=key,
            value=value,
            timestamp=timestamp,
            node_id=node_id,
            op_id=op_id,
        )
        self.stub.Put(request)

    def delete(self, key, timestamp=None, node_id="", op_id=""):
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        request = replication_pb2.KeyRequest(
            key=key,
            timestamp=timestamp,
            node_id=node_id,
            op_id=op_id,
        )
        self.stub.Delete(request)

    def get(self, key):
        request = replication_pb2.KeyRequest(key=key, timestamp=0, node_id="")
        response = self.stub.Get(request)
        return response.value if response.value else None

    def close(self):
        self.channel.close()

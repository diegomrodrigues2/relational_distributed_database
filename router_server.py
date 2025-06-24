import grpc
from concurrent import futures

from replica import replication_pb2, replication_pb2_grpc
from replica.client import GRPCReplicaClient


class RouterService(replication_pb2_grpc.ReplicaServicer):
    """gRPC service forwarding requests to the correct partition owner."""

    def __init__(self, cluster):
        self.cluster = cluster
        self.partition_map = cluster.get_partition_map()
        self.clients_by_id = {nid: node.client for nid, node in cluster.nodes_by_id.items()}
        self.server = None

    def update_partition_map(self, mapping):
        self.partition_map = dict(mapping or {})

    # internal -----------------------------------------------------------
    def _split_key(self, key: str):
        if "|" in key:
            pk, ck = key.split("|", 1)
        else:
            pk, ck = key, None
        return pk, ck

    def _client_for(self, key: str) -> GRPCReplicaClient:
        pk, ck = self._split_key(key)
        pid = self.cluster.get_partition_id(pk, ck)
        owner = self.partition_map.get(pid)
        client = self.clients_by_id[owner]
        # Ensure gRPC channel is initialized after forking
        client._ensure_channel()
        return client

    # RPC methods -------------------------------------------------------
    def Put(self, request, context):
        client = self._client_for(request.key)
        return client.stub.Put(request)

    def Delete(self, request, context):
        client = self._client_for(request.key)
        return client.stub.Delete(request)

    def Get(self, request, context):
        client = self._client_for(request.key)
        return client.stub.Get(request)

    def ScanRange(self, request, context):
        pid = self.cluster.get_partition_id(request.partition_key)
        owner = self.partition_map.get(pid)
        client = self.clients_by_id[owner]
        return client.stub.ScanRange(request)

    def UpdatePartitionMap(self, request, context):
        self.update_partition_map(request.items)
        return replication_pb2.Empty()


def run_router(cluster, host="localhost", port=7000):
    """Launch a RouterService for ``cluster`` on the given ``host``/``port``."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service = RouterService(cluster)
    replication_pb2_grpc.add_ReplicaServicer_to_server(service, server)
    server.add_insecure_port(f"{host}:{port}")
    service.server = server
    server.start()
    server.wait_for_termination()


import grpc
from concurrent import futures
import threading
import time

from replica import replication_pb2, replication_pb2_grpc
from replica import router_pb2_grpc, metadata_pb2, metadata_pb2_grpc
from replica.client import GRPCReplicaClient


class RouterService(router_pb2_grpc.RouterServicer):
    """gRPC service forwarding requests to the correct partition owner."""

    def __init__(self, cluster, registry_host=None, registry_port=None):
        self.cluster = cluster
        self.partition_map = cluster.get_partition_map()
        self.clients_by_id = {nid: node.client for nid, node in cluster.nodes_by_id.items()}
        self.server = None
        self.registry_host = registry_host
        self.registry_port = registry_port
        self._registry_channel = None
        self._registry_stub = None
        self._watch_stop = threading.Event()
        self._watch_thread = None
        if registry_host and registry_port:
            self._registry_channel = grpc.insecure_channel(f"{registry_host}:{registry_port}")
            self._registry_stub = metadata_pb2_grpc.MetadataServiceStub(self._registry_channel)
            try:
                state = self._registry_stub.GetClusterState(replication_pb2.Empty())
                self.update_partition_map(state.partition_map.items)
            except Exception:
                pass
            self._start_watch_thread()

    def update_partition_map(self, mapping):
        self.partition_map = dict(mapping or {})

    def _watch_loop(self):
        if not self._registry_stub:
            return
        while not self._watch_stop.is_set():
            try:
                stream = self._registry_stub.WatchClusterState(replication_pb2.Empty())
                for state in stream:
                    self.update_partition_map(state.partition_map.items)
                    if self._watch_stop.is_set():
                        break
            except Exception:
                time.sleep(1.0)

    def _start_watch_thread(self):
        if self._watch_thread and self._watch_thread.is_alive():
            return
        t = threading.Thread(target=self._watch_loop, daemon=True)
        self._watch_thread = t
        t.start()

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


def run_router(cluster, host="localhost", port=7000, registry_addr=None):
    """Launch a RouterService for ``cluster`` on the given ``host``/``port``."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    if registry_addr:
        rh, rp = registry_addr
    else:
        rh = rp = None
    service = RouterService(cluster, registry_host=rh, registry_port=rp)
    router_pb2_grpc.add_RouterServicer_to_server(service, server)
    server.add_insecure_port(f"{host}:{port}")
    service.server = server
    server.start()
    server.wait_for_termination()


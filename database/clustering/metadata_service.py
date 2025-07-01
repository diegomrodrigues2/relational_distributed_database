import time
import grpc
from concurrent import futures
import threading
import queue

from ..replication.replica import metadata_pb2, metadata_pb2_grpc, replication_pb2


class MetadataService(metadata_pb2_grpc.MetadataServiceServicer):
    """Simple registry of active nodes and partition mapping."""

    def __init__(self):
        self.nodes: dict[str, tuple[str, int]] = {}
        self.last_seen: dict[str, float] = {}
        self.partition_map: dict[int, str] = {}
        self.server = None
        self._watchers: list[queue.Queue] = []

    def _broadcast(self) -> None:
        state = self._cluster_state()
        for q in list(self._watchers):
            try:
                q.put_nowait(state)
            except Exception:
                pass

    # internal helpers -------------------------------------------------
    def _cluster_state(self) -> metadata_pb2.ClusterState:
        nodes = [
            metadata_pb2.NodeInfo(node_id=nid, host=h, port=p)
            for nid, (h, p) in self.nodes.items()
        ]
        pmap = replication_pb2.PartitionMap(items=self.partition_map)
        return metadata_pb2.ClusterState(nodes=nodes, partition_map=pmap)

    # RPC methods ------------------------------------------------------
    def RegisterNode(self, request, context):
        node = request.node
        self.nodes[node.node_id] = (node.host, node.port)
        self.last_seen[node.node_id] = time.time()
        state = self._cluster_state()
        self._broadcast()
        return state

    def Heartbeat(self, request, context):
        self.last_seen[request.node_id] = time.time()
        state = self._cluster_state()
        self._broadcast()
        return state

    def GetClusterState(self, request, context):
        return self._cluster_state()

    def WatchClusterState(self, request, context):
        q: queue.Queue = queue.Queue()
        self._watchers.append(q)
        def _remove():
            if q in self._watchers:
                self._watchers.remove(q)
            return True
        context.add_callback(_remove)
        q.put(self._cluster_state())
        while True:
            try:
                state = q.get(timeout=1.0)
            except queue.Empty:
                if not context.is_active():
                    break
                continue
            yield state

    def UpdateClusterState(self, request, context):
        """Replace the registry data with provided cluster state."""
        self.nodes = {n.node_id: (n.host, n.port) for n in request.nodes}
        self.partition_map = dict(request.partition_map.items)
        self._broadcast()
        return replication_pb2.Empty()

    # helpers for the cluster -----------------------------------------
    def update_partition_map(self, mapping: dict[int, str]):
        self.partition_map = dict(mapping)
        self._broadcast()


def run_metadata_service(host="localhost", port=9100):
    """Start a MetadataService on the given host/port."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service = MetadataService()
    metadata_pb2_grpc.add_MetadataServiceServicer_to_server(service, server)
    server.add_insecure_port(f"{host}:{port}")
    service.server = server
    server.start()
    server.wait_for_termination()

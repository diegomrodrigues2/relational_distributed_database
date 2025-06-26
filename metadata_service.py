import time
import grpc
from concurrent import futures

from replica import metadata_pb2, metadata_pb2_grpc, replication_pb2


class MetadataService(metadata_pb2_grpc.MetadataServiceServicer):
    """Simple registry of active nodes and partition mapping."""

    def __init__(self):
        self.nodes: dict[str, tuple[str, int]] = {}
        self.last_seen: dict[str, float] = {}
        self.partition_map: dict[int, str] = {}
        self.server = None

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
        return self._cluster_state()

    def Heartbeat(self, request, context):
        self.last_seen[request.node_id] = time.time()
        return self._cluster_state()

    def GetClusterState(self, request, context):
        return self._cluster_state()

    def UpdateClusterState(self, request, context):
        """Replace the registry data with provided cluster state."""
        self.nodes = {n.node_id: (n.host, n.port) for n in request.nodes}
        self.partition_map = dict(request.partition_map.items)
        return replication_pb2.Empty()

    # helpers for the cluster -----------------------------------------
    def update_partition_map(self, mapping: dict[int, str]):
        self.partition_map = dict(mapping)


def run_metadata_service(host="localhost", port=9100):
    """Start a MetadataService on the given host/port."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service = MetadataService()
    metadata_pb2_grpc.add_MetadataServiceServicer_to_server(service, server)
    server.add_insecure_port(f"{host}:{port}")
    service.server = server
    server.start()
    server.wait_for_termination()

"""gRPC node server with simple multi-leader replication."""

import threading
import time
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from lamport import LamportClock
from lsm_db import SimpleLSMDB
from . import replication_pb2, replication_pb2_grpc
from .client import GRPCReplicaClient


class ReplicaService(replication_pb2_grpc.ReplicaServicer):
    """Service exposing database operations."""

    def __init__(self, node):
        self._node = node

    def Put(self, request, context):
        self._node.clock.update(request.timestamp)
        _, current_ts = self._node.db.get_record(request.key)
        if current_ts is None or request.timestamp > current_ts:
            self._node.db.put(request.key, request.value, timestamp=request.timestamp)
        if request.node_id == self._node.node_id:
            op_id = self._node.next_op_id()
            self._node.replication_log[op_id] = (request.key, request.value, request.timestamp)
            self._node.replicate("PUT", request.key, request.value, request.timestamp, op_id=op_id)
        return replication_pb2.Empty()

    def Delete(self, request, context):
        self._node.clock.update(request.timestamp)
        _, current_ts = self._node.db.get_record(request.key)
        if current_ts is None or request.timestamp > current_ts:
            self._node.db.delete(request.key, timestamp=request.timestamp)
        if request.node_id == self._node.node_id:
            op_id = self._node.next_op_id()
            self._node.replication_log[op_id] = (request.key, None, request.timestamp)
            self._node.replicate("DELETE", request.key, None, request.timestamp, op_id=op_id)
        return replication_pb2.Empty()

    def Get(self, request, context):
        value = self._node.db.get(request.key)
        if value is None:
            value = ""
        return replication_pb2.ValueResponse(value=value)


class NodeServer:
    """Encapsulates gRPC server and replication logic for a node."""

    local_seq: int
    last_seen: dict[str, int]
    replication_log: dict[str, tuple]

    def __init__(self, db_path, host="localhost", port=8000, node_id="node", peers=None):
        self.db = SimpleLSMDB(db_path=db_path)
        self.host = host
        self.port = port
        self.node_id = node_id
        self.peers = peers or []

        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        replication_pb2_grpc.add_ReplicaServicer_to_server(
            ReplicaService(self), self.server
        )

        self.health_servicer = health.HealthServicer()
        health_pb2_grpc.add_HealthServicer_to_server(self.health_servicer, self.server)
        self.health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

        self.server.add_insecure_port(f"{host}:{port}")

        self.clock = LamportClock()
        self.local_seq = 0
        self.last_seen: dict[str, int] = {}
        self.replication_log: dict[str, tuple] = {}
        self.peer_clients = []
        for ph, pp in self.peers:
            if ph == self.host and pp == self.port:
                continue
            self.peer_clients.append(GRPCReplicaClient(ph, pp))

    def next_op_id(self) -> str:
        """Return next operation identifier."""
        self.local_seq += 1
        return f"{self.node_id}:{self.local_seq}"

    # replication helpers -------------------------------------------------
    def replicate(self, op, key, value, timestamp, op_id=""):
        def _send(client):
            try:
                if op == "PUT":
                    client.put(
                        key,
                        value,
                        timestamp=timestamp,
                        node_id=self.node_id,
                        op_id=op_id,
                    )
                else:
                    client.delete(
                        key,
                        timestamp=timestamp,
                        node_id=self.node_id,
                        op_id=op_id,
                    )
            except Exception as exc:
                print(f"Falha ao replicar para {client.channel.target}: {exc}")
        for c in self.peer_clients:
            threading.Thread(target=_send, args=(c,), daemon=True).start()

    # lifecycle -----------------------------------------------------------
    def start(self):
        self.server.start()
        self.server.wait_for_termination()

    def stop(self):
        for c in self.peer_clients:
            c.close()
        self.server.stop(0).wait()


def run_server(db_path, host="localhost", port=8000, node_id="node", peers=None):
    node = NodeServer(db_path, host, port, node_id=node_id, peers=peers)
    node.start()


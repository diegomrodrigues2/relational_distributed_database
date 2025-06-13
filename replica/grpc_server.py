"""gRPC node server with simple multi-leader replication."""

import threading
import time
import json
import os
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

        origem = seq = None
        apply_update = True
        if request.op_id:
            origem, seq = request.op_id.split(":")
            seq = int(seq)
            last = self._node.last_seen.get(origem, 0)
            if seq > last:
                self._node.last_seen[origem] = seq
            else:
                apply_update = False

        if apply_update:
            _, current_ts = self._node.db.get_record(request.key)
            if current_ts is None or request.timestamp > current_ts:
                self._node.db.put(
                    request.key, request.value, timestamp=request.timestamp
                )

        if request.node_id == self._node.node_id and apply_update:
            op_id = self._node.next_op_id()
            self._node.replication_log[op_id] = (
                request.key,
                request.value,
                request.timestamp,
            )
            self._node.replicate(
                "PUT", request.key, request.value, request.timestamp, op_id=op_id
            )

        return replication_pb2.Empty()

    def Delete(self, request, context):
        self._node.clock.update(request.timestamp)

        origem = seq = None
        apply_update = True
        if request.op_id:
            origem, seq = request.op_id.split(":")
            seq = int(seq)
            last = self._node.last_seen.get(origem, 0)
            if seq > last:
                self._node.last_seen[origem] = seq
            else:
                apply_update = False

        if apply_update:
            _, current_ts = self._node.db.get_record(request.key)
            if current_ts is None or request.timestamp > current_ts:
                self._node.db.delete(request.key, timestamp=request.timestamp)

        if request.node_id == self._node.node_id and apply_update:
            op_id = self._node.next_op_id()
            self._node.replication_log[op_id] = (request.key, None, request.timestamp)
            self._node.replicate(
                "DELETE", request.key, None, request.timestamp, op_id=op_id
            )

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

    def __init__(
        self, db_path, host="localhost", port=8000, node_id="node", peers=None
    ):
        self.db_path = db_path
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
        self._cleanup_stop = threading.Event()
        self._cleanup_thread = None
        self._replay_stop = threading.Event()
        self._replay_thread = None
        self.peer_clients = []
        for ph, pp in self.peers:
            if ph == self.host and pp == self.port:
                continue
            self.peer_clients.append(GRPCReplicaClient(ph, pp))

    # persistence helpers ------------------------------------------------
    def _last_seen_file(self) -> str:
        return os.path.join(self.db_path, "last_seen.json")

    def load_last_seen(self) -> None:
        """Load last_seen from JSON file if available."""
        path = self._last_seen_file()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                try:
                    self.last_seen = json.load(f)
                except Exception:
                    self.last_seen = {}

    def save_last_seen(self) -> None:
        """Persist last_seen to JSON file."""
        path = self._last_seen_file()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.last_seen, f)

    def next_op_id(self) -> str:
        """Return next operation identifier."""
        self.local_seq += 1
        return f"{self.node_id}:{self.local_seq}"

    def cleanup_replication_log(self) -> None:
        """Remove acknowledged operations from replication_log."""
        if not self.last_seen:
            return
        min_seen = min(self.last_seen.values())
        to_remove = [
            op_id
            for op_id in list(self.replication_log.keys())
            if int(op_id.split(":")[1]) <= int(min_seen)
        ]
        for op_id in to_remove:
            self.replication_log.pop(op_id, None)

    def _replay_replication_log(self) -> None:
        """Resend operations from replication_log to peers."""
        for op_id, (key, value, ts) in list(self.replication_log.items()):
            for client in self.peer_clients:
                try:
                    if value is None:
                        client.delete(
                            key, timestamp=ts, node_id=self.node_id, op_id=op_id
                        )
                    else:
                        client.put(
                            key, value, timestamp=ts, node_id=self.node_id, op_id=op_id
                        )
                except Exception:
                    pass

    def _cleanup_loop(self) -> None:
        while not self._cleanup_stop.is_set():
            self.cleanup_replication_log()
            time.sleep(1)

    def _replay_loop(self) -> None:
        while not self._replay_stop.is_set():
            self._replay_replication_log()
            time.sleep(0.5)

    def _start_cleanup_thread(self) -> None:
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            return
        t = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread = t
        t.start()

    def _start_replay_thread(self) -> None:
        if self._replay_thread and self._replay_thread.is_alive():
            return
        t = threading.Thread(target=self._replay_loop, daemon=True)
        self._replay_thread = t
        t.start()

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
                print(f"Falha ao replicar: {exc}")

        for c in self.peer_clients:
            threading.Thread(target=_send, args=(c,), daemon=True).start()

    # lifecycle -----------------------------------------------------------
    def start(self):
        self.load_last_seen()
        self._start_cleanup_thread()
        self._start_replay_thread()
        self.server.start()
        self.server.wait_for_termination()

    def stop(self):
        self.save_last_seen()
        self._cleanup_stop.set()
        self._replay_stop.set()
        if self._cleanup_thread:
            self._cleanup_thread.join()
        if self._replay_thread:
            self._replay_thread.join()
        for c in self.peer_clients:
            c.close()
        self.server.stop(0).wait()


def run_server(db_path, host="localhost", port=8000, node_id="node", peers=None):
    node = NodeServer(db_path, host, port, node_id=node_id, peers=peers)
    node.start()

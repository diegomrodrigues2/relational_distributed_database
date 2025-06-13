"""Reusable gRPC node server used by leader and followers."""

import threading
import time
from lamport import LamportClock
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from lsm_db import SimpleLSMDB
from . import replication_pb2
from . import replication_pb2_grpc


class ReplicaService(replication_pb2_grpc.ReplicaServicer):
    """gRPC service exposing database operations."""

    def __init__(self, node):
        self._node = node

    def Put(self, request, context):
        self._node.clock.update(request.timestamp)
        self._node.db.put(request.key, request.value, timestamp=request.timestamp)
        return replication_pb2.Empty()

    def Delete(self, request, context):
        ts = self._node.clock.tick()
        self._node.db.delete(request.key, timestamp=ts)
        return replication_pb2.Empty()

    def Get(self, request, context):
        value = self._node.db.get(request.key)
        if value is None:
            value = ""
        return replication_pb2.ValueResponse(value=value)


class HeartbeatService(replication_pb2_grpc.HeartbeatServiceServicer):
    """Heartbeat service shared by leader and followers."""

    def __init__(self, node):
        self._node = node

    def Ping(self, request, context):
        if request.node_id == "leader":
            self._node.last_leader_heartbeat = time.time()
        else:
            self._node.record_heartbeat(request.node_id)
        return replication_pb2.Empty()


class NodeServer:
    """Encapsulates gRPC server and heartbeat logic for a replica node."""

    def __init__(self, db_path, host="localhost", port=8000,
                 leader_host=None, leader_port=None, node_id="follower",
                 priority_index=0, role="follower"):
        self.db = SimpleLSMDB(db_path=db_path)
        self.host = host
        self.port = port
        self.node_id = node_id
        self.leader_host = leader_host
        self.leader_port = leader_port
        self.role = role
        self.priority_index = priority_index

        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        replication_pb2_grpc.add_ReplicaServicer_to_server(
            ReplicaService(self), self.server
        )
        replication_pb2_grpc.add_HeartbeatServiceServicer_to_server(
            HeartbeatService(self), self.server
        )

        self.health_servicer = health.HealthServicer()
        health_pb2_grpc.add_HealthServicer_to_server(
            self.health_servicer, self.server
        )
        self.health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

        self.server.add_insecure_port(f"{host}:{port}")
        self._stop = threading.Event()
        self._hb_stop = threading.Event()
        self._hb_thread = None
        self.last_leader_heartbeat = time.time()
        self._heartbeat_callbacks = {}
        self.clock = LamportClock()

    def record_heartbeat(self, node_id):
        """Called when a follower heartbeat is received."""
        self._heartbeat_callbacks[node_id] = time.time()

    def become_leader(self):
        """Switch this node to leader role."""
        if self.role == "leader":
            return
        self.role = "leader"
        # stop sending heartbeats to the former leader
        if self._hb_thread:
            self._hb_stop.set()
            self._hb_thread.join(timeout=1)
            self._hb_thread = None
        self.leader_host = None
        self.leader_port = None
        print(f"{self.node_id} tornou-se LÍDER")

    def start(self):
        self.server.start()
        if self.role == "follower" and self.leader_host and self.leader_port:
            self._start_heartbeat_sender()
        self._start_leader_monitor()
        self.server.wait_for_termination()

    # internal helpers -----------------------------------------------------
    def _start_heartbeat_sender(self):
        channel = grpc.insecure_channel(f"{self.leader_host}:{self.leader_port}")
        stub = replication_pb2_grpc.HeartbeatServiceStub(channel)
        req = replication_pb2.Heartbeat(node_id=self.node_id)

        self._hb_stop.clear()

        def _loop():
            while not self._stop.is_set() and not self._hb_stop.is_set():
                try:
                    stub.Ping(req)
                except Exception as exc:
                    print(f"Falha ao enviar heartbeat ao líder: {exc}")
                time.sleep(1)

        self._hb_thread = threading.Thread(target=_loop, daemon=True)
        self._hb_thread.start()

    def _start_leader_monitor(self):
        def _loop():
            while not self._stop.is_set():
                time.sleep(2)
                if self.role == "follower" and time.time() - self.last_leader_heartbeat > 3:
                    print("Líder inativo para este seguidor.")
                    if self.priority_index == 0:
                        self.become_leader()

        threading.Thread(target=_loop, daemon=True).start()

    def stop(self):
        self._stop.set()
        if self._hb_thread:
            self._hb_stop.set()
            self._hb_thread.join(timeout=1)
        self.server.stop(0).wait()


def run_server(db_path, host="localhost", port=8000,
               leader_host=None, leader_port=None, node_id="follower",
               priority_index=0, role="follower"):
    """Compatibility wrapper used in tests to start a node server."""
    node = NodeServer(db_path, host, port, leader_host, leader_port,
                      node_id, priority_index=priority_index, role=role)
    node.start()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run gRPC replica server")
    parser.add_argument("--path", required=True, help="Database path")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--leader_host", default=None)
    parser.add_argument("--leader_port", type=int, default=None)
    parser.add_argument("--node_id", default="follower")
    parser.add_argument("--priority_index", type=int, default=0)
    parser.add_argument("--role", default="follower")
    args = parser.parse_args()

    run_server(args.path, args.host, args.port,
               args.leader_host, args.leader_port, args.node_id,
               args.priority_index, args.role)


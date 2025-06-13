"""gRPC node server with simple multi-leader replication."""

import threading
import time
import json
import os
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from lamport import LamportClock
from vector_clock import VectorClock
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
            if request.vector.items:
                new_vc = VectorClock(dict(request.vector.items))
            else:
                new_vc = VectorClock({"ts": int(request.timestamp)})

            mode = self._node.consistency_mode

            if mode == "crdt" and request.key in self._node.crdts:
                crdt = self._node.crdts[request.key]
                try:
                    other_data = json.loads(request.value) if request.value else {}
                except Exception:
                    other_data = {}
                other = type(crdt).from_dict(request.node_id, other_data)
                crdt.merge(other)
                self._node.db.put(
                    request.key,
                    json.dumps(crdt.to_dict()),
                    vector_clock=new_vc,
                )
            elif mode in ("vector", "crdt"):
                versions = self._node.db.get_record(request.key)
                dominated = False
                for _, vc in versions:
                    cmp = new_vc.compare(vc)
                    if cmp == "<":
                        dominated = True
                        break
                if not dominated:
                    self._node.db.put(
                        request.key,
                        request.value,
                        vector_clock=new_vc,
                    )
                else:
                    apply_update = False
            else:  # lww
                versions = self._node.db.get_record(request.key)
                latest_ts = -1
                for _, vc in versions:
                    ts_val = vc.clock.get("ts", 0)
                    if ts_val > latest_ts:
                        latest_ts = ts_val
                if int(request.timestamp) >= latest_ts:
                    self._node.db.put(
                        request.key,
                        request.value,
                        timestamp=int(request.timestamp),
                    )
                else:
                    apply_update = False

        if apply_update:
            op_id = request.op_id
            if not op_id:
                op_id = self._node.next_op_id()
                self._node.replication_log[op_id] = (
                    request.key,
                    request.value,
                    request.timestamp,
                )
                self._node.save_replication_log()
            self._node.replicate(
                "PUT",
                request.key,
                request.value,
                request.timestamp,
                op_id=op_id,
                vector=new_vc.clock,
                skip_id=request.node_id,
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
            if request.vector.items:
                new_vc = VectorClock(dict(request.vector.items))
            else:
                new_vc = VectorClock({"ts": int(request.timestamp)})

            mode = self._node.consistency_mode

            if mode in ("vector", "crdt"):
                versions = self._node.db.get_record(request.key)
                dominated = False
                for _, vc in versions:
                    cmp = new_vc.compare(vc)
                    if cmp == "<":
                        dominated = True
                        break
                if not dominated:
                    self._node.db.delete(request.key, vector_clock=new_vc)
                else:
                    apply_update = False
            else:  # lww
                versions = self._node.db.get_record(request.key)
                latest_ts = -1
                for _, vc in versions:
                    ts_val = vc.clock.get("ts", 0)
                    if ts_val > latest_ts:
                        latest_ts = ts_val
                if int(request.timestamp) >= latest_ts:
                    self._node.db.delete(request.key, timestamp=int(request.timestamp))
                else:
                    apply_update = False

        if apply_update:
            op_id = request.op_id
            if not op_id:
                op_id = self._node.next_op_id()
                self._node.replication_log[op_id] = (request.key, None, request.timestamp)
                self._node.save_replication_log()
            self._node.replicate(
                "DELETE",
                request.key,
                None,
                request.timestamp,
                op_id=op_id,
                vector=new_vc.clock,
                skip_id=request.node_id,
            )

        return replication_pb2.Empty()

    def Get(self, request, context):
        value = self._node.db.get(request.key)
        if value is None:
            value = ""
        return replication_pb2.ValueResponse(value=value)

    def FetchUpdates(self, request, context):
        """Handle anti-entropy synchronization with a peer."""
        last_seen = dict(request.vector.items)
        remote_hashes = dict(request.segment_hashes)

        for op in request.ops:
            if op.delete:
                req = replication_pb2.KeyRequest(
                    key=op.key,
                    timestamp=op.timestamp,
                    node_id=op.node_id,
                    op_id=op.op_id,
                )
                self.Delete(req, context)
            else:
                req = replication_pb2.KeyValue(
                    key=op.key,
                    value=op.value,
                    timestamp=op.timestamp,
                    node_id=op.node_id,
                    op_id=op.op_id,
                )
                self.Put(req, context)

        ops = []
        for op_id, (key, value, ts) in self._node.replication_log.items():
            origin, seq = op_id.split(":")
            seq = int(seq)
            seen = last_seen.get(origin, 0)
            if seq > seen:
                ops.append(
                    replication_pb2.Operation(
                        key=key,
                        value=value if value is not None else "",
                        timestamp=ts,
                        node_id=origin,
                        op_id=op_id,
                        delete=value is None,
                    )
                )

        local_hashes = self._node.db.segment_hashes
        if remote_hashes:
            for seg, h in local_hashes.items():
                if remote_hashes.get(seg) == h:
                    continue
                for k, v, vc in self._node.db.get_segment_items(seg):
                    ts_val = vc.clock.get("ts", 0) if vc is not None else 0
                    ops.append(
                        replication_pb2.Operation(
                            key=k,
                            value=v if v is not None else "",
                            timestamp=ts_val,
                            node_id=self._node.node_id,
                            op_id="",
                            delete=v is None,
                        )
                    )

        return replication_pb2.FetchResponse(ops=ops, segment_hashes=local_hashes)


class NodeServer:
    """Encapsulates gRPC server and replication logic for a node."""

    local_seq: int
    last_seen: dict[str, int]
    replication_log: dict[str, tuple]

    def __init__(
        self,
        db_path,
        host="localhost",
        port=8000,
        node_id="node",
        peers=None,
        ring=None,
        replication_factor: int = 3,
        consistency_mode: str = "lww",
        anti_entropy_interval: float = 5.0,
        max_batch_size: int = 50,
        crdt_config: dict | None = None,
    ):
        self.db_path = db_path
        self.db = SimpleLSMDB(db_path=db_path)
        self.host = host
        self.port = port
        self.node_id = node_id
        self.peers = peers or []
        self.ring = ring
        self.replication_factor = replication_factor
        self.consistency_mode = consistency_mode
        self.crdt_config = crdt_config or {}

        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.service = ReplicaService(self)
        replication_pb2_grpc.add_ReplicaServicer_to_server(
            self.service, self.server
        )

        self.health_servicer = health.HealthServicer()
        health_pb2_grpc.add_HealthServicer_to_server(self.health_servicer, self.server)
        self.health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

        self.server.add_insecure_port(f"{host}:{port}")

        self.clock = LamportClock()
        self.vector_clock = VectorClock()
        self.local_seq = 0
        self.last_seen: dict[str, int] = {}
        self.replication_log: dict[str, tuple] = {}
        self._cleanup_stop = threading.Event()
        self._cleanup_thread = None
        self._replay_stop = threading.Event()
        self._replay_thread = None
        self._anti_entropy_stop = threading.Event()
        self._anti_entropy_thread = None
        self.anti_entropy_interval = anti_entropy_interval
        self.max_batch_size = max_batch_size
        self.peer_clients = []
        self.client_map = {}
        self.clients_by_id = {}
        for peer in self.peers:
            if len(peer) == 2:
                ph, pp = peer
                pid = f"{ph}:{pp}"
            else:
                ph, pp, pid = peer
            if ph == self.host and pp == self.port:
                continue
            c = GRPCReplicaClient(ph, pp)
            self.peer_clients.append(c)
            self.client_map[f"{ph}:{pp}"] = c
            self.clients_by_id[pid] = c

        self.hints: dict[str, list[tuple]] = {}
        self._replog_fp = None
        self._replog_lock = threading.Lock()

        # Initialize CRDT instances for configured keys
        self.crdts = {}
        from crdt import GCounter, ORSet
        for key, typ in self.crdt_config.items():
            if typ == "gcounter" or typ == GCounter:
                cls = GCounter if typ == "gcounter" else typ
            elif typ == "orset" or typ == ORSet:
                cls = ORSet if typ == "orset" else typ
            else:
                cls = typ
            try:
                self.crdts[key] = cls(self.node_id)
            except Exception:
                self.crdts[key] = cls

    def _iter_peers(self):
        """Yield tuples of (host, port, node_id, client) for all peers."""
        if self.clients_by_id:
            return [
                (c.host, c.port, node_id, c)
                for node_id, c in self.clients_by_id.items()
            ]
        return [
            (c.host, c.port, f"{c.host}:{c.port}", c)
            for c in self.peer_clients
        ]

    # persistence helpers ------------------------------------------------
    def _last_seen_file(self) -> str:
        return os.path.join(self.db_path, "last_seen.json")

    def _hints_file(self) -> str:
        return os.path.join(self.db_path, "hints.json")

    def _replication_log_file(self) -> str:
        return os.path.join(self.db_path, "replication_log.json")

    def load_last_seen(self) -> None:
        """Load last_seen from JSON file if available."""
        path = self._last_seen_file()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                try:
                    self.last_seen = json.load(f)
                    self.vector_clock.merge(VectorClock(self.last_seen))
                except Exception:
                    self.last_seen = {}

    def load_hints(self) -> None:
        """Load hints from disk if present."""
        path = self._hints_file()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    self.hints = {k: [tuple(op) for op in v] for k, v in data.items()}
                except Exception:
                    self.hints = {}

    def load_replication_log(self) -> None:
        """Load replication log from disk if available and open file handle."""
        path = self._replication_log_file()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    self.replication_log = {k: tuple(v) for k, v in data.items()}
                except Exception:
                    self.replication_log = {}
        os.makedirs(self.db_path, exist_ok=True)
        if not os.path.exists(path):
            open(path, "w", encoding="utf-8").close()
        self._replog_fp = open(path, "r+", encoding="utf-8")
        self._persist_replication_log()

    def save_last_seen(self) -> None:
        """Persist last_seen to JSON file."""
        path = self._last_seen_file()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.last_seen, f)

    def save_hints(self) -> None:
        """Persist hints to disk."""
        path = self._hints_file()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.hints, f)

    def _persist_replication_log(self) -> None:
        if not self._replog_fp:
            return
        with self._replog_lock:
            self._replog_fp.seek(0)
            json.dump(self.replication_log, self._replog_fp)
            self._replog_fp.truncate()
            self._replog_fp.flush()
            os.fsync(self._replog_fp.fileno())

    def save_replication_log(self) -> None:
        self._persist_replication_log()

    def next_op_id(self) -> str:
        """Return next operation identifier."""
        seq = self.vector_clock.increment(self.node_id)
        self.local_seq = seq
        self.last_seen[self.node_id] = seq
        return f"{self.node_id}:{seq}"

    def apply_crdt(self, key: str, op) -> None:
        """Apply a local CRDT operation and replicate the new state."""
        if key not in self.crdts:
            raise KeyError(key)
        crdt = self.crdts[key]
        crdt.apply(op)
        state_json = json.dumps(crdt.to_dict())
        ts = int(time.time() * 1000)
        vc = VectorClock({"ts": ts})
        self.db.put(key, state_json, vector_clock=vc)
        op_id = self.next_op_id()
        self.replication_log[op_id] = (key, state_json, ts)
        self.save_replication_log()
        self.replicate(
            "PUT",
            key,
            state_json,
            ts,
            op_id=op_id,
            vector=vc.clock,
        )

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
        if to_remove:
            self.save_replication_log()

    def _replay_replication_log(self) -> None:
        """Resend operations from replication_log to peers in batches."""
        self.sync_from_peer()

    def _cleanup_loop(self) -> None:
        while not self._cleanup_stop.is_set():
            self.cleanup_replication_log()
            time.sleep(1)

    def _replay_loop(self) -> None:
        while not self._replay_stop.is_set():
            self._replay_replication_log()
            time.sleep(0.5)

    def _anti_entropy_loop(self) -> None:
        while not self._anti_entropy_stop.is_set():
            self.sync_from_peer()
            time.sleep(self.anti_entropy_interval)

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

    def _start_anti_entropy_thread(self) -> None:
        if self._anti_entropy_thread and self._anti_entropy_thread.is_alive():
            return
        t = threading.Thread(target=self._anti_entropy_loop, daemon=True)
        self._anti_entropy_thread = t
        t.start()

    # replication helpers -------------------------------------------------
    def replicate(self, op, key, value, timestamp, op_id="", vector=None, skip_id=None):
        def _send(client, peer_id):
            try:
                if op == "PUT":
                    client.put(
                        key,
                        value,
                        timestamp=timestamp,
                        node_id=self.node_id,
                        op_id=op_id,
                        vector=vector,
                    )
                else:
                    client.delete(
                        key,
                        timestamp=timestamp,
                        node_id=self.node_id,
                        op_id=op_id,
                        vector=vector,
                    )
            except Exception as exc:
                print(f"Falha ao replicar: {exc}")
                self.hints.setdefault(peer_id, []).append(
                    [op_id, op, key, value, timestamp]
                )
                self.save_hints()

        for host, port, peer_id, client in self._iter_peers():
            if skip_id is not None:
                if self.clients_by_id:
                    if peer_id == skip_id:
                        continue
                else:
                    if f"{host}:{port}" == skip_id:
                        continue
            threading.Thread(
                target=_send,
                args=(client, f"{host}:{port}"),
                daemon=True,
            ).start()

    def sync_from_peer(self) -> None:
        """Exchange updates with all peers."""
        peer_list = list(self._iter_peers())
        if not peer_list:
            return

        pending_ops = []
        for op_id, (key, value, ts) in list(self.replication_log.items())[: self.max_batch_size]:
            pending_ops.append(
                replication_pb2.Operation(
                    key=key,
                    value=value if value is not None else "",
                    timestamp=ts,
                    node_id=self.node_id,
                    op_id=op_id,
                    delete=value is None,
                )
            )

        hashes = self.db.segment_hashes

        for host, port, peer_id, client in peer_list:
            try:
                resp = client.fetch_updates(self.last_seen, pending_ops, hashes)
            except Exception:
                continue

            if resp.segment_hashes:
                self.db.segment_hashes = dict(resp.segment_hashes)

            for op in resp.ops:
                if op.delete:
                    req_del = replication_pb2.KeyRequest(
                        key=op.key,
                        timestamp=op.timestamp,
                        node_id=op.node_id,
                        op_id=op.op_id,
                    )
                    self.service.Delete(req_del, None)
                else:
                    req_put = replication_pb2.KeyValue(
                        key=op.key,
                        value=op.value,
                        timestamp=op.timestamp,
                        node_id=op.node_id,
                        op_id=op.op_id,
                    )
                    self.service.Put(req_put, None)

            # attempt to flush hinted handoff operations
            addr_id = f"{host}:{port}"
            hints = self.hints.get(addr_id, [])
            if hints:
                remaining = []
                for h_op_id, h_op, h_key, h_val, h_ts in hints:
                    try:
                        if h_op == "PUT":
                            client.put(
                                h_key,
                                h_val,
                                timestamp=h_ts,
                                node_id=self.node_id,
                                op_id=h_op_id,
                            )
                        else:
                            client.delete(
                                h_key,
                                timestamp=h_ts,
                                node_id=self.node_id,
                                op_id=h_op_id,
                            )
                    except Exception:
                        remaining.append((h_op_id, h_op, h_key, h_val, h_ts))
                if remaining:
                    self.hints[addr_id] = [list(r) for r in remaining]
                else:
                    self.hints.pop(addr_id, None)
                self.save_hints()

    # lifecycle -----------------------------------------------------------
    def start(self):
        self.load_replication_log()
        self.load_last_seen()
        self.load_hints()
        self.sync_from_peer()
        self._start_cleanup_thread()
        self._start_replay_thread()
        self._start_anti_entropy_thread()
        self.server.start()
        self.server.wait_for_termination()

    def stop(self):
        self.save_last_seen()
        self.save_hints()
        self.save_replication_log()
        if self._replog_fp:
            self._replog_fp.close()
            self._replog_fp = None
        self._cleanup_stop.set()
        self._replay_stop.set()
        self._anti_entropy_stop.set()
        if self._cleanup_thread:
            self._cleanup_thread.join()
        if self._replay_thread:
            self._replay_thread.join()
        if self._anti_entropy_thread:
            self._anti_entropy_thread.join()
        for _, _, _, c in self._iter_peers():
            c.close()
        self.server.stop(0).wait()


def run_server(
    db_path,
    host="localhost",
    port=8000,
    node_id="node",
    peers=None,
    ring=None,
    replication_factor: int = 3,
    *,
    consistency_mode: str = "lww",
):
    node = NodeServer(
        db_path,
        host,
        port,
        node_id=node_id,
        peers=peers,
        ring=ring,
        replication_factor=replication_factor,
        consistency_mode=consistency_mode,
    )
    node.start()

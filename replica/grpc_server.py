"""gRPC node server with simple multi-leader replication."""

import threading
import time
import json
import os
from bisect import bisect_right
from concurrent import futures
from collections import OrderedDict

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from lamport import LamportClock
from vector_clock import VectorClock
from lsm_db import SimpleLSMDB
from merkle import MerkleNode, build_merkle_tree, diff_trees
from partitioning import hash_key
from index_manager import IndexManager
from global_index_manager import GlobalIndexManager
from . import replication_pb2, replication_pb2_grpc
from .client import GRPCReplicaClient


class ReplicaService(replication_pb2_grpc.ReplicaServicer):
    """Service exposing database operations."""

    def __init__(self, node):
        self._node = node

    # ------------------------------------------------------------------
    def _owner_for_key(self, key: str) -> str:
        """Return node_id of partition owner for given key."""
        pmap = getattr(self._node, "partition_map", None) or {}
        key_for_hash = key
        if key.startswith("idx:"):
            parts = key.split(":", 3)
            if len(parts) >= 3:
                key_for_hash = ":".join(parts[:3])
        if self._node.hash_ring is not None and self._node.hash_ring._ring:
            key_hash = hash_key(key_for_hash)
            hashes = [h for h, _ in self._node.hash_ring._ring]
            idx = bisect_right(hashes, key_hash) % len(hashes)
            return pmap.get(idx, self._node.hash_ring._ring[idx][1])
        if getattr(self._node, "range_table", None):
            for i, ((start, end), _) in enumerate(self._node.range_table):
                if start <= key < end:
                    return pmap.get(i, self._node.range_table[i][1])
        if self._node.partition_modulus is not None and self._node.node_index is not None:
            pid = hash_key(key_for_hash) % self._node.partition_modulus
            return pmap.get(pid, f"node_{pid % len(self._node.peers) if self._node.peers else pid}")
        return self._node.node_id

    def Put(self, request, context):
        owner_id = self._owner_for_key(request.key)
        if getattr(self._node, "enable_forwarding", False):
            if owner_id != self._node.node_id and request.node_id != owner_id:
                client = self._node.clients_by_id.get(owner_id)
                if client:
                    return client.stub.Put(request)
        else:
            if owner_id != self._node.node_id and request.node_id == "":
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "NotOwner")
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

            existing = self._node.db.get(request.key)
            mode = self._node.consistency_mode

            if mode == "crdt" and request.key in self._node.crdts:
                crdt = self._node.crdts[request.key]
                try:
                    other_data = json.loads(request.value) if request.value else {}
                except Exception:
                    other_data = {}
                other = type(crdt).from_dict(request.node_id, other_data)
                crdt.merge(other)
                if isinstance(existing, list):
                    for val in existing:
                        self._node.index_manager.remove_record(request.key, val)
                elif existing is not None:
                    self._node.index_manager.remove_record(request.key, existing)
                self._node.db.put(
                    request.key,
                    json.dumps(crdt.to_dict()),
                    vector_clock=new_vc,
                )
                self._node._cache_delete(request.key)
                self._node.index_manager.add_record(request.key, json.dumps(crdt.to_dict()))
            elif mode in ("vector", "crdt"):
                versions = self._node.db.get_record(request.key)
                dominated = False
                for _, vc in versions:
                    cmp = new_vc.compare(vc)
                    if cmp == "<":
                        dominated = True
                        break
                if not dominated:
                    if isinstance(existing, list):
                        for val in existing:
                            self._node.index_manager.remove_record(request.key, val)
                    elif existing is not None:
                        self._node.index_manager.remove_record(request.key, existing)
                    self._node.db.put(
                        request.key,
                        request.value,
                        vector_clock=new_vc,
                    )
                    self._node._cache_delete(request.key)
                    self._node.index_manager.add_record(request.key, request.value)
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
                    if isinstance(existing, list):
                        for val in existing:
                            self._node.index_manager.remove_record(request.key, val)
                    elif existing is not None:
                        self._node.index_manager.remove_record(request.key, existing)
                    self._node.db.put(
                        request.key,
                        request.value,
                        timestamp=int(request.timestamp),
                    )
                    self._node._cache_delete(request.key)
                    self._node.index_manager.add_record(request.key, request.value)
                else:
                    apply_update = False

        if apply_update and request.hinted_for and request.hinted_for != self._node.node_id:
            self._node.hints.setdefault(request.hinted_for, []).append(
                [request.op_id or "", "PUT", request.key, request.value, int(request.timestamp)]
            )
            self._node.save_hints()
            return replication_pb2.Empty()

        if apply_update:
            is_coordinator = True
            if self._node.hash_ring is not None:
                preferred = self._node.hash_ring.get_preference_list(
                    request.key, 1
                )
                if preferred and preferred[0] != self._node.node_id:
                    is_coordinator = False

            if is_coordinator:
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

                # ------------------------------------------------------------------
                # Update global secondary indexes only once by the coordinator
                if not request.key.startswith("idx:") and self._node.global_index_fields:
                    try:
                        data = json.loads(request.value)
                    except Exception:
                        data = None
                    if isinstance(data, dict):
                        for field in self._node.global_index_fields:
                            if field not in data:
                                continue
                            val = data[field]
                            idx_key = f"idx:{field}:{val}:{request.key}"
                            owner = self._owner_for_key(f"idx:{field}:{val}")
                            if owner == self._node.node_id:
                                self._node.db.put(
                                    idx_key,
                                    "1",
                                    timestamp=int(request.timestamp),
                                )
                                self._node.global_index_manager.add_entry(field, val, request.key)
                            else:
                                client = self._node.clients_by_id.get(owner)
                                if client:
                                    client.put(
                                        idx_key,
                                        "1",
                                        timestamp=int(request.timestamp),
                                        node_id=self._node.node_id,
                                    )

        return replication_pb2.Empty()

    def Delete(self, request, context):
        owner_id = self._owner_for_key(request.key)
        if getattr(self._node, "enable_forwarding", False):
            if owner_id != self._node.node_id and request.node_id != owner_id:
                client = self._node.clients_by_id.get(owner_id)
                if client:
                    return client.stub.Delete(request)
        else:
            if owner_id != self._node.node_id and request.node_id == "":
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "NotOwner")
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

            existing = self._node.db.get(request.key)

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
                    self._node._cache_delete(request.key)
                    if isinstance(existing, list):
                        for val in existing:
                            self._node.index_manager.remove_record(request.key, val)
                    elif existing is not None:
                        self._node.index_manager.remove_record(request.key, existing)
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
                    self._node._cache_delete(request.key)
                    if isinstance(existing, list):
                        for val in existing:
                            self._node.index_manager.remove_record(request.key, val)
                    elif existing is not None:
                        self._node.index_manager.remove_record(request.key, existing)
                else:
                    apply_update = False

        if apply_update and request.hinted_for and request.hinted_for != self._node.node_id:
            self._node.hints.setdefault(request.hinted_for, []).append(
                [request.op_id or "", "DELETE", request.key, None, int(request.timestamp)]
            )
            self._node.save_hints()
            return replication_pb2.Empty()

        if apply_update:
            is_coordinator = True
            if self._node.hash_ring is not None:
                preferred = self._node.hash_ring.get_preference_list(
                    request.key, 1
                )
                if preferred and preferred[0] != self._node.node_id:
                    is_coordinator = False

            if is_coordinator:
                op_id = request.op_id
                if not op_id:
                    op_id = self._node.next_op_id()
                    self._node.replication_log[op_id] = (
                        request.key,
                        None,
                        request.timestamp,
                    )
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

                # ------------------------------------------------------------------
                # Remove any global index entries related to this key
                if not request.key.startswith("idx:") and self._node.global_index_fields:
                    old_records = []
                    if isinstance(existing, list):
                        old_records.extend(existing)
                    elif existing is not None:
                        old_records.append(existing)
                    for old_val in old_records:
                        try:
                            data = json.loads(old_val)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue
                        for field in self._node.global_index_fields:
                            if field not in data:
                                continue
                            val = data[field]
                            idx_key = f"idx:{field}:{val}:{request.key}"
                            owner = self._owner_for_key(f"idx:{field}:{val}")
                            if owner == self._node.node_id:
                                self._node.db.delete(
                                    idx_key,
                                    timestamp=int(request.timestamp),
                                )
                                self._node.global_index_manager.remove_entry(field, val, request.key)
                            else:
                                client = self._node.clients_by_id.get(owner)
                                if client:
                                    client.delete(
                                        idx_key,
                                        timestamp=int(request.timestamp),
                                        node_id=self._node.node_id,
                                    )

        return replication_pb2.Empty()

    def Get(self, request, context):
        owner_id = self._owner_for_key(request.key)
        if getattr(self._node, "enable_forwarding", False) and owner_id != self._node.node_id:
            client = self._node.clients_by_id.get(owner_id)
            if client:
                return client.stub.Get(request)

        records = self._node._cache_get(request.key)
        if records is None:
            records = self._node.db.get_record(request.key)
            if records:
                self._node._cache_set(request.key, records)
        if not records:
            return replication_pb2.ValueResponse(values=[])

        values = []
        for val, vc in records:
            ts = vc.clock.get("ts", 0) if vc is not None else 0
            values.append(
                replication_pb2.VersionedValue(
                    value=val,
                    timestamp=ts,
                    vector=replication_pb2.VersionVector(items=vc.clock if vc else {}),
                )
            )

        return replication_pb2.ValueResponse(values=values)

    def ScanRange(self, request, context):
        owner_id = self._owner_for_key(request.partition_key)
        if getattr(self._node, "enable_forwarding", False) and owner_id != self._node.node_id:
            client = self._node.clients_by_id.get(owner_id)
            if client:
                return client.stub.ScanRange(request)
        elif owner_id != self._node.node_id:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "NotOwner")

        items = self._node.db.scan_range(
            request.partition_key, request.start_ck, request.end_ck
        )
        resp_items = []
        for ck, val, vc in items:
            ts = vc.clock.get("ts", 0) if vc is not None else 0
            resp_items.append(
                replication_pb2.RangeItem(
                    clustering_key=ck,
                    value=val,
                    timestamp=ts,
                    vector=replication_pb2.VersionVector(items=vc.clock if vc else {}),
                )
            )
        return replication_pb2.RangeResponse(items=resp_items)

    def FetchUpdates(self, request, context):
        """Handle anti-entropy synchronization with a peer."""
        last_seen = dict(request.vector.items)
        remote_hashes = dict(request.segment_hashes)
        remote_trees = {}
        for t in request.trees:
            remote_trees[t.segment] = MerkleNode.from_proto(t.root)

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
                items = [
                    (k, v)
                    for k, v, _vc in self._node.db.get_segment_items(seg)
                    if v != "__TOMBSTONE__"
                ]
                local_tree = build_merkle_tree(items)
                diff_keys = diff_trees(local_tree, remote_trees.get(seg))
                for key in diff_keys:
                    for val, vc in self._node.db.get_record(key):
                        ts_val = vc.clock.get("ts", 0) if vc is not None else 0
                        ops.append(
                            replication_pb2.Operation(
                                key=key,
                                value=val if val is not None else "",
                                timestamp=ts_val,
                                node_id=self._node.node_id,
                                op_id="",
                                delete=val is None,
                            )
                        )
                        if len(ops) >= self._node.max_batch_size:
                            break
                    if len(ops) >= self._node.max_batch_size:
                        break
                if len(ops) >= self._node.max_batch_size:
                    break

        return replication_pb2.FetchResponse(ops=ops, segment_hashes=local_hashes)

    def UpdatePartitionMap(self, request, context):
        """Replace the node's partition map."""
        new_map = dict(request.items)
        self._node.update_partition_map(new_map)
        return replication_pb2.Empty()

    def ListByIndex(self, request, context):
        """Return keys matching an index query."""
        try:
            value = json.loads(request.value)
        except Exception:
            value = request.value
        keys = self._node.query_index(request.field, value)
        return replication_pb2.KeyList(keys=keys)


class HeartbeatService(replication_pb2_grpc.HeartbeatServiceServicer):
    """Simple heartbeat service used for peer liveness checks."""

    def __init__(self, node):
        self._node = node

    def Ping(self, request, context):
        """Respond to heartbeat ping with an empty message."""
        return replication_pb2.Empty()


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
        hash_ring=None,
        *,
        partition_map=None,
        partition_modulus: int | None = None,
        node_index: int | None = None,
        replication_factor: int = 3,
        write_quorum: int | None = None,
        read_quorum: int | None = None,
        consistency_mode: str = "lww",
        anti_entropy_interval: float = 5.0,
        max_batch_size: int = 50,
        crdt_config: dict | None = None,
        enable_forwarding: bool = False,
        cache_size: int = 0,
        index_fields: list[str] | None = None,
        global_index_fields: list[str] | None = None,
    ):
        self.db_path = db_path
        self.db = SimpleLSMDB(db_path=db_path)
        self.host = host
        self.port = port
        self.node_id = node_id
        self.peers = peers or []
        self.hash_ring = hash_ring
        self.partition_map = partition_map or {}
        self.partition_modulus = partition_modulus
        self.node_index = node_index
        self.replication_factor = replication_factor
        self.write_quorum = write_quorum or (replication_factor // 2 + 1)
        self.read_quorum = read_quorum or (replication_factor // 2 + 1)
        self.consistency_mode = consistency_mode
        self.crdt_config = crdt_config or {}
        self.enable_forwarding = bool(enable_forwarding)
        self.cache_size = int(cache_size)
        self.cache = OrderedDict() if self.cache_size > 0 else None
        self.index_fields = list(index_fields or [])
        self.index_manager = IndexManager(self.index_fields)
        self.index_manager.rebuild(self.db)
        self.global_index_fields = list(global_index_fields or [])
        self.global_index_manager = GlobalIndexManager(self.global_index_fields)

        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.service = ReplicaService(self)
        replication_pb2_grpc.add_ReplicaServicer_to_server(
            self.service, self.server
        )
        self.heartbeat_service = HeartbeatService(self)
        replication_pb2_grpc.add_HeartbeatServiceServicer_to_server(
            self.heartbeat_service, self.server
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
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread = None
        self._hinted_stop = threading.Event()
        self._hinted_thread = None
        self.anti_entropy_interval = anti_entropy_interval
        self.heartbeat_interval = 1.0
        self.heartbeat_timeout = 3.0
        self.hinted_handoff_interval = 1.0
        self.max_batch_size = max_batch_size
        self.peer_clients = []
        self.client_map = {}
        self.clients_by_id = {}
        self.peer_status: dict[str, float | None] = {}
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
            self.peer_status[pid] = None

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

    def update_partition_map(self, new_map) -> None:
        """Update the cached partition map."""
        self.partition_map = new_map or {}

    def query_index(self, field: str, value) -> list[str]:
        """Return list of keys matching ``field``/``value`` in the secondary index."""
        return self.index_manager.query(field, value)

    def get_index_owner(self, field: str, value) -> str:
        """Return node_id responsible for ``field``/``value`` index key."""
        key = f"idx:{field}:{value}"
        # Delegate to ReplicaService logic for owner determination
        return self.service._owner_for_key(key)

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

    # cache helpers ------------------------------------------------
    def _cache_get(self, key):
        if self.cache is None:
            return None
        if key in self.cache:
            val = self.cache.pop(key)
            self.cache[key] = val
            return val
        return None

    def _cache_set(self, key, records):
        if self.cache is None:
            return
        if key in self.cache:
            self.cache.pop(key)
        self.cache[key] = records
        if len(self.cache) > self.cache_size:
            self.cache.popitem(last=False)

    def _cache_delete(self, key):
        if self.cache is None:
            return
        self.cache.pop(key, None)

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

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.is_set():
            now = time.time()
            for host, port, peer_id, client in self._iter_peers():
                try:
                    client.ping(self.node_id)
                    self.peer_status[peer_id] = now
                except Exception:
                    pass
            for pid, ts in list(self.peer_status.items()):
                if ts is not None and now - ts > self.heartbeat_timeout:
                    self.peer_status[pid] = None
            time.sleep(self.heartbeat_interval)

    def _hinted_handoff_loop(self) -> None:
        while not self._hinted_stop.is_set():
            updated = False
            for peer_id, hints in list(self.hints.items()):
                if self.peer_status.get(peer_id) is None:
                    continue
                client = self.clients_by_id.get(peer_id) or self.client_map.get(peer_id)
                if not client:
                    continue
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
                                hinted_for="",
                            )
                        else:
                            client.delete(
                                h_key,
                                timestamp=h_ts,
                                node_id=self.node_id,
                                op_id=h_op_id,
                                hinted_for="",
                            )
                    except Exception:
                        remaining.append((h_op_id, h_op, h_key, h_val, h_ts))
                if remaining:
                    self.hints[peer_id] = [list(r) for r in remaining]
                else:
                    self.hints.pop(peer_id, None)
                updated = True
            if updated:
                self.save_hints()
            time.sleep(self.hinted_handoff_interval)

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

    def _start_heartbeat_thread(self) -> None:
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        t = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread = t
        t.start()

    def _start_hinted_handoff_thread(self) -> None:
        if self._hinted_thread and self._hinted_thread.is_alive():
            return
        t = threading.Thread(target=self._hinted_handoff_loop, daemon=True)
        self._hinted_thread = t
        t.start()

    # replication helpers -------------------------------------------------
    def replicate(self, op, key, value, timestamp, op_id="", vector=None, skip_id=None):
        """Synchronously replicate an operation to responsible peers."""

        if self.replication_factor <= 1:
            # single replica configured - no replication needed
            return

        # Determine peers responsible for this key according to the hash ring.
        if self.hash_ring and self.clients_by_id:
            pref_nodes = self.hash_ring.get_preference_list(key, len(self.clients_by_id) + 1)
            peer_list = []
            missing = []
            for node_id in pref_nodes:
                if node_id == self.node_id:
                    continue
                client = self.clients_by_id.get(node_id)
                if not client:
                    continue
                if self.peer_status.get(node_id) is None:
                    if len(peer_list) >= self.replication_factor - 1:
                        self.hints.setdefault(node_id, []).append([op_id, op, key, value, timestamp])
                        continue
                    missing.append(node_id)
                    peer_list.append((client.host, client.port, node_id, "", client))
                    if len(peer_list) >= self.replication_factor - 1:
                        break
                    continue
                peer_list.append((client.host, client.port, node_id, "", client))
                if len(peer_list) >= self.replication_factor - 1:
                    break

            for node_id in pref_nodes:
                if len(peer_list) >= self.replication_factor - 1 or not missing:
                    break
                if node_id == self.node_id or any(p[2] == node_id for p in peer_list) or node_id in missing:
                    continue
                client = self.clients_by_id.get(node_id)
                if not client or self.peer_status.get(node_id) is None:
                    continue
                hinted = missing.pop(0)
                peer_list.append((client.host, client.port, node_id, hinted, client))

            for node_id in missing:
                self.hints.setdefault(node_id, []).append([op_id, op, key, value, timestamp])
        else:
            peer_list = []
            missing = []
            peers = list(self._iter_peers())
            for host, port, node_id, client in peers:
                if self.peer_status.get(node_id) is None:
                    if len(peer_list) >= self.replication_factor - 1:
                        self.hints.setdefault(node_id, []).append([op_id, op, key, value, timestamp])
                        continue
                    missing.append(node_id)
                    peer_list.append((host, port, node_id, "", client))
                    if len(peer_list) >= self.replication_factor - 1:
                        break
                    continue
                peer_list.append((host, port, node_id, "", client))
                if len(peer_list) >= self.replication_factor - 1:
                    break
            for host, port, node_id, client in peers:
                if len(peer_list) >= self.replication_factor - 1 or not missing:
                    break
                if any(p[2] == node_id for p in peer_list) or node_id in missing or self.peer_status.get(node_id) is None:
                    continue
                hinted = missing.pop(0)
                peer_list.append((host, port, node_id, hinted, client))

            for node_id in missing:
                self.hints.setdefault(node_id, []).append([op_id, op, key, value, timestamp])

        errors = []
        def do_rpc(params):
            host, port, peer_id, hinted_for, client = params
            if skip_id is not None:
                if self.clients_by_id:
                    if peer_id == skip_id:
                        return False
                else:
                    if f"{host}:{port}" == skip_id:
                        return False
            if op == "PUT":
                client.put(
                    key,
                    value,
                    timestamp=timestamp,
                    node_id=self.node_id,
                    op_id=op_id,
                    vector=vector,
                    hinted_for=hinted_for,
                )
            else:
                client.delete(
                    key,
                    timestamp=timestamp,
                    node_id=self.node_id,
                    op_id=op_id,
                    vector=vector,
                    hinted_for=hinted_for,
                )
            return True

        if not peer_list:
            return

        ack = 1  # local write
        futures_map = {}
        with futures.ThreadPoolExecutor(max_workers=len(peer_list)) as ex:
            for p in peer_list:
                fut = ex.submit(do_rpc, p)
                futures_map[fut] = p
            for fut in futures.as_completed(futures_map):
                host, port, peer_id, hinted_for, _ = futures_map[fut]
                try:
                    res = fut.result()
                    if res:
                        ack += 1
                except Exception as exc:
                    print(f"Falha ao replicar: {exc}")
                    self.hints.setdefault(peer_id, []).append(
                        [op_id, op, key, value, timestamp]
                    )
                    errors.append(exc)
                if ack >= self.write_quorum:
                    break

        self.save_hints()
        if ack < self.write_quorum:
            raise RuntimeError("replication failed")

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
        trees = []
        for seg in hashes:
            items = [
                (k, v)
                for k, v, _vc in self.db.get_segment_items(seg)
                if v != "__TOMBSTONE__"
            ]
            root = build_merkle_tree(items)
            trees.append(
                replication_pb2.SegmentTree(segment=seg, root=root.to_proto())
            )

        for host, port, peer_id, client in peer_list:
            try:
                try:
                    resp = client.fetch_updates(self.last_seen, pending_ops, hashes, trees)
                except TypeError:
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
                    try:
                        self.service.Delete(req_del, None)
                    except Exception:
                        pass
                else:
                    req_put = replication_pb2.KeyValue(
                        key=op.key,
                        value=op.value,
                        timestamp=op.timestamp,
                        node_id=op.node_id,
                        op_id=op.op_id,
                    )
                    try:
                        self.service.Put(req_put, None)
                    except Exception:
                        pass

            # attempt to flush hinted handoff operations
            hints = self.hints.get(peer_id, [])
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
                                hinted_for="",
                            )
                        else:
                            client.delete(
                                h_key,
                                timestamp=h_ts,
                                node_id=self.node_id,
                                op_id=h_op_id,
                                hinted_for="",
                            )
                    except Exception:
                        remaining.append((h_op_id, h_op, h_key, h_val, h_ts))
                if remaining:
                    self.hints[peer_id] = [list(r) for r in remaining]
                else:
                    self.hints.pop(peer_id, None)
                self.save_hints()

    # lifecycle -----------------------------------------------------------
    def start(self):
        self.load_replication_log()
        self.load_last_seen()
        self.load_hints()
        self.global_index_manager.rebuild(self.db)
        self.server.start()
        if self.replication_factor > 1:
            self._start_cleanup_thread()
            self._start_replay_thread()
            self._start_anti_entropy_thread()
            self._start_hinted_handoff_thread()
            self.sync_from_peer()
        self._start_heartbeat_thread()
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
        self._heartbeat_stop.set()
        self._hinted_stop.set()
        if self._cleanup_thread:
            self._cleanup_thread.join()
        if self._replay_thread:
            self._replay_thread.join()
        if self._anti_entropy_thread:
            self._anti_entropy_thread.join()
        if self._heartbeat_thread:
            self._heartbeat_thread.join()
        if self._hinted_thread:
            self._hinted_thread.join()
        for _, _, _, c in self._iter_peers():
            c.close()
        self.server.stop(0).wait()


def run_server(
    db_path,
    host="localhost",
    port=8000,
    node_id="node",
    peers=None,
    hash_ring=None,
    partition_map=None,
    replication_factor: int = 3,
    write_quorum: int | None = None,
    read_quorum: int | None = None,
    *,
    consistency_mode: str = "lww",
    enable_forwarding: bool = False,
    partition_modulus: int | None = None,
    node_index: int | None = None,
    index_fields: list[str] | None = None,
    global_index_fields: list[str] | None = None,
):
    node = NodeServer(
        db_path,
        host,
        port,
        node_id=node_id,
        peers=peers,
        hash_ring=hash_ring,
        partition_map=partition_map,
        replication_factor=replication_factor,
        write_quorum=write_quorum,
        read_quorum=read_quorum,
        consistency_mode=consistency_mode,
        enable_forwarding=enable_forwarding,
        partition_modulus=partition_modulus,
        node_index=node_index,
        index_fields=index_fields,
        global_index_fields=global_index_fields,
    )
    node.start()

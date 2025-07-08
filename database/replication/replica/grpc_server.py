"""gRPC node server with simple multi-leader replication."""

import threading
import time
import json
import os
from bisect import bisect_right
from concurrent import futures
from collections import OrderedDict
import uuid

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from ...utils.lamport import LamportClock
from ...utils.vector_clock import VectorClock
from ...lsm.lsm_db import SimpleLSMDB
from ...utils.merkle import MerkleNode, build_merkle_tree, diff_trees
from ...clustering.partitioning import hash_key
from ...clustering.index_manager import IndexManager
from ...clustering.global_index_manager import GlobalIndexManager
from ...clustering.hash_ring import HashRing
from ...utils.event_logger import EventLogger
import logging

logger = logging.getLogger(__name__)
from . import replication_pb2, replication_pb2_grpc, metadata_pb2, metadata_pb2_grpc
from .client import GRPCReplicaClient

# Global lock used to serialize Transfer operations
global_transfer_lock = threading.Lock()


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

        if request.tx_id:
            if self._node.tx_lock_strategy == "2pl":
                self._node._acquire_exclusive_lock(request.key, request.tx_id, context)
            with self._node._tx_lock:
                txdata = self._node.active_transactions.setdefault(
                    request.tx_id,
                    {
                        "ops": [],
                        "read_versions": {},
                        "reads": set(),
                        "writes": set(),
                    },
                )
                txdata["ops"].append(("put", request))
                txdata["writes"].add(request.key)
            return replication_pb2.Empty()

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
                for _, vc, *_ in versions:
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
                for _, vc, *_ in versions:
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
                    with self._node._replog_lock:
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
                    skip_id=(
                        request.node_id
                        if request.node_id and request.node_id != self._node.node_id
                        else None
                    ),
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

        if request.tx_id:
            if self._node.tx_lock_strategy == "2pl":
                self._node._acquire_exclusive_lock(request.key, request.tx_id, context)
            with self._node._tx_lock:
                txdata = self._node.active_transactions.setdefault(
                    request.tx_id,
                    {
                        "ops": [],
                        "read_versions": {},
                        "reads": set(),
                        "writes": set(),
                    },
                )
                txdata["ops"].append(("delete", request))
                txdata["writes"].add(request.key)
            return replication_pb2.Empty()

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
                for _, vc, *_ in versions:
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
                for _, vc, *_ in versions:
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
                    with self._node._replog_lock:
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
                    skip_id=(
                        request.node_id
                        if request.node_id and request.node_id != self._node.node_id
                        else None
                    ),
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

        if request.tx_id and self._node.tx_lock_strategy == "2pl":
            self._node._acquire_shared_lock(request.key, request.tx_id, context)

        # ------------------------------------------------------------------
        # Check for pending operations from other transactions touching this key.
        # If another transaction has modified the key but not committed yet,
        # we must ignore those buffered values and return the committed version.
        skip_cache = False
        with self._node._tx_lock:
            for tx_id, txdata in self._node.active_transactions.items():
                if tx_id == (request.tx_id or ""):
                    continue
                for op, req in txdata.get("ops", []):
                    if req.key == request.key:
                        skip_cache = True
                        break
                if skip_cache:
                    break

        if skip_cache:
            if request.tx_id:
                raise RuntimeError("Conflict")
            records = self._node.db.get_record(
                request.key,
                tx_id=request.tx_id or None,
                in_progress=list(request.in_progress),
            )
        else:
            records = self._node._cache_get(request.key)
            if records is None:
                records = self._node.db.get_record(
                    request.key,
                    tx_id=request.tx_id or None,
                    in_progress=list(request.in_progress),
                )
                if records:
                    self._node._cache_set(request.key, records)
        if not records:
            if request.tx_id:
                with self._node._tx_lock:
                    txdata = self._node.active_transactions.setdefault(
                        request.tx_id,
                        {
                            "ops": [],
                            "read_versions": {},
                            "reads": set(),
                            "writes": set(),
                        },
                    )
                    txdata["read_versions"][request.key] = None
                    txdata["reads"].add(request.key)
            return replication_pb2.ValueResponse(values=[])

        # Choose the visible version we return and record its originating tx_id
        mode = self._node.consistency_mode
        best_rec = records[0]
        best_vc = best_rec[1]
        best_tx = best_rec[2] if len(best_rec) > 2 else None
        best_ts = best_vc.clock.get("ts", 0)
        for val, vc, *rest in records[1:]:
            ts = vc.clock.get("ts", 0)
            if mode in ("vector", "crdt"):
                cmp = vc.compare(best_vc)
                if cmp == ">" or (cmp is None and ts > best_ts):
                    best_vc = vc
                    best_tx = rest[0] if len(rest) > 0 else None
                    best_ts = ts
            else:
                if ts > best_ts:
                    best_vc = vc
                    best_tx = rest[0] if len(rest) > 0 else None
                    best_ts = ts

        if request.tx_id:
            with self._node._tx_lock:
                txdata = self._node.active_transactions.setdefault(
                    request.tx_id,
                    {
                        "ops": [],
                        "read_versions": {},
                        "reads": set(),
                        "writes": set(),
                    },
                )
                txdata["read_versions"][request.key] = best_tx
                txdata["reads"].add(request.key)

        values = []
        for val, vc, *_ in records:
            ts = vc.clock.get("ts", 0) if vc is not None else 0
            values.append(
                replication_pb2.VersionedValue(
                    value=val,
                    timestamp=ts,
                    vector=replication_pb2.VersionVector(items=vc.clock if vc else {}),
                )
            )

        return replication_pb2.ValueResponse(values=values)

    def GetForUpdate(self, request, context):
        """Acquire a lock on the key and return its current value."""
        owner_id = self._owner_for_key(request.key)
        if getattr(self._node, "enable_forwarding", False) and owner_id != self._node.node_id:
            client = self._node.clients_by_id.get(owner_id)
            if client:
                return client.stub.GetForUpdate(request)
        if not request.tx_id:
            if context:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "MissingTxId")
            raise RuntimeError("MissingTxId")
        self._node._acquire_exclusive_lock(request.key, request.tx_id, context)
        resp = self.Get(request, context)
        # Remove the key from the transaction's read set since the exclusive
        # lock ensures no concurrent writes can cause a conflict.
        with self._node._tx_lock:
            txdata = self._node.active_transactions.get(request.tx_id)
            if txdata:
                txdata.get("reads", set()).discard(request.key)
        return resp

    def Increment(self, request, context):
        """Atomically increment a numeric value."""
        lock = self._node._increment_locks.setdefault(
            request.key, threading.Lock()
        )
        lock.acquire()
        try:
            cur = self._node.db.get(request.key)
            try:
                cur_val = int(cur) if cur is not None else 0
            except Exception:
                cur_val = 0
            new_val = cur_val + request.amount
            ts = self._node.clock.tick()
            self._node.db.put(request.key, str(new_val), timestamp=ts)
        finally:
            lock.release()
        return replication_pb2.Empty()

    def Transfer(self, request, context):
        """Atomically transfer amount from one key to another."""
        global_transfer_lock.acquire()
        try:
            from_val = self._node.db.get(request.from_key)
            to_val = self._node.db.get(request.to_key)

            def _to_int(value):
                if isinstance(value, list):
                    value = value[-1]
                try:
                    return int(value) if value is not None else 0
                except Exception:
                    return 0

            from_balance = _to_int(from_val)
            to_balance = _to_int(to_val)
            if from_balance < request.amount:
                if context:
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "InsufficientFunds")
                raise RuntimeError("InsufficientFunds")
            new_from = from_balance - request.amount
            new_to = to_balance + request.amount
            ts = self._node.clock.tick()
            self._node.db.put(request.from_key, str(new_from), timestamp=ts)
            self._node.db.put(request.to_key, str(new_to), timestamp=ts)
        finally:
            global_transfer_lock.release()
        return replication_pb2.Empty()

    def BeginTransaction(self, request, context):
        tx_id = uuid.uuid4().hex
        with self._node._tx_lock:
            snapshot = list(self._node.active_transactions.keys())
            self._node.active_transactions[tx_id] = {
                "ops": [],
                "read_versions": {},
                "reads": set(),
                "writes": set(),
                "start_time": time.time(),
            }
        msg = f"Transação {tx_id} iniciada."
        if self._node.event_logger:
            self._node.event_logger.log(msg)
        else:
            logger.info(msg)
        return replication_pb2.TransactionId(id=tx_id, in_progress=snapshot)

    def CommitTransaction(self, request, context):
        with self._node._tx_lock:
            txdata = self._node.active_transactions.pop(
                request.tx_id,
                {"ops": [], "read_versions": {}, "reads": set(), "writes": set()},
            )
        ops = txdata.get("ops", [])
        reads = txdata.get("read_versions", {})
        read_keys = txdata.get("reads", set())
        start_time = txdata.get("start_time", 0)

        # Detect write conflicts based on versions read
        for op, req in ops:
            if req.key in reads:
                read_txid = reads[req.key]
                latest = self._node._latest_txid(req.key)
                if latest != read_txid:
                    with self._node._tx_lock:
                        locked = self._node.locks_by_tx.pop(request.tx_id, set())
                        for k in locked:
                            lock = self._node.locks.get(k)
                            if lock and request.tx_id in lock.get("owners", set()):
                                lock["owners"].discard(request.tx_id)
                                if not lock["owners"]:
                                    self._node.locks.pop(k, None)
                            with self._node._write_lock:
                                if self._node.write_locks.get(k) == request.tx_id:
                                    self._node.write_locks.pop(k, None)
                    raise RuntimeError("Conflict")

        # Detect read-write conflicts with transactions committed after start
        if read_keys:
            with self._node._tx_lock:
                committed = list(self._node.committed_transactions)
            for other in committed:
                if other.get("commit_time", 0) > start_time:
                    if read_keys & other.get("writes", set()):
                        with self._node._tx_lock:
                            locked = self._node.locks_by_tx.pop(request.tx_id, set())
                            for k in locked:
                                lock = self._node.locks.get(k)
                                if lock and request.tx_id in lock.get("owners", set()):
                                    lock["owners"].discard(request.tx_id)
                                    if not lock["owners"]:
                                        self._node.locks.pop(k, None)
                                with self._node._write_lock:
                                    if self._node.write_locks.get(k) == request.tx_id:
                                        self._node.write_locks.pop(k, None)
                        raise RuntimeError("Conflict")
        ops_by_key = {}
        for op, req in ops:
            ops_by_key[req.key] = (op, req)

        for op, req in ops_by_key.values():
            if op == "put":
                new_vc = (
                    VectorClock(dict(req.vector.items))
                    if req.vector.items
                    else VectorClock({"ts": int(req.timestamp)})
                )
                self._node.db.put(
                    req.key,
                    req.value,
                    vector_clock=new_vc,
                    tx_id=request.tx_id,
                )
                self._node._cache_delete(req.key)
                self._node.replicate(
                    "PUT",
                    req.key,
                    req.value,
                    req.timestamp,
                    op_id=req.op_id,
                    vector=new_vc.clock,
                    skip_id=(req.node_id if req.node_id != self._node.node_id else None),
                )
            else:
                new_vc = (
                    VectorClock(dict(req.vector.items))
                    if req.vector.items
                    else VectorClock({"ts": int(req.timestamp)})
                )
                self._node.db.delete(
                    req.key,
                    vector_clock=new_vc,
                    tx_id=request.tx_id,
                )
                self._node._cache_delete(req.key)
                self._node.replicate(
                    "DELETE",
                    req.key,
                    None,
                    req.timestamp,
                    op_id=req.op_id,
                    vector=new_vc.clock,
                    skip_id=(req.node_id if req.node_id != self._node.node_id else None),
                )
        commit_info = {
            "tx_id": request.tx_id,
            "commit_time": time.time(),
            "writes": set(txdata.get("writes", set())),
        }
        with self._node._tx_lock:
            self._node.committed_transactions.append(commit_info)
        self._node._release_locks(request.tx_id)
        msg = f"Transação {request.tx_id} commit realizada com sucesso."
        if self._node.event_logger:
            self._node.event_logger.log(msg)
        else:
            logger.info(msg)
        return replication_pb2.Empty()

    def AbortTransaction(self, request, context):
        with self._node._tx_lock:
            self._node.active_transactions.pop(request.tx_id, None)
        self._node._release_locks(request.tx_id)
        msg = f"Transação {request.tx_id} abortada."
        if self._node.event_logger:
            self._node.event_logger.log(msg)
        else:
            logger.info(msg)
        return replication_pb2.Empty()

    def ListTransactions(self, request, context):
        """Return IDs of currently active transactions."""
        with self._node._tx_lock:
            tx_ids = list(self._node.active_transactions.keys())
        return replication_pb2.TransactionList(tx_ids=tx_ids)

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
        for op_id, (key, value, ts) in list(self._node.replication_log.items()):
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
            for seg, h in list(local_hashes.items()):
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
                    for val, vc, *_ in self._node.db.get_record(key):
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

    def UpdateHashRing(self, request, context):
        """Replace the node's hash ring."""
        entries = [(int(e.hash), e.node_id) for e in request.items]
        self._node.update_hash_ring(entries)
        return replication_pb2.Empty()

    def ListByIndex(self, request, context):
        """Return keys matching an index query."""
        try:
            value = json.loads(request.value)
        except Exception:
            value = request.value
        if request.field in getattr(self._node.global_index_manager, "fields", []):
            keys = self._node.global_index_manager.query(request.field, value)
        else:
            keys = self._node.query_index(request.field, value)
        return replication_pb2.KeyList(keys=keys)

    def GetNodeInfo(self, request, context):
        """Return information about this node."""
        return self._node.get_node_info()

    def GetReplicationStatus(self, request, context):
        """Return replication metadata like last seen sequences and hints."""
        return self._node.get_replication_status()

    def GetWalEntries(self, request, context):
        """Return entries currently stored in the node WAL."""
        entries = self._node.get_wal_entries()
        return replication_pb2.WalEntriesResponse(entries=entries)

    def GetMemtableEntries(self, request, context):
        """Return key/value pairs stored in the MemTable."""
        entries = self._node.get_memtable_entries()
        return replication_pb2.StorageEntriesResponse(entries=entries)

    def GetSSTables(self, request, context):
        """Return metadata for SSTable segments on disk."""
        tables = self._node.get_sstables()
        return replication_pb2.SSTableInfoResponse(tables=tables)

    def GetSSTableContent(self, request, context):
        """Return all key/value pairs for a specific SSTable."""
        # ``SSTableContentRequest`` defines the field ``sstable_id`` to specify
        # which table should be returned. Older versions used ``id`` so handle
        # both for compatibility.
        sst_id = getattr(request, "sstable_id", None) or getattr(request, "id", "")
        entries = self._node.get_sstable_content(sst_id)
        return replication_pb2.StorageEntriesResponse(entries=entries)

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
        registry_host: str | None = None,
        registry_port: int | None = None,
        event_logger: EventLogger | str | None = None,
        tx_lock_strategy: str = "2pl",
        lock_timeout: float = 1.0,
    ):
        self.db_path = db_path
        if isinstance(event_logger, str):
            event_logger = EventLogger(event_logger)
        self.event_logger = event_logger
        self.db = SimpleLSMDB(db_path=db_path, event_logger=event_logger)
        self.start_time = time.time()
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
        self.tx_lock_strategy = tx_lock_strategy
        self.lock_timeout = float(lock_timeout)
        self.cache_size = int(cache_size)
        self.cache = OrderedDict() if self.cache_size > 0 else None
        self.index_fields = list(index_fields or [])
        self.index_manager = IndexManager(self.index_fields)
        self.index_manager.rebuild(self.db)
        self.global_index_fields = list(global_index_fields or [])
        self.global_index_manager = GlobalIndexManager(self.global_index_fields)
        self.registry_host = registry_host
        self.registry_port = registry_port
        self._registry_channel = None
        self._registry_stub = None
        self._registry_stop = threading.Event()
        self._registry_thread = None
        self._registry_watch_thread = None

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

        self.start_time = time.time()
        self.clock = LamportClock()
        self.vector_clock = VectorClock()
        self.local_seq = 0
        self.last_seen: dict[str, int] = {}
        self.replication_log: dict[str, tuple] = {}
        # Track operations, read versions and read/write sets for active
        # transactions
        # ``{tx_id: {"ops": [(op, request), ...],
        #            "read_versions": {key: read_txid},
        #            "reads": set(), "writes": set()}}``
        self.active_transactions: dict[str, dict] = {}
        # Track committed transactions for SSI conflict detection
        # ``[{"tx_id": str, "commit_time": float, "writes": set[str]}]``
        self.committed_transactions: list[dict] = []
        # Simple lock manager mapping key -> {'type': lock_type, 'owners': {tx_ids}}
        self.locks: dict[str, dict] = {}
        self.locks_by_tx: dict[str, set[str]] = {}
        # Write locks used for transactional writes
        self.write_locks: dict[str, str] = {}
        self._write_lock = threading.Lock()
        self._tx_lock = threading.Lock()
        self._mem_lock = threading.Lock()
        # Locks used to protect atomic increment operations per key
        self._increment_locks: dict[str, threading.Lock] = {}
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
        self._set_peers(self.peers)

        self.hints: dict[str, list[tuple]] = {}
        self._replog_fp = None
        self._replog_lock = threading.Lock()

        # Initialize CRDT instances for configured keys
        self.crdts = {}
        # Import CRDT implementations from the utils package. This path changed
        # when the project was reorganised into packages.
        from ...utils.crdt import GCounter, ORSet
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

    def update_hash_ring(self, entries) -> None:
        """Rebuild hash ring from list of ``(hash_int, node_id)`` entries."""
        if not entries:
            self.hash_ring = None
            return
        ring = HashRing()
        for h, nid in entries:
            ring._ring.append((int(h), nid))
            ring._nodes.setdefault(nid, []).append((int(h), nid))
        ring._ring.sort(key=lambda x: x[0])
        self.hash_ring = ring

    def query_index(self, field: str, value) -> list[str]:
        """Return list of keys matching ``field``/``value`` in the secondary index."""
        return self.index_manager.query(field, value)

    def get_index_owner(self, field: str, value) -> str:
        """Return node_id responsible for ``field``/``value`` index key."""
        key = f"idx:{field}:{value}"
        # Delegate to ReplicaService logic for owner determination
        return self.service._owner_for_key(key)

    # registry helpers ----------------------------------------------------
    def _set_peers(self, peers) -> None:
        """Replace peer list and rebuild gRPC clients."""
        self.peers = peers
        self.peer_clients = []
        self.client_map = {}
        self.clients_by_id = {}
        self.peer_status = {}
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

    def _apply_cluster_state(self, state) -> None:
        """Update peers and partition map from ClusterState message."""
        peers = [
            (n.host, n.port, n.node_id)
            for n in getattr(state, "nodes", [])
            if n.node_id != self.node_id
        ]
        pmap = getattr(state, "partition_map", None)
        if pmap is not None and hasattr(pmap, "items"):
            self.partition_map = dict(pmap.items)
        self._set_peers(peers)

    def _iter_peers(self):
        """Yield tuples of (host, port, node_id, client) for all peers."""
        if self.clients_by_id:
            return [
                (c.host, c.port, node_id, c)
                for node_id, c in list(self.clients_by_id.items())
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

    # lock helpers ------------------------------------------------
    def _acquire_shared_lock(self, key: str, tx_id: str, context=None):
        """Acquire a shared (read) lock for ``tx_id`` on ``key``."""
        if self.tx_lock_strategy != "2pl":
            return
        deadline = time.time() + self.lock_timeout
        while True:
            with self._tx_lock:
                lock = self.locks.get(key)
                if lock is None:
                    self.locks[key] = {"type": "shared", "owners": {tx_id}}
                    self.locks_by_tx.setdefault(tx_id, set()).add(key)
                    return
                if lock["type"] == "exclusive" and tx_id not in lock.get("owners", set()):
                    pass  # wait
                else:
                    lock["owners"].add(tx_id)
                    self.locks_by_tx.setdefault(tx_id, set()).add(key)
                    return
            if time.time() >= deadline:
                if context:
                    context.abort(grpc.StatusCode.ABORTED, "Deadlock")
                raise RuntimeError("Deadlock")
            time.sleep(0.01)

    def _acquire_exclusive_lock(self, key: str, tx_id: str, context=None):
        """Acquire an exclusive (write) lock for ``tx_id`` on ``key``."""
        deadline = time.time() + self.lock_timeout
        while True:
            with self._tx_lock:
                lock = self.locks.get(key)
                if lock is None:
                    self.locks[key] = {"type": "exclusive", "owners": {tx_id}}
                    self.locks_by_tx.setdefault(tx_id, set()).add(key)
                    return
                elif lock["type"] == "exclusive":
                    if tx_id in lock.get("owners", set()):
                        self.locks_by_tx.setdefault(tx_id, set()).add(key)
                        return
                else:  # shared lock
                    owners = lock.get("owners", set())
                    if owners == {tx_id}:
                        lock["type"] = "exclusive"
                        self.locks_by_tx.setdefault(tx_id, set()).add(key)
                        return
            if time.time() >= deadline:
                if context:
                    context.abort(grpc.StatusCode.ABORTED, "Deadlock")
                raise RuntimeError("Deadlock")
            time.sleep(0.01)

    def _release_locks(self, tx_id: str) -> None:
        """Release all locks held by ``tx_id``."""
        with self._tx_lock:
            locked = self.locks_by_tx.pop(tx_id, set())
            for k in locked:
                lock = self.locks.get(k)
                if lock and tx_id in lock.get("owners", set()):
                    lock["owners"].discard(tx_id)
                    if not lock["owners"]:
                        self.locks.pop(k, None)
                with self._write_lock:
                    if self.write_locks.get(k) == tx_id:
                        self.write_locks.pop(k, None)

    def _latest_txid(self, key: str):
        """Return the created_txid of the most recent visible version."""
        records = self.db.get_record(key)
        if not records:
            return None
        mode = self.consistency_mode
        if mode in ("vector", "crdt"):
            val, best_vc, *rest = records[0]
            best_tx = rest[0] if len(rest) > 0 else None
            best_ts = best_vc.clock.get("ts", 0)
            for _val, vc, *r in records[1:]:
                cmp = vc.compare(best_vc)
                ts = vc.clock.get("ts", 0)
                if cmp == ">" or (cmp is None and ts > best_ts):
                    best_vc = vc
                    best_tx = r[0] if len(r) > 0 else None
                    best_ts = ts
        else:
            best_tx = None
            best_ts = -1
            for _val, vc, *r in records:
                ts = vc.clock.get("ts", 0)
                if ts > best_ts or (ts == best_ts and best_tx is None):
                    best_tx = r[0] if len(r) > 0 else None
                    best_ts = ts
        return best_tx

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
        with self._replog_lock:
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

    def get_node_info(self):
        """Return runtime statistics for this node."""

        # Actual resource monitoring may require the ``psutil`` package.
        log_file = self._replication_log_file()
        hints_file = self._hints_file()
        log_size = os.path.getsize(log_file) if os.path.exists(log_file) else 0
        hints_size = os.path.getsize(hints_file) if os.path.exists(hints_file) else 0
        try:
            import psutil  # type: ignore

            cpu = psutil.cpu_percent(interval=None)
            memory = psutil.virtual_memory().percent
            disk = psutil.disk_usage(self.db_path).percent
        except Exception:
            cpu = memory = disk = 0.0

        uptime = time.time() - self.start_time
        return replication_pb2.NodeInfoResponse(
            node_id=self.node_id,
            status="running",
            cpu=cpu,
            memory=memory,
            disk=disk,
            uptime=int(uptime),
            replication_log_size=log_size,
            hints_count=hints_size,
        )

    def get_replication_status(self):
        """Return last seen sequence numbers and hint counts."""

        hints_count = {peer: len(ops) for peer, ops in list(self.hints.items())}
        return replication_pb2.ReplicationStatusResponse(
            last_seen={peer: int(seq) for peer, seq in list(self.last_seen.items())},
            hints=hints_count,
        )

    def get_wal_entries(self):
        """Return WAL operations still stored on disk."""
        entries = []
        for _idx, op_type, key, (val, vc) in self.db.wal.read_all():
            entries.append(
                replication_pb2.WalEntry(
                    type=op_type,
                    key=key,
                    value="" if val is None else str(val),
                    vector=replication_pb2.VersionVector(items=vc.clock),
                )
            )
        return entries

    def get_memtable_entries(self):
        """Return current MemTable items."""
        entries = []
        for key, versions in self.db.memtable.get_sorted_items():
            for val, vc, *_ in versions:
                if val == "__TOMBSTONE__":
                    continue
                entries.append(
                    replication_pb2.StorageEntry(
                        key=key,
                        value=str(val),
                        vector=replication_pb2.VersionVector(items=vc.clock),
                    )
                )
        return entries

    def get_sstables(self):
        """Return metadata about SSTables stored by this node."""
        tables = []
        for _ts, path, _index in self.db.sstable_manager.sstable_segments:
            size = os.path.getsize(path) // 1024
            item_count = 0
            first_key = last_key = ""
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except Exception:
                        continue
                    k = data.get("key", "")
                    if not first_key:
                        first_key = k
                    last_key = k
                    item_count += 1
            tables.append(
                replication_pb2.SSTableInfo(
                    id=os.path.basename(path),
                    level=0,
                    size=size,
                    item_count=item_count,
                    start_key=first_key,
                    end_key=last_key,
                )
            )
        return tables

    def get_sstable_content(self, sstable_id: str):
        """Return all entries stored in ``sstable_id``."""
        entries = []
        for _ts, path, _index in self.db.sstable_manager.sstable_segments:
            if os.path.basename(path) != sstable_id:
                continue
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except Exception:
                        continue
                    entries.append(
                        replication_pb2.StorageEntry(
                            key=data.get("key", ""),
                            value=str(data.get("value", "")),
                            vector=replication_pb2.VersionVector(
                                items=data.get("vector", {})
                            ),
                        )
                    )
            break
        return entries

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
        if to_remove:
            with self._replog_lock:
                for op_id in to_remove:
                    self.replication_log.pop(op_id, None)
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
                # Attempt handoff regardless of heartbeat status to reduce
                # latency when a node comes back online.
                client = self.clients_by_id.get(peer_id) or self.client_map.get(peer_id)
                if not client:
                    continue
                remaining = []
                delivered = 0
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
                        delivered += 1
                    except Exception:
                        remaining.append((h_op_id, h_op, h_key, h_val, h_ts))
                if delivered:
                    msg = (
                        f"Hinted handoff para {peer_id}: {delivered} "
                        f"operacao{'es' if delivered > 1 else ''} entregues"
                    )
                    if remaining:
                        msg += f", {len(remaining)} pendente{'s' if len(remaining) > 1 else ''}"
                    if self.event_logger:
                        self.event_logger.log(msg)
                    else:
                        print(msg)
                if remaining:
                    self.hints[peer_id] = [list(r) for r in remaining]
                else:
                    self.hints.pop(peer_id, None)
                updated = True
            if updated:
                self.save_hints()
            time.sleep(self.hinted_handoff_interval)

    def _registry_heartbeat_loop(self) -> None:
        if not self._registry_stub:
            return
        while not self._registry_stop.is_set():
            try:
                resp = self._registry_stub.Heartbeat(
                    metadata_pb2.HeartbeatRequest(node_id=self.node_id)
                )
                self._apply_cluster_state(resp)
            except Exception:
                pass
            time.sleep(self.heartbeat_interval)

    def _registry_watch_loop(self) -> None:
        if not self._registry_stub:
            return
        while not self._registry_stop.is_set():
            try:
                stream = self._registry_stub.WatchClusterState(replication_pb2.Empty())
                for state in stream:
                    self._apply_cluster_state(state)
                    if self._registry_stop.is_set():
                        break
            except Exception:
                time.sleep(self.heartbeat_interval)

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

    def _start_registry_thread(self) -> None:
        if (
            not self.registry_host
            or not self.registry_port
            or (self._registry_thread and self._registry_thread.is_alive())
        ):
            return
        t = threading.Thread(target=self._registry_heartbeat_loop, daemon=True)
        self._registry_thread = t
        t.start()

    def _start_registry_watch_thread(self) -> None:
        if (
            not self.registry_host
            or not self.registry_port
            or (self._registry_watch_thread and self._registry_watch_thread.is_alive())
        ):
            return
        t = threading.Thread(target=self._registry_watch_loop, daemon=True)
        self._registry_watch_thread = t
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
                if node_id == self.node_id or node_id == skip_id:
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
                if (
                    node_id == self.node_id
                    or node_id == skip_id
                    or any(p[2] == node_id for p in peer_list)
                    or node_id in missing
                ):
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
                if node_id == skip_id:
                    continue
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
                if (
                    node_id == skip_id
                    or any(p[2] == node_id for p in peer_list)
                    or node_id in missing
                    or self.peer_status.get(node_id) is None
                ):
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
        for seg in list(hashes):
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
        if self.registry_host and self.registry_port:
            self._registry_channel = grpc.insecure_channel(
                f"{self.registry_host}:{self.registry_port}"
            )
            self._registry_stub = metadata_pb2_grpc.MetadataServiceStub(
                self._registry_channel
            )
            try:
                resp = self._registry_stub.RegisterNode(
                    metadata_pb2.RegisterRequest(
                        node=metadata_pb2.NodeInfo(
                            node_id=self.node_id, host=self.host, port=self.port
                        )
                    )
                )
                self._apply_cluster_state(resp)
            except Exception:
                pass
            self._start_registry_thread()
            self._start_registry_watch_thread()
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
        self._registry_stop.set()
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
        if self._registry_thread:
            self._registry_thread.join()
        if self._registry_watch_thread:
            self._registry_watch_thread.join()
        for _, _, _, c in self._iter_peers():
            c.close()
        if self._registry_channel:
            self._registry_channel.close()
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
    registry_host: str | None = None,
    registry_port: int | None = None,
    event_logger: EventLogger | str | None = None,
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
        registry_host=registry_host,
        registry_port=registry_port,
        event_logger=event_logger,
    )
    node.start()

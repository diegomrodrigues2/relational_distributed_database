"""Simple multi-leader replication utilities."""

import os
import time
import shutil
import multiprocessing
import threading
import hashlib
import random
from partitioning import hash_key, compose_key
from dataclasses import dataclass
from concurrent import futures

from replica.grpc_server import run_server
from replica.client import GRPCReplicaClient
from hash_ring import HashRing as ConsistentHashRing
from vector_clock import VectorClock
from lsm_db import _merge_version_lists


@dataclass
class ClusterNode:
    node_id: str
    host: str
    port: int
    process: multiprocessing.Process
    client: GRPCReplicaClient

    def put(self, key, value):
        ts = int(time.time() * 1000)
        self.client.put(key, value, timestamp=ts, node_id=self.node_id)

    def delete(self, key):
        ts = int(time.time() * 1000)
        self.client.delete(key, timestamp=ts, node_id=self.node_id)

    def get(self, key):
        recs = self.client.get(key)
        return recs[0][0] if recs else None

    def stop(self):
        if self.process.is_alive():
            self.process.terminate()
        self.process.join()
        self.client.close()


class NodeCluster:
    """Launches multiple nodes that replicate to each other."""

    def __init__(
        self,
        base_path: str,
        num_nodes: int = 3,
        topology: dict[int, list[int]] | None = None,
        *,
        consistency_mode: str = "lww",
        replication_factor: int | None = None,
        write_quorum: int | None = None,
        read_quorum: int | None = None,
        key_ranges: list | None = None,
        partition_strategy: str = "range",
        num_partitions: int | None = None,
    ):
        self.base_path = base_path
        if os.path.exists(base_path):
            shutil.rmtree(base_path)
        os.makedirs(base_path)

        base_port = 9000
        self.nodes = []
        self.nodes_by_id: dict[str, ClusterNode] = {}
        self.salted_keys: dict[str, int] = {}
        self.consistency_mode = consistency_mode
        if replication_factor is None:
            replication_factor = 1 if key_ranges else 3
        self.replication_factor = replication_factor
        self.write_quorum = write_quorum or (replication_factor // 2 + 1)
        self.read_quorum = read_quorum or (replication_factor // 2 + 1)
        self.partition_strategy = partition_strategy
        if partition_strategy not in ("range", "hash"):
            raise ValueError("invalid partition_strategy")
        self.key_ranges = None
        self.partitions: list[tuple[tuple, ClusterNode]] = []
        self.ring = None if key_ranges else ConsistentHashRing()
        if num_partitions is None:
            num_partitions = num_nodes
        if partition_strategy == "hash" or key_ranges is None:
            self.num_partitions = num_partitions
        peers = [
            ("localhost", base_port + i, f"node_{i}")
            for i in range(num_nodes)
        ]
        if self.ring is not None:
            for _, _, nid in peers:
                self.ring.add_node(nid)

        for i in range(num_nodes):
            node_id = f"node_{i}"
            db_path = os.path.join(base_path, node_id)
            port = base_port + i
            if topology is None:
                peers_i = peers
            else:
                peers_i = [
                    ("localhost", base_port + j, f"node_{j}")
                    for j in topology.get(i, [])
                ]

            rf = self.replication_factor
            if key_ranges is not None:
                peers_i = []
                rf = 1

            p = multiprocessing.Process(
                target=run_server,
                args=(
                    db_path,
                    "localhost",
                    port,
                    node_id,
                    peers_i,
                    self.ring,
                    rf,
                    self.write_quorum,
                    self.read_quorum,
                ),
                kwargs={"consistency_mode": self.consistency_mode},
                daemon=True,
            )
            p.start()
            time.sleep(0.2)
            client = GRPCReplicaClient("localhost", port)
            node = ClusterNode(node_id, "localhost", port, p, client)
            self.nodes.append(node)
            self.nodes_by_id[node_id] = node

        time.sleep(1)
        if key_ranges is not None:
            self._setup_partitions(key_ranges)

    def enable_salt(self, key: str, buckets: int) -> None:
        """Enable random prefixing for ``key`` using ``buckets`` variants."""
        if buckets < 1:
            raise ValueError("buckets must be >= 1")
        self.salted_keys[str(key)] = int(buckets)

    def get_node_for_key(
        self, partition_key: str, clustering_key: str | None = None
    ) -> ClusterNode:
        """Return the node responsible for ``partition_key`` based on ``key_ranges``."""
        if self.key_ranges is None:
            raise ValueError("key_ranges not configured")
        for (start, end), node in self.partitions:
            if start <= partition_key < end:
                return node
        return self.partitions[-1][1]

    def _setup_partitions(self, key_ranges: list) -> None:
        if not key_ranges:
            raise ValueError("key_ranges cannot be empty")
        if all(isinstance(r, tuple) and len(r) == 2 for r in key_ranges):
            ranges = list(key_ranges)
        else:
            if len(key_ranges) < 2:
                raise ValueError("key_ranges must have at least two boundaries")
            ranges = [
                (key_ranges[i], key_ranges[i + 1])
                for i in range(len(key_ranges) - 1)
            ]
        last_end = None
        for start, end in ranges:
            if last_end is not None and start < last_end:
                raise ValueError("key_ranges must be ordered and non-overlapping")
            if start >= end:
                raise ValueError("invalid range")
            last_end = end
        self.key_ranges = ranges
        self.partitions = [
            (rng, self.nodes[i % len(self.nodes)])
            for i, rng in enumerate(ranges)
        ]
        self.num_partitions = len(ranges)

    def get_partition_id(
        self, partition_key: str, clustering_key: str | None = None
    ) -> int:
        """Return partition id for ``partition_key`` based on hashing."""
        return hash_key(partition_key) % self.num_partitions

    def _coordinator(
        self, partition_key: str, clustering_key: str | None = None
    ) -> ClusterNode:
        if self.partition_strategy == "hash":
            pid = self.get_partition_id(partition_key, clustering_key)
            return self.nodes[pid % len(self.nodes)]
        if self.key_ranges is not None:
            return self.get_node_for_key(partition_key, clustering_key)
        if self.ring is None:
            h = hash_key(partition_key)
            for (start, end), node in self.partitions:
                if start <= h < end:
                    return node
            return self.partitions[-1][1]
        node_id = self.ring.get_preference_list(partition_key, 1)[0]
        return self.nodes_by_id[node_id]

    def put(
        self,
        node_index: int,
        partition_key: str,
        clustering_key: str,
        value: str | None = None,
    ):
        """Insert ``value`` using ``partition_key`` and ``clustering_key``.

        For backwards compatibility ``clustering_key`` may actually be the
        ``value`` when only a single key component is provided.
        """
        if value is None:
            value = clustering_key
            clustering_key = None
        if partition_key in self.salted_keys:
            buckets = self.salted_keys[partition_key]
            prefix = random.randint(0, buckets - 1)
            partition_key = f"{prefix}#{partition_key}"
        composed_key = compose_key(partition_key, clustering_key)
        node = self._coordinator(partition_key, clustering_key)
        node.put(composed_key, value)

    def delete(
        self,
        node_index: int,
        partition_key: str,
        clustering_key: str | None = None,
    ):
        """Delete key identified by ``partition_key`` and ``clustering_key``.

        Backwards compatible with calls that omit ``clustering_key`` and pass a
        single combined key as ``partition_key``.
        """
        composed_key = compose_key(partition_key, clustering_key)
        node = self._coordinator(partition_key, clustering_key)
        node.delete(composed_key)

    def get(
        self,
        node_index: int,
        partition_key: str,
        clustering_key: str | None = None,
        *,
        merge: bool = True,
        ignore_salt: bool = False,
    ):
        """Retrieve the value stored under ``partition_key`` and ``clustering_key``.

        The optional ``clustering_key`` keeps compatibility with the previous
        API where a single key string was used.
        """
        if not ignore_salt and partition_key in self.salted_keys:
            buckets = self.salted_keys[partition_key]
            merged = []
            for i in range(buckets):
                skey = f"{i}#{partition_key}"
                vals = self.get(
                    node_index,
                    skey,
                    clustering_key,
                    merge=False,
                    ignore_salt=True,
                )
                for val, vc_dict in vals or []:
                    merged = _merge_version_lists(merged, [(val, VectorClock(vc_dict))])
            if not merged:
                return None if merge else []
            if merge:
                if self.consistency_mode in ("vector", "crdt"):
                    best_val, best_vc = merged[0]
                    best_ts = best_vc.clock.get("ts", 0)
                    for val, vc in merged[1:]:
                        cmp = vc.compare(best_vc)
                        ts = vc.clock.get("ts", 0)
                        if cmp == ">" or (cmp is None and ts > best_ts):
                            best_val, best_vc, best_ts = val, vc, ts
                else:
                    best_val = None
                    best_ts = -1
                    best_vc = None
                    for val, vc in merged:
                        ts = vc.clock.get("ts", 0)
                        if ts > best_ts:
                            best_val, best_vc, best_ts = val, vc, ts
                return best_val
            else:
                return [(val, vc.clock) for val, vc in merged]

        composed_key = compose_key(partition_key, clustering_key)
        if self.partition_strategy == "hash":
            node = self._coordinator(partition_key, clustering_key)
            recs = node.client.get(composed_key)
            if merge:
                return recs[0][0] if recs else None
            return [(val, vc_dict) for val, ts, vc_dict in recs]
        if self.key_ranges is not None:
            node = self.get_node_for_key(partition_key, clustering_key)
            recs = node.client.get(composed_key)
            if merge:
                return recs[0][0] if recs else None
            return [(val, vc_dict) for val, ts, vc_dict in recs]
        if self.ring is None:
            node = self._coordinator(partition_key, clustering_key)
            recs = node.client.get(composed_key)
            if merge:
                return recs[0][0] if recs else None
            return [(val, vc_dict) for val, ts, vc_dict in recs]
        pref_nodes = self.ring.get_preference_list(
            partition_key, self.replication_factor
        )
        nodes = []
        for nid in pref_nodes:
            n = self.nodes_by_id[nid]
            try:
                n.client.ping(n.node_id)
                nodes.append(n)
            except Exception:
                continue

        if len(nodes) < self.read_quorum:
            all_nodes = self.ring.get_preference_list(partition_key, len(self.nodes))
            for nid in all_nodes:
                if len(nodes) >= self.read_quorum:
                    break
                if nid in pref_nodes or any(x.node_id == nid for x in nodes):
                    continue
                n = self.nodes_by_id[nid]
                try:
                    n.client.ping(n.node_id)
                    nodes.append(n)
                except Exception:
                    continue

        if not nodes:
            return None

        responses = []
        with futures.ThreadPoolExecutor(max_workers=len(nodes)) as ex:
            future_map = {ex.submit(n.client.get, composed_key): n for n in nodes}
            for fut in futures.as_completed(future_map):
                node = future_map[fut]
                try:
                    recs = fut.result()
                except Exception:
                    recs = []
                responses.append((node, recs))

        if not responses:
            return None

        merged = []
        for _, recs in responses:
            for val, ts, vc_dict in recs:
                merged = _merge_version_lists(merged, [(val, VectorClock(vc_dict))])

        if not merged:
            return None

        if merge:
            if self.consistency_mode in ("vector", "crdt"):
                best_val, best_vc = merged[0]
                best_ts = best_vc.clock.get("ts", 0)
                for val, vc in merged[1:]:
                    cmp = vc.compare(best_vc)
                    ts = vc.clock.get("ts", 0)
                    if cmp == ">" or (cmp is None and ts > best_ts):
                        best_val, best_vc, best_ts = val, vc, ts
            else:
                best_val = None
                best_ts = -1
                best_vc = None
                for val, vc in merged:
                    ts = vc.clock.get("ts", 0)
                    if ts > best_ts:
                        best_val, best_vc, best_ts = val, vc, ts

            stale_nodes = []
            for node, recs in responses:
                has = False
                for val, ts, vc_dict in recs:
                    if val == best_val and vc_dict == best_vc.clock:
                        has = True
                        break
                if not has:
                    stale_nodes.append(node)

            def _repair(n):
                try:
                    if self.consistency_mode in ("vector", "crdt"):
                        n.client.put(
                            composed_key,
                            best_val,
                            timestamp=best_ts,
                            node_id=n.node_id,
                            vector=best_vc.clock,
                        )
                    else:
                        n.client.put(
                            composed_key,
                            best_val,
                            timestamp=best_ts,
                            node_id=n.node_id,
                        )
                except Exception:
                    pass

            for sn in stale_nodes:
                t = threading.Thread(target=_repair, args=(sn,), daemon=True)
                t.start()

            return best_val
        else:
            return [(val, vc.clock) for val, vc in merged]

        def _repair(n):
            try:
                if self.consistency_mode in ("vector", "crdt"):
                    n.client.put(
                        composed_key,
                        best_val,
                        timestamp=best_ts,
                        node_id=n.node_id,
                        vector=best_vc_dict,
                    )
                else:
                    n.client.put(
                        composed_key,
                        best_val,
                        timestamp=best_ts,
                        node_id=n.node_id,
                    )
            except Exception:
                pass

        for sn in stale_nodes:
            t = threading.Thread(target=_repair, args=(sn,), daemon=True)
            t.start()

        return best_val

    def get_range(self, partition_key: str, start_ck: str, end_ck: str):
        """Return a list of (clustering_key, value) for a key range."""
        node = self._coordinator(partition_key, start_ck)
        items = node.client.scan_range(partition_key, start_ck, end_ck)
        return [(ck, val) for ck, val, _, _ in items]

    def shutdown(self):
        for n in self.nodes:
            n.stop()


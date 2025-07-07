"""Simple multi-leader replication utilities."""

import os
import time
import shutil
import multiprocessing
import threading
import hashlib
import random
import json
from bisect import bisect_right
from ..clustering.partitioning import (
    hash_key,
    compose_key,
    RangePartitioner,
    HashPartitioner,
    ConsistentHashPartitioner,
)
from dataclasses import dataclass
from concurrent import futures

from .replica.grpc_server import run_server
from .replica.client import GRPCReplicaClient, GRPCRouterClient
from .replica import metadata_pb2, metadata_pb2_grpc, replication_pb2
import grpc
from ..clustering.router_server import run_router
from ..clustering.metadata_service import run_metadata_service
from ..clustering.hash_ring import HashRing as ConsistentHashRing
from ..utils.vector_clock import VectorClock
from ..lsm.lsm_db import _merge_version_lists, SimpleLSMDB
from ..lsm.sstable import TOMBSTONE

from ..utils.event_logger import EventLogger

DEFAULT_NUM_PARTITIONS = 128


@dataclass
class ClusterNode:
    node_id: str
    host: str
    port: int
    process: multiprocessing.Process
    client: GRPCReplicaClient
    event_logger: EventLogger | None = None

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
        partitions_per_node: int = 1,
        max_transfer_rate: int | None = None,
        enable_forwarding: bool = False,
        load_balance_reads: bool = False,
        index_fields: list[str] | None = None,
        global_index_fields: list[str] | None = None,
        cold_check_interval: float | None = None,
        start_router: bool = False,
        router_port: int = 7000,
        use_registry: bool = False,
        registry_addr: tuple[str, int] | None = None,
    ):
        self.base_path = base_path
        if os.path.exists(base_path):
            shutil.rmtree(base_path)
        os.makedirs(base_path)

        # Initialize event logger for cluster operations
        self.event_logger = EventLogger(os.path.join(base_path, "event_log.txt"))
        self.event_logger.log("NodeCluster created")
        self.node_loggers: dict[str, EventLogger] = {}

        base_port = 9000
        self.base_port = base_port
        self.nodes = []
        self.nodes_by_id: dict[str, ClusterNode] = {}
        self.drivers: list = []
        self._tx_lock = threading.Lock()
        self._tx_counter = 0
        self.salted_keys: dict[str, int] = {}
        self.consistency_mode = consistency_mode
        if replication_factor is None:
            replication_factor = 1 if key_ranges else 3
        self.replication_factor = replication_factor
        self.write_quorum = write_quorum or (replication_factor // 2 + 1)
        self.read_quorum = read_quorum or (replication_factor // 2 + 1)
        # Partition strategies:
        #   "range" - keeps keys ordered in contiguous ranges allowing efficient
        #             scans (e.g. Bigtable/HBase). May lead to hotspots when
        #             some ranges grow faster than others.
        #   "hash"  - distributes keys via hashing for better balance as in
        #             Dynamo or Cassandra, but range scans are expensive.
        self.partition_strategy = partition_strategy
        if partition_strategy not in ("range", "hash"):
            raise ValueError("invalid partition_strategy")
        self.partitions_per_node = max(1, int(partitions_per_node))
        self.max_transfer_rate = max_transfer_rate
        self.enable_forwarding = enable_forwarding
        self.load_balance_reads = load_balance_reads
        self.index_fields = index_fields
        self.global_index_fields = global_index_fields
        self.cold_check_interval = cold_check_interval
        self.key_ranges = None
        self.partitions: list[tuple[tuple, ClusterNode]] = []
        self.partition_map: dict[int, str] = {}
        self.router_process: multiprocessing.Process | None = None
        self.router_client: GRPCRouterClient | None = None
        self.router_port = router_port
        self.use_registry = bool(use_registry)
        self.registry_addr = registry_addr or ("localhost", 9100)
        self.registry_process: multiprocessing.Process | None = None
        self._registry_channel = None
        self._registry_stub = None

        use_ring = not key_ranges and not (
            partition_strategy == "hash" and replication_factor == 1 and not enable_forwarding
        )
        if use_ring:
            self.partitioner = ConsistentHashPartitioner(
                partitions_per_node=self.partitions_per_node
            )
            self.ring = self.partitioner.ring
        else:
            self.partitioner = None
            self.ring = None
        if num_partitions is None:
            if partition_strategy == "hash" and not use_ring:
                num_partitions = DEFAULT_NUM_PARTITIONS
            else:
                num_partitions = num_nodes
        if not use_ring:
            self.num_partitions = num_partitions
            self.partition_ops = [0] * self.num_partitions
        else:
            self.num_partitions = 0
            self.partition_ops = []
        self.key_freq: dict[str, int] = {}
        self.partition_item_counts: dict[int, int] = {
            i: 0 for i in range(self.num_partitions)
        }
        self._known_keys: set[str] = set()
        peers = [
            ("localhost", base_port + i, f"node_{i}")
            for i in range(num_nodes)
        ]
        if self.use_registry:
            host, port = self.registry_addr
            self.registry_process = multiprocessing.Process(
                target=run_metadata_service,
                args=(host, port),
                daemon=True,
            )
            self.registry_process.start()
            time.sleep(0.5)
            self._registry_channel = grpc.insecure_channel(f"{host}:{port}")
            self._registry_stub = metadata_pb2_grpc.MetadataServiceStub(
                self._registry_channel
            )
        if use_ring:
            for _, _, nid in peers:
                self.partitioner.ring.add_node(nid, weight=self.partitions_per_node)

        # Build initial partition map before launching nodes
        if key_ranges is not None:
            if all(isinstance(r, tuple) and len(r) == 2 for r in key_ranges):
                ranges = list(key_ranges)
            else:
                ranges = [
                    (key_ranges[i], key_ranges[i + 1])
                    for i in range(len(key_ranges) - 1)
                ]
            self.num_partitions = len(ranges)
            self.partition_map = {
                i: peers[i % len(peers)][2] for i in range(self.num_partitions)
            }
        elif use_ring and self.partitioner.ring._ring:
            self.partition_map = self.partitioner.get_partition_map()
            self.num_partitions = len(self.partitioner.ring._ring)
        else:
            self.partition_map = {
                pid: peers[pid % len(peers)][2]
                for pid in range(self.num_partitions)
            }

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

            log_path = os.path.join(db_path, "event_log.txt")
            node_logger = EventLogger(log_path)
            self.node_loggers[node_id] = node_logger
            p = multiprocessing.Process(
                target=run_server,
                args=(
                    db_path,
                    "localhost",
                    port,
                    node_id,
                    peers_i,
                    self.ring,
                    self.partition_map,
                    rf,
                    self.write_quorum,
                    self.read_quorum,
                ),
                kwargs={
                    "consistency_mode": self.consistency_mode,
                    "enable_forwarding": self.enable_forwarding,
                    "partition_modulus": self.num_partitions if self.ring is None else None,
                    "node_index": i if self.ring is None else None,
                    "index_fields": self.index_fields,
                    "global_index_fields": self.global_index_fields,
                    "registry_host": self.registry_addr[0] if self.use_registry else None,
                    "registry_port": self.registry_addr[1] if self.use_registry else None,
                    "event_logger": log_path,
                },
                daemon=True,
            )
            p.start()
            time.sleep(0.2)
            client = GRPCReplicaClient("localhost", port)
            node = ClusterNode(node_id, "localhost", port, p, client, node_logger)
            self.nodes.append(node)
            self.nodes_by_id[node_id] = node

        time.sleep(1)
        if use_ring and key_ranges is None:
            self.partitioner.nodes = self.nodes
            self._rebuild_ring_partitions()
        if key_ranges is not None:
            if not use_ring:
                self._setup_partitions(key_ranges)
        if not use_ring:
            if key_ranges is not None:
                self.partitioner = RangePartitioner(key_ranges, self.nodes)
                self.partitions = self.partitioner.partitions
                self.key_ranges = self.partitioner.key_ranges
            else:
                self.partitioner = HashPartitioner(self.num_partitions, self.nodes)
            self.partition_map = self.partitioner.get_partition_map()
            self.update_partition_map()
        else:
            self.partition_map = self.partitioner.get_partition_map()
            self.update_partition_map()

        if start_router:
            raddr = self.registry_addr if self.use_registry else None
            self.router_process = multiprocessing.Process(
                target=run_router,
                args=(self, "localhost", router_port, raddr),
                daemon=True,
            )
            self.router_process.start()
            time.sleep(0.2)
            self.router_client = GRPCRouterClient("localhost", router_port)

        self._cold_stop = threading.Event()
        self._cold_thread = None
        if self.cold_check_interval:
            def _auto_cold():
                while not self._cold_stop.is_set():
                    time.sleep(self.cold_check_interval)
                    try:
                        self.check_cold_partitions()
                    except Exception:
                        pass

            self._cold_thread = threading.Thread(target=_auto_cold, daemon=True)
            self._cold_thread.start()

    def register_driver(self, driver) -> None:
        """Register a Driver instance for partition map updates."""
        if driver not in self.drivers:
            self.drivers.append(driver)

    def next_txid(self) -> str:
        """Return a new sequential transaction id as string."""
        with self._tx_lock:
            self._tx_counter += 1
            return str(self._tx_counter)

    def enable_salt(self, key: str, buckets: int) -> None:
        """Enable random prefixing for ``key`` using ``buckets`` variants."""
        if buckets < 1:
            raise ValueError("buckets must be >= 1")
        self.salted_keys[str(key)] = int(buckets)

    def mark_hot_key(self, key: str, buckets: int, migrate: bool = False) -> None:
        """Start salting ``key`` and optionally migrate existing data."""
        self.enable_salt(key, buckets)
        self.event_logger.log(
            f"Chave {key} marcada como hot com {buckets} buckets."
            f" Migracao {'ativada' if migrate else 'desativada'}."
        )
        if migrate:
            value = self.get(0, key, ignore_salt=True)
            if value is None:
                return
            for i in range(buckets):
                salted = f"{i}#{key}"
                self.put(0, salted, value)

    def check_hot_keys(self, threshold: int, buckets: int) -> None:
        """Mark keys with high access frequency as hot.

        Iterates over ``key_freq`` and calls :py:meth:`mark_hot_key` for any
        key whose counter exceeds ``threshold`` and isn't already salted.
        The frequency counter for a key is reset once salting is enabled.
        """
        for comp_key, count in list(self.key_freq.items()):
            if count >= threshold:
                pk, _ = self._split_key_components(comp_key)
                if pk in self.salted_keys:
                    continue
                self.mark_hot_key(pk, buckets)
                self.key_freq[comp_key] = 0

    def set_max_transfer_rate(self, rate: int | None) -> None:
        """Configure maximum transfer rate in bytes/second."""
        self.max_transfer_rate = rate

    def reset_metrics(self) -> None:
        """Reset partition and key frequency counters."""
        self.partition_ops = [0] * self.num_partitions
        self.key_freq = {}
        self.partition_item_counts = {i: 0 for i in range(self.num_partitions)}
        self._known_keys.clear()

    def get_hot_partitions(self, threshold: float = 2.0) -> list[int]:
        """Return ids of partitions with ops above ``threshold`` times the average."""
        if not self.partition_ops:
            return []
        avg = sum(self.partition_ops) / len(self.partition_ops)
        limit = avg * threshold
        return [i for i, cnt in enumerate(self.partition_ops) if cnt > limit]

    def get_cold_partitions(self, threshold: float = 0.5) -> list[int]:
        """Return ids of partitions with ops below ``threshold`` times the average."""
        if not self.partition_ops:
            return []
        avg = sum(self.partition_ops) / len(self.partition_ops)
        limit = avg * threshold
        return [i for i, cnt in enumerate(self.partition_ops) if cnt < limit]

    def check_hot_partitions(self, threshold: float = 2.0, min_keys: int = 2) -> None:
        """Split partitions with heavy traffic involving many different keys.

        Calls :py:meth:`split_partition` for each hot partition with at least
        ``min_keys`` accessed keys since the last metric reset. After a
        successful split the metrics are cleared via :py:meth:`reset_metrics`.
        """
        candidates = self.get_hot_partitions(threshold)
        for pid in candidates:
            key_count = self.partition_item_counts.get(pid, 0)
            if key_count >= min_keys:
                self.split_partition(pid)
                self.event_logger.log(
                    f"Particao {pid} estava sobrecarregada – {key_count} chaves – e foi dividida automaticamente."
                )
                self.reset_metrics()

    def check_cold_partitions(self, threshold: float = 0.5, max_keys: int = 1) -> None:
        """Merge adjacent cold partitions with few distinct keys accessed."""
        if self.partitioner is None:
            return

        cold = set(self.get_cold_partitions(threshold))
        pid = 0
        while pid < self.num_partitions - 1:
            if pid in cold and pid + 1 in cold:
                left_count = self.partition_item_counts.get(pid, 0)
                right_count = self.partition_item_counts.get(pid + 1, 0)
                if left_count <= max_keys and right_count <= max_keys:
                    self.merge_partitions(pid, pid + 1)
                    self.event_logger.log(
                        f"Particoes {pid} e {pid + 1} estavam frias e foram unidas automaticamente."
                    )
                    self.reset_metrics()
                    cold = set(self.get_cold_partitions(threshold))
                    continue
            pid += 1

    def get_hot_keys(self, top_n: int = 5) -> list[str]:
        """Return most frequently accessed keys."""
        return [k for k, _ in sorted(self.key_freq.items(), key=lambda kv: kv[1], reverse=True)[:top_n]]

    def get_partition_stats(self) -> dict[int, int]:
        """Return a mapping pid -> operation count."""
        return {i: cnt for i, cnt in enumerate(self.partition_ops)}

    def get_partition_item_counts(self) -> dict[int, int]:
        """Return a mapping pid -> item count since last reset."""
        return {i: self.partition_item_counts.get(i, 0) for i in range(self.num_partitions)}

    def get_node_for_key(
        self, partition_key: str, clustering_key: str | None = None
    ) -> ClusterNode:
        """Return the node responsible for ``partition_key`` based on ``key_ranges``."""
        if self.partitioner is not None and hasattr(self.partitioner, "partitions"):
            for (start, end), node in self.partitioner.partitions:
                if start <= partition_key < end:
                    return node
            return self.partitioner.partitions[-1][1]
        if self.key_ranges is None:
            raise ValueError("key_ranges not configured")
        for (start, end), node in self.partitions:
            if start <= partition_key < end:
                return node
        return self.partitions[-1][1]

    def _range_partition_id(self, partition_key: str) -> int:
        """Return index of range partition containing ``partition_key``."""
        if self.partitioner is not None:
            return self.partitioner.get_partition_id(partition_key)
        if self.key_ranges is None:
            raise ValueError("key_ranges not configured")
        for i, (rng, _) in enumerate(self.partitions):
            start, end = rng
            if start <= partition_key < end:
                return i
        return len(self.partitions) - 1

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
        self.partition_ops = [0] * self.num_partitions
        self.partition_item_counts = {i: 0 for i in range(self.num_partitions)}

    def _rebuild_ring_partitions(self) -> None:
        """Recalculate partition metadata from the hash ring."""
        ring = getattr(self.partitioner, "ring", None)
        if ring is None:
            return
        self.num_partitions = len(ring._ring)
        self.partition_ops = [0] * self.num_partitions
        self.partition_item_counts = {i: 0 for i in range(self.num_partitions)}

    def get_partition_map(self) -> dict[int, str]:
        """Return mapping from partition id to owning node id."""
        return dict(self.partition_map)

    def get_partition_ranges(self) -> dict[int, tuple[str, str]]:
        """Return human friendly boundaries for each partition.

        For range partitioning the configured ranges are returned. Hash based
        strategies expose the underlying hash/token intervals in hexadecimal
        notation to aid debugging on the dashboard."""

        if self.key_ranges is not None:
            return {i: (str(start), str(end)) for i, (start, end) in enumerate(self.key_ranges)}

        if isinstance(self.partitioner, ConsistentHashPartitioner):
            ring = self.partitioner.ring._ring
            if not ring:
                return {}
            max_hash = 1 << 160
            ranges = {}
            for i, (token, _) in enumerate(ring):
                prev = ring[i - 1][0] if i > 0 else 0
                ranges[i] = (hex(prev), hex(token))
            # last range wraps around to the first token
            ranges[len(ring) - 1] = (ranges[len(ring) - 1][0], hex(max_hash))
            return ranges

        if self.partitioner is not None:
            n = self.partitioner.num_partitions
        else:
            n = self.num_partitions
        max_hash = 1 << 160
        step = max_hash // n
        ranges = {}
        for pid in range(n):
            start = pid * step
            end = start + step if pid < n - 1 else max_hash
            ranges[pid] = (hex(start), hex(end))
        return ranges

    def update_partition_map(self, manual: bool = False) -> dict[int, str]:
        """Send current partition map to all nodes via RPC and return it.

        If ``manual`` is ``True`` the update originated from an explicit
        rebalance request via the API.
        """
        if manual:
            self.event_logger.log("Rebalanceamento manual disparado via API.")
        for node in self.nodes:
            try:
                node.client.update_partition_map(self.partition_map)
            except Exception:
                pass
        for driver in self.drivers:
            try:
                driver.update_partition_map(self.partition_map)
            except Exception:
                pass
        self.update_hash_ring()
        self._notify_registry()
        return dict(self.partition_map)

    def update_hash_ring(self) -> None:
        """Send current hash ring to all nodes if using consistent hashing."""
        ring = getattr(self.partitioner, "ring", None)
        if ring is None:
            return
        entries = [(str(h), nid) for h, nid in ring._ring]
        for node in self.nodes:
            try:
                node.client.update_hash_ring(entries)
            except Exception:
                pass

    def _notify_registry(self) -> None:
        if not self.use_registry or not self._registry_stub:
            return
        nodes = [
            metadata_pb2.NodeInfo(node_id=n.node_id, host=n.host, port=n.port)
            for n in self.nodes
        ]
        pmap = replication_pb2.PartitionMap(items=self.partition_map)
        state = metadata_pb2.ClusterState(nodes=nodes, partition_map=pmap)
        try:
            self._registry_stub.UpdateClusterState(state)
        except Exception:
            pass

    def get_partition_id(
        self, partition_key: str, clustering_key: str | None = None
    ) -> int:
        """Return partition id for ``partition_key``."""
        if self.partitioner is not None:
            return self.partitioner.get_partition_id(partition_key)
        return hash_key(partition_key) % self.num_partitions

    def get_index_owner(self, field: str, value) -> str:
        """Return node_id responsible for index entry ``field``/``value``."""
        key = f"idx:{field}:{value}"
        pmap = self.partition_map or {}
        pid = self.get_partition_id(key)
        return pmap.get(pid, self.nodes[pid % len(self.nodes)].node_id)

    def secondary_query(self, field: str, value) -> list[str]:
        """Query secondary indexes across all cluster nodes.

        Results from every node are merged and sorted. Due to asynchronous
        replication the returned keys may be stale or contain duplicates until
        anti entropy synchronizes all nodes."""

        results: set[str] = set()

        def _call(node: ClusterNode) -> list[str]:
            try:
                return node.client.list_by_index(field, value)
            except Exception:
                return []

        with futures.ThreadPoolExecutor(max_workers=len(self.nodes)) as ex:
            for keys in ex.map(_call, self.nodes):
                results.update(keys)

        return sorted(results)

    def _pid_for_key(self, partition_key: str, clustering_key: str | None = None) -> int:
        return self.get_partition_id(partition_key, clustering_key)

    def _coordinator(
        self, partition_key: str, clustering_key: str | None = None
    ) -> ClusterNode:
        ring = getattr(self.partitioner, "ring", None)
        if ring is not None:
            node_id = ring.get_preference_list(partition_key, 1)[0]
            return self.nodes_by_id[node_id]
        pid = (
            self.partitioner.get_partition_id(partition_key)
            if self.partitioner is not None
            else hash_key(partition_key) % self.num_partitions
        )
        node_id = self.partition_map.get(pid)
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
        self.key_freq[composed_key] = self.key_freq.get(composed_key, 0) + 1
        node = self._coordinator(partition_key, clustering_key)
        node.put(composed_key, value)
        pid = self._pid_for_key(partition_key, clustering_key)
        if composed_key not in self._known_keys:
            self.partition_item_counts[pid] = self.partition_item_counts.get(pid, 0) + 1
            self._known_keys.add(composed_key)
        if pid >= len(self.partition_ops):
            self.partition_ops.extend([0] * (pid + 1 - len(self.partition_ops)))
        self.partition_ops[pid] += 1

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
        self.key_freq[composed_key] = self.key_freq.get(composed_key, 0) + 1
        node = self._coordinator(partition_key, clustering_key)
        node.delete(composed_key)
        pid = self._pid_for_key(partition_key, clustering_key)
        if composed_key in self._known_keys:
            self.partition_item_counts[pid] = max(
                0, self.partition_item_counts.get(pid, 0) - 1
            )
            self._known_keys.remove(composed_key)
        if pid >= len(self.partition_ops):
            self.partition_ops.extend([0] * (pid + 1 - len(self.partition_ops)))
        self.partition_ops[pid] += 1

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
                    best_val, best_vc, *_ = merged[0]
                    best_ts = best_vc.clock.get("ts", 0)
                    for val, vc, *_ in merged[1:]:
                        cmp = vc.compare(best_vc)
                        ts = vc.clock.get("ts", 0)
                        if cmp == ">" or (cmp is None and ts > best_ts):
                            best_val, best_vc, best_ts = val, vc, ts
                else:
                    best_val = None
                    best_ts = -1
                    best_vc = None
                    for val, vc, *_ in merged:
                        ts = vc.clock.get("ts", 0)
                        if ts > best_ts:
                            best_val, best_vc, best_ts = val, vc, ts
                return best_val
            else:
                return [(val, vc.clock) for val, vc, *_ in merged]

        composed_key = compose_key(partition_key, clustering_key)
        self.key_freq[composed_key] = self.key_freq.get(composed_key, 0) + 1
        ring = getattr(self.partitioner, "ring", None)
        if self.load_balance_reads and ring is not None:
            pref_nodes = ring.get_preference_list(
                partition_key, self.replication_factor
            )
            random.shuffle(pref_nodes)
            recs = None
            node = None
            for nid in pref_nodes:
                n = self.nodes_by_id[nid]
                try:
                    recs = n.client.get(composed_key)
                    node = n
                    break
                except Exception:
                    continue
            if node is None:
                return None
            pid = self._pid_for_key(partition_key, clustering_key)
            if pid >= len(self.partition_ops):
                self.partition_ops.extend([0] * (pid + 1 - len(self.partition_ops)))
            self.partition_ops[pid] += 1
            if merge:
                return recs[0][0] if recs else None
            return [(val, vc_dict) for val, ts, vc_dict in recs]
        if self.partition_strategy == "hash" and ring is None:
            node = self._coordinator(partition_key, clustering_key)
            recs = node.client.get(composed_key)
            pid = self._pid_for_key(partition_key, clustering_key)
            if pid >= len(self.partition_ops):
                self.partition_ops.extend([0] * (pid + 1 - len(self.partition_ops)))
            self.partition_ops[pid] += 1
            if merge:
                return recs[0][0] if recs else None
            return [(val, vc_dict) for val, ts, vc_dict in recs]
        if self.key_ranges is not None:
            node = self.get_node_for_key(partition_key, clustering_key)
            recs = node.client.get(composed_key)
            pid = self._pid_for_key(partition_key, clustering_key)
            if pid >= len(self.partition_ops):
                self.partition_ops.extend([0] * (pid + 1 - len(self.partition_ops)))
            self.partition_ops[pid] += 1
            if merge:
                return recs[0][0] if recs else None
            return [(val, vc_dict) for val, ts, vc_dict in recs]
        if ring is None:
            node = self._coordinator(partition_key, clustering_key)
            recs = node.client.get(composed_key)
            pid = self._pid_for_key(partition_key, clustering_key)
            if pid >= len(self.partition_ops):
                self.partition_ops.extend([0] * (pid + 1 - len(self.partition_ops)))
            self.partition_ops[pid] += 1
            if merge:
                return recs[0][0] if recs else None
            return [(val, vc_dict) for val, ts, vc_dict in recs]
        pref_nodes = ring.get_preference_list(
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
            all_nodes = ring.get_preference_list(partition_key, len(self.nodes))
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
                best_val, best_vc, *_ = merged[0]
                best_ts = best_vc.clock.get("ts", 0)
                for val, vc, *_ in merged[1:]:
                    cmp = vc.compare(best_vc)
                    ts = vc.clock.get("ts", 0)
                    if cmp == ">" or (cmp is None and ts > best_ts):
                        best_val, best_vc, best_ts = val, vc, ts
            else:
                best_val = None
                best_ts = -1
                best_vc = None
                for val, vc, *_ in merged:
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
            return [(val, vc.clock) for val, vc, *_ in merged]

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
        if partition_key in self.salted_keys:
            buckets = self.salted_keys[partition_key]
            merged: dict[str, list[tuple]] = {}
            for i in range(buckets):
                salted_pk = f"{i}#{partition_key}"
                node = self._coordinator(salted_pk, start_ck)
                items = node.client.scan_range(salted_pk, start_ck, end_ck)
                for ck, val, ts, vc_dict in items:
                    vc = VectorClock(vc_dict)
                    merged.setdefault(ck, [])
                    merged[ck] = _merge_version_lists(merged[ck], [(val, vc)])
            result = []
            for ck in sorted(merged):
                versions = [v for v in merged[ck] if v[0] != TOMBSTONE]
                if not versions:
                    continue
                best_val, best_vc, *_ = versions[0]
                best_ts = best_vc.clock.get("ts", 0)
                for val, vc, *_ in versions[1:]:
                    cmp = vc.compare(best_vc)
                    ts = vc.clock.get("ts", 0)
                    if cmp == ">" or (cmp is None and ts > best_ts):
                        best_val, best_vc, best_ts = val, vc, ts
                result.append((ck, best_val))
            return result

        node = self._coordinator(partition_key, start_ck)
        items = node.client.scan_range(partition_key, start_ck, end_ck)
        return [(ck, val) for ck, val, _, _ in items]

    # See Bigtable ("Bigtable: A Distributed Storage System for Structured Data")
    # and HBase region splitting for background on dynamic range splits.
    def split_partition(self, pid: int, split_key: str | None = None) -> None:
        """Split a range partition creating a new one."""
        if self.partition_strategy == "hash" and self.key_ranges is None:
            self.partitioner.split_partition()
            self.num_partitions = self.partitioner.num_partitions
            self.partition_ops.append(0)
            self.update_partition_map()
            self.event_logger.log(
                f"Partição {pid} dividida (hash). Total agora = {self.num_partitions}."
            )
            return
        if self.partitioner is None:
            raise ValueError("range partitions not configured")
        if pid < 0 or pid >= len(self.partitioner.partitions):
            raise IndexError("invalid partition id")
        new_pid, old_node, new_node = self.partitioner.split_partition(pid, split_key)
        self.partitions = self.partitioner.partitions
        self.key_ranges = self.partitioner.key_ranges
        self.num_partitions = self.partitioner.num_partitions
        self.partition_ops = [0] * self.num_partitions
        if new_node is not old_node:
            self.transfer_partition(old_node, new_node, new_pid)
        self.update_partition_map()
        msg = f"Partição {pid} dividida em {pid} e {new_pid}"
        if split_key is not None:
            msg += f" (chave de corte = {split_key})"
        msg += "."
        self.event_logger.log(msg)

    def merge_partitions(self, pid1: int, pid2: int) -> None:
        """Merge two adjacent range partitions."""
        if self.partitioner is None:
            raise ValueError("range partitions not configured")

        if abs(pid1 - pid2) != 1:
            raise ValueError("partitions must be adjacent")

        left_pid = min(pid1, pid2)
        right_pid = left_pid + 1
        if right_pid >= len(self.partitioner.partitions):
            raise IndexError("invalid partition id")

        rng_left, node_left = self.partitioner.partitions[left_pid]
        rng_right, node_right = self.partitioner.partitions[right_pid]

        if rng_left[1] != rng_right[0]:
            raise ValueError("ranges are not contiguous")

        dest_id, src_id = self.partitioner.merge_partitions(left_pid)

        self.partitions = self.partitioner.partitions
        self.key_ranges = self.partitioner.key_ranges
        self.num_partitions = self.partitioner.num_partitions
        self.partition_ops = [0] * self.num_partitions

        if dest_id != src_id:
            src_node = self.nodes_by_id[src_id]
            dest_node = self.nodes_by_id[dest_id]
            self._move_range_partition(rng_right, src_node, dest_node, right_pid)

        self.partition_map = self.partitioner.get_partition_map()
        self.update_partition_map()
        self.event_logger.log(
            f"Partições {left_pid} e {right_pid} mescladas em uma só."
        )

    def _split_key_components(self, key: str) -> tuple[str, str | None]:
        if "|" in key:
            pk, ck = key.split("|", 1)
        else:
            pk, ck = key, None
        return pk, ck

    # Partition migration approach inspired by Dynamo's data transfer during
    # membership changes (see "Dynamo: Amazon's Highly Available Key-value Store").
    def transfer_partition(
        self, src_node: ClusterNode, dst_node: ClusterNode, partition_id: int
    ) -> None:
        """Move all records for ``partition_id`` from ``src_node`` to ``dst_node``."""
        if src_node is dst_node:
            return
        self.partition_map[partition_id] = dst_node.node_id

        items = self._load_node_items(src_node)
        start_ts = time.time()
        bytes_copied = 0

        if self.key_ranges is not None:
            start, end = self.key_ranges[partition_id]
            def belongs(pk: str) -> bool:
                return start <= pk < end
        else:
            def belongs(pk: str, ck: str | None) -> bool:
                if self.partitioner is not None:
                    return self.partitioner.get_partition_id(pk) == partition_id
                return self.get_partition_id(pk, ck) == partition_id

        for key, versions in items.items():
            pk, ck = self._split_key_components(key)
            if self.key_ranges is not None:
                if not belongs(pk):
                    continue
            else:
                if not belongs(pk, ck):
                    continue
            for val, vc, *_ in versions:
                ts = vc.clock.get("ts", 0)
                dst_node.client.put(
                    key, val, timestamp=ts, node_id=dst_node.node_id, vector=vc.clock
                )
                src_node.client.delete(key, timestamp=ts + 1, node_id=src_node.node_id)
                if self.max_transfer_rate:
                    record_size = len(key.encode("utf-8"))
                    if val is not None:
                        record_size += len(str(val).encode("utf-8"))
                    record_size += len(json.dumps(vc.clock).encode("utf-8"))
                    bytes_copied += record_size
                    elapsed = time.time() - start_ts
                    expected = bytes_copied / float(self.max_transfer_rate)
                    if expected > elapsed:
                        time.sleep(expected - elapsed)
        self.event_logger.log(
            f"moved partition {partition_id} from {src_node.node_id} to {dst_node.node_id}"
        )

    def _load_node_items(self, node: ClusterNode) -> dict[str, list[tuple]]:
        path = os.path.join(self.base_path, node.node_id)
        db = SimpleLSMDB(db_path=path)
        merged: dict[str, list[tuple]] = {}
        for k, versions in db.memtable.get_sorted_items():
            merged[k] = _merge_version_lists(merged.get(k, []), versions)
        for _, seg_path, _ in db.sstable_manager.sstable_segments:
            with open(seg_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except Exception:
                        continue
                    key = data.get("key")
                    val = data.get("value")
                    vc = VectorClock(data.get("vector", {}))
                    merged[key] = _merge_version_lists(merged.get(key, []), [(val, vc)])
        db.close()
        return {k: [tpl for tpl in v if tpl[0] != TOMBSTONE] for k, v in merged.items()}

    def list_records(
        self, offset: int = 0, limit: int | None = None
    ) -> list[tuple[str, str | None, object]]:
        """Return records stored across the cluster with optional slicing.

        The method iterates over the known keys (``self._known_keys``). When the
        set is empty it loads the keys from disk for each node using
        :py:meth:`_load_node_items`. For each discovered key the value is fetched
        via :py:meth:`get` and tombstones are ignored. The resulting list is
        ordered by primary and clustering keys.
        """

        key_set: set[str] = set(self._known_keys)
        if not key_set:
            for node in self.nodes:
                try:
                    key_set.update(self._load_node_items(node).keys())
                except Exception:
                    continue

        records: list[tuple[str, str | None, object]] = []
        for key in sorted(key_set):
            pk, ck = self._split_key_components(key)
            value = self.get(0, pk, ck)
            if value is None or value == TOMBSTONE:
                continue
            records.append((pk, ck, value))

        if offset < 0:
            offset = 0
        end = offset + limit if limit is not None else None
        return records[offset:end]

    def _move_hash_partition(self, pid: int, src: ClusterNode, dest: ClusterNode) -> None:
        items = self._load_node_items(src)
        for key, versions in items.items():
            pk, ck = self._split_key_components(key)
            target_pid = (
                self.partitioner.get_partition_id(pk)
                if self.partitioner is not None
                else self.get_partition_id(pk, ck)
            )
            if target_pid != pid:
                continue
            for val, vc, *_ in versions:
                ts = vc.clock.get("ts", 0)
                dest.client.put(key, val, timestamp=ts, node_id=dest.node_id, vector=vc.clock)
                src.client.delete(key, timestamp=ts + 1, node_id=src.node_id)
        print(f"moved partition {pid} from {src.node_id} to {dest.node_id}")

    def _move_range_partition(self, rng: tuple, src: ClusterNode, dest: ClusterNode, pid: int) -> None:
        start, end = rng
        items = self._load_node_items(src)
        for key, versions in items.items():
            pk, ck = self._split_key_components(key)
            if not (start <= pk < end):
                continue
            for val, vc, *_ in versions:
                ts = vc.clock.get("ts", 0)
                dest.client.put(key, val, timestamp=ts, node_id=dest.node_id, vector=vc.clock)
                src.client.delete(key, timestamp=ts + 1, node_id=src.node_id)
        print(f"moved partition {pid} from {src.node_id} to {dest.node_id}")

    def _rebalance_after_add(self, new_node: ClusterNode) -> None:
        """Rebalance partitions after adding ``new_node`` for simple hash partitioning."""
        self.event_logger.log(
            f"Iniciando rebalanceamento após adição de {new_node.node_id}."
        )
        if self.partition_strategy != "hash" or self.ring is not None:
            return
        counts: dict[str, int] = {n.node_id: 0 for n in self.nodes}
        for pid, nid in self.partition_map.items():
            counts[nid] = counts.get(nid, 0) + 1
        num_nodes = len(self.nodes)
        base = self.num_partitions // num_nodes
        rem = self.num_partitions % num_nodes
        targets = {}
        for i, node in enumerate(self.nodes):
            targets[node.node_id] = base + (1 if i < rem else 0)
        need = targets[new_node.node_id] - counts.get(new_node.node_id, 0)
        if need <= 0:
            return
        for node in self.nodes:
            if node.node_id == new_node.node_id:
                continue
            while counts[node.node_id] > targets[node.node_id] and need > 0:
                pid = next(pid for pid, nid in self.partition_map.items() if nid == node.node_id)
                self.transfer_partition(node, new_node, pid)
                self.partition_map[pid] = new_node.node_id
                counts[node.node_id] -= 1
                need -= 1

    def _rebalance_after_remove(self, node_id: str) -> None:
        """Rebalance partitions after removing ``node_id`` for simple hash partitioning."""
        if self.partition_strategy != "hash" or self.ring is not None or not self.nodes:
            return
        removed_node = self.nodes_by_id[node_id]
        counts: dict[str, int] = {n.node_id: 0 for n in self.nodes}
        removed_pids = [pid for pid, nid in self.partition_map.items() if nid == node_id]
        for pid, nid in self.partition_map.items():
            if nid != node_id and nid in counts:
                counts[nid] += 1
        num_nodes = len(self.nodes)
        base = self.num_partitions // num_nodes
        rem = self.num_partitions % num_nodes
        targets = {}
        for i, node in enumerate(self.nodes):
            targets[node.node_id] = base + (1 if i < rem else 0)
        for node in self.nodes:
            want = targets[node.node_id] - counts[node.node_id]
            while want > 0 and removed_pids:
                pid = removed_pids.pop()
                self.transfer_partition(removed_node, node, pid)
                self.partition_map[pid] = node.node_id
                counts[node.node_id] += 1
                want -= 1

    def add_node(self) -> ClusterNode:
        idx = len(self.nodes)
        node_id = f"node_{idx}"
        db_path = os.path.join(self.base_path, node_id)
        port = self.base_port + idx
        peers = [("localhost", self.base_port + i, f"node_{i}") for i in range(len(self.nodes) + 1)]
        if isinstance(self.partitioner, ConsistentHashPartitioner):
            old_ring = list(self.partitioner.ring._ring)
            self.partitioner.ring.add_node(node_id, weight=self.partitions_per_node)
            self.partition_map = self.partitioner.get_partition_map()
        log_path = os.path.join(db_path, "event_log.txt")
        node_logger = EventLogger(log_path)
        self.node_loggers[node_id] = node_logger
        p = multiprocessing.Process(
            target=run_server,
            args=(
                db_path,
                "localhost",
                port,
                node_id,
                peers,
                self.ring,
                self.partition_map,
                self.replication_factor,
                self.write_quorum,
                self.read_quorum,
            ),
            kwargs={
                "consistency_mode": self.consistency_mode,
                "enable_forwarding": self.enable_forwarding,
                "index_fields": self.index_fields,
                "global_index_fields": self.global_index_fields,
                "registry_host": self.registry_addr[0] if self.use_registry else None,
                "registry_port": self.registry_addr[1] if self.use_registry else None,
                "event_logger": log_path,
            },
            daemon=True,
        )
        p.start()
        time.sleep(0.2)
        client = GRPCReplicaClient("localhost", port)
        node = ClusterNode(node_id, "localhost", port, p, client, node_logger)
        self.nodes.append(node)
        self.nodes_by_id[node_id] = node
        self.event_logger.log(f"Node {node_id} adicionado ao cluster.")
        if isinstance(self.partitioner, ConsistentHashPartitioner):
            self.partitioner.nodes = self.nodes
            self._rebuild_ring_partitions()
            new_ring = list(self.partitioner.ring._ring)
            for i, (h, nid) in enumerate(new_ring):
                if (h, nid) not in old_ring:
                    prev_h = new_ring[i - 1][0]
                    max_hash = 1 << 160
                    mid = (prev_h + h) // 2 if prev_h < h else ((prev_h + max_hash + h) // 2) % max_hash
                    old_owner_id = None
                    if old_ring:
                        hashes = [x[0] for x in old_ring]
                        idx = bisect_right(hashes, mid) % len(old_ring)
                        old_owner_id = old_ring[idx][1]
                    if old_owner_id and old_owner_id != nid:
                        pid = self.get_partition_id(str(mid))
                        self.transfer_partition(self.nodes_by_id[old_owner_id], node, pid)

        if self.partition_strategy == "hash" and not isinstance(self.partitioner, ConsistentHashPartitioner):
            self._rebalance_after_add(node)
        elif self.key_ranges is not None:
            new_parts = []
            for i, (rng, nd) in enumerate(self.partitions):
                target = self.nodes[i % len(self.nodes)]
                if nd is not target:
                    self.transfer_partition(nd, target, i)
                new_parts.append((rng, target))
            self.partitions = new_parts
        if self.partitioner is not None and not isinstance(self.partitioner, ConsistentHashPartitioner):
            # ``HashPartitioner`` stores the same node list used by ``NodeCluster``.
            # Avoid duplicating entries when adding a node.
            if self.partitioner.nodes is not self.nodes:
                self.partitioner.add_node(node)
            if hasattr(self.partitioner, "partitions"):
                self.partitions = self.partitioner.partitions
        self.update_partition_map()
        return node

    def remove_node(self, node_id: str) -> None:
        if node_id not in self.nodes_by_id:
            return
        node = self.nodes_by_id[node_id]
        self.nodes = [n for n in self.nodes if n.node_id != node_id]
        if isinstance(self.partitioner, ConsistentHashPartitioner):
            old_ring = list(self.partitioner.ring._ring)
            removed_tokens = [t for t in old_ring if t[1] == node_id]
            self.partitioner.remove_node(node)
            self.partition_map = self.partitioner.get_partition_map()
            self._rebuild_ring_partitions()
            if self.nodes:
                for h, _ in removed_tokens:
                    idx = next(i for i, t in enumerate(old_ring) if t[0] == h and t[1] == node_id)
                    prev_h = old_ring[idx - 1][0]
                    max_hash = 1 << 160
                    mid = (prev_h + h) // 2 if prev_h < h else ((prev_h + max_hash + h) // 2) % max_hash
                    dest_id = self.partitioner.ring.get_preference_list(str(mid), 1)[0]
                    dest_node = self.nodes_by_id[dest_id]
                    pid = self.get_partition_id(str(mid))
                    self.transfer_partition(node, dest_node, pid)

        if self.partition_strategy == "hash" and self.ring is None and self.nodes:
            self._rebalance_after_remove(node_id)
        elif self.key_ranges is not None and self.nodes:
            new_parts = []
            for i, (rng, nd) in enumerate(self.partitions):
                new_target = self.nodes[i % len(self.nodes)]
                if nd is not new_target:
                    self.transfer_partition(nd, new_target, i)
                new_parts.append((rng, new_target))
            self.partitions = new_parts

        if self.partitioner is not None and not isinstance(self.partitioner, ConsistentHashPartitioner):
            self.partitioner.remove_node(node)
            if hasattr(self.partitioner, "partitions"):
                self.partitions = self.partitioner.partitions

        self.nodes_by_id.pop(node_id)
        logger = self.node_loggers.pop(node_id, None)
        if logger:
            logger.close()
        self.event_logger.log(f"Node {node_id} removido do cluster.")
        self.update_partition_map()
        node.stop()

    def stop_node(self, node_id: str) -> None:
        """Stop the process for ``node_id`` without altering membership."""
        node = self.nodes_by_id.get(node_id)
        if not node:
            return
        node.stop()

    def start_node(self, node_id: str) -> None:
        """Restart a previously stopped node using stored configuration."""
        node = self.nodes_by_id.get(node_id)
        if not node:
            return
        if node.process.is_alive():
            return

        db_path = os.path.join(self.base_path, node.node_id)
        peers = [(n.host, n.port, n.node_id) for n in self.nodes]
        log_path = os.path.join(db_path, "event_log.txt")
        logger = self.node_loggers.get(node_id)
        if logger is None:
            logger = EventLogger(log_path)
            self.node_loggers[node_id] = logger

        p = multiprocessing.Process(
            target=run_server,
            args=(
                db_path,
                node.host,
                node.port,
                node.node_id,
                peers,
                self.ring,
                self.partition_map,
                self.replication_factor,
                self.write_quorum,
                self.read_quorum,
            ),
            kwargs={
                "consistency_mode": self.consistency_mode,
                "enable_forwarding": self.enable_forwarding,
                "partition_modulus": self.num_partitions if self.ring is None else None,
                "node_index": self.nodes.index(node) if self.ring is None else None,
                "index_fields": self.index_fields,
                "global_index_fields": self.global_index_fields,
                "registry_host": self.registry_addr[0] if self.use_registry else None,
                "registry_port": self.registry_addr[1] if self.use_registry else None,
                "event_logger": log_path,
            },
            daemon=True,
        )
        p.start()
        time.sleep(0.2)
        node.process = p
        node.client = GRPCReplicaClient(node.host, node.port)
        self.update_partition_map()


    def shutdown(self):
        if self._cold_thread:
            self._cold_stop.set()
            self._cold_thread.join(timeout=1)
        if self.router_process is not None:
            if self.router_process.is_alive():
                self.router_process.terminate()
            self.router_process.join()
            if self.router_client is not None:
                self.router_client.close()
        for n in self.nodes:
            n.stop()
        if self.use_registry and self.registry_process is not None:
            if self.registry_process.is_alive():
                self.registry_process.terminate()
            self.registry_process.join()
            if self._registry_channel:
                self._registry_channel.close()
        for logger in self.node_loggers.values():
            logger.close()
        self.event_logger.close()


import hashlib
import random
from bisect import bisect_right
from abc import ABC, abstractmethod
from hash_ring import HashRing


def hash_key(key: str) -> int:
    """Return a stable integer hash for ``key`` using SHA-1."""
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return int(h, 16)


def compose_key(partition_key: str, clustering_key: str | None = None) -> str:
    """Compose partition and clustering components into a sortable key.

    The representation preserves lexicographic ordering for the pair. When
    no ``clustering_key`` is provided, the function simply returns the
    ``partition_key`` to keep backwards compatibility with existing
    single-part keys.
    """
    if clustering_key is None or clustering_key == "":
        return str(partition_key)
    return f"{partition_key}|{clustering_key}"


class Partitioner(ABC):
    """Abstract base for partitioning strategies."""

    @abstractmethod
    def get_partition_id(self, key: str) -> int:
        """Return partition id for ``key``."""

    @abstractmethod
    def add_node(self, node) -> None:
        """Add a node to the partitioner."""

    @abstractmethod
    def remove_node(self, node) -> None:
        """Remove ``node`` from the partitioner."""


class RangePartitioner(Partitioner):
    """Range partitioning similar to Bigtable or HBase.

    Keeps keys ordered for fast range scans but may create hotspots when some
    ranges receive disproportionate traffic.
    """

    def __init__(self, key_ranges: list, nodes: list):
        self.key_ranges = self._normalize_ranges(key_ranges)
        self.nodes = nodes
        self.partitions = [
            (rng, self.nodes[i % len(self.nodes)])
            for i, rng in enumerate(self.key_ranges)
        ]
        self.num_partitions = len(self.partitions)

    def _normalize_ranges(self, key_ranges: list) -> list[tuple[str, str]]:
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
        return ranges

    def get_partition_id(self, key: str) -> int:
        for i, (rng, _) in enumerate(self.partitions):
            start, end = rng
            if start <= key < end:
                return i
        return len(self.partitions) - 1

    def add_node(self, node) -> None:
        self.nodes.append(node)
        self.partitions = [
            (rng, self.nodes[i % len(self.nodes)])
            for i, (rng, _) in enumerate(self.partitions)
        ]

    def remove_node(self, node) -> None:
        if node in self.nodes:
            self.nodes.remove(node)
            if self.nodes:
                self.partitions = [
                    (rng, self.nodes[i % len(self.nodes)])
                    for i, (rng, _) in enumerate(self.partitions)
                ]

    def get_partition_map(self) -> dict[int, str]:
        return {i: n.node_id for i, (_, n) in enumerate(self.partitions)}

    def split_partition(self, pid: int, split_key: str | None = None):
        (start, end), node = self.partitions[pid]
        if split_key is None:
            if start is None or end is None:
                raise ValueError("split_key required for unbounded range")
            if len(start) == 1 and len(end) == 1:
                split_key = chr((ord(start) + ord(end)) // 2)
            else:
                h1 = hash_key(start)
                h2 = hash_key(end)
                split_key = str((h1 + h2) // 2)
        if not (start < split_key < end):
            raise ValueError("split_key must be within range")
        if len(self.nodes) > len(self.partitions):
            new_node = self.nodes[len(self.partitions)]
        else:
            new_node = self.nodes[(pid + 1) % len(self.nodes)]
        new_parts = []
        for i, (rng, nd) in enumerate(self.partitions):
            if i == pid:
                new_parts.append(((start, split_key), nd))
                new_parts.append(((split_key, end), new_node))
            else:
                new_parts.append((rng, nd))
        self.partitions = new_parts
        self.key_ranges = [rng for rng, _ in new_parts]
        self.num_partitions = len(self.partitions)
        return pid + 1, node, new_node

    def merge_partitions(self, left_pid: int):
        """Merge the range at ``left_pid`` with the next partition."""
        if left_pid < 0 or left_pid >= len(self.partitions) - 1:
            raise IndexError("invalid partition id")

        (start1, end1), node_left = self.partitions[left_pid]
        (start2, end2), node_right = self.partitions[left_pid + 1]

        if end1 != start2:
            raise ValueError("ranges are not contiguous")

        merged_range = (start1, end2)
        new_parts = []
        for i, (rng, nd) in enumerate(self.partitions):
            if i == left_pid:
                new_parts.append((merged_range, node_left))
            elif i == left_pid + 1:
                continue
            else:
                new_parts.append((rng, nd))

        self.partitions = new_parts
        self.key_ranges = [rng for rng, _ in new_parts]
        self.num_partitions = len(self.partitions)

        return node_left.node_id, node_right.node_id


class HashPartitioner(Partitioner):
    """Modulo based partitioning as used in Dynamo or Cassandra.

    Provides an even key distribution but makes ordered range scans
    inefficient since adjacent keys land on different partitions.
    """

    def __init__(self, num_partitions: int, nodes: list):
        self.num_partitions = max(1, int(num_partitions))
        self.nodes = nodes

    def get_partition_id(self, key: str) -> int:
        return hash_key(key) % self.num_partitions

    def add_node(self, node) -> None:
        self.nodes.append(node)

    def remove_node(self, node) -> None:
        if node in self.nodes:
            self.nodes.remove(node)

    def get_partition_map(self) -> dict[int, str]:
        return {
            pid: self.nodes[pid % len(self.nodes)].node_id
            for pid in range(self.num_partitions)
        }

    def split_partition(self):
        self.num_partitions += 1


class ConsistentHashPartitioner(Partitioner):
    """Partitioner backed by :class:`HashRing` with random tokens."""

    def __init__(self, nodes: list | None = None, *, partitions_per_node: int = 1) -> None:
        self.ring = HashRing()
        self.nodes: list = []
        if nodes:
            for node in nodes:
                self.add_node(node, weight=partitions_per_node)

    def get_partition_id(self, key: str) -> int:
        if not self.ring._ring:
            return 0
        key_hash = hash_key(key)
        hashes = [h for h, _ in self.ring._ring]
        idx = bisect_right(hashes, key_hash) % len(hashes)
        return idx

    def add_node(self, node, weight: int = 1) -> None:
        self.nodes.append(node)
        replicas = []
        for _ in range(weight):
            token = random.getrandbits(160)
            replicas.append((token, node.node_id))
            self.ring._ring.append((token, node.node_id))
        self.ring._nodes.setdefault(node.node_id, []).extend(replicas)
        self.ring._ring.sort(key=lambda x: x[0])

    def remove_node(self, node) -> None:
        if node in self.nodes:
            self.nodes.remove(node)
        nid = node.node_id
        if nid in self.ring._nodes:
            replicas = set(self.ring._nodes.pop(nid))
            self.ring._ring = [entry for entry in self.ring._ring if entry not in replicas]
            self.ring._ring.sort(key=lambda x: x[0])

    def get_partition_map(self) -> dict[int, str]:
        return {i: nid for i, (_, nid) in enumerate(self.ring._ring)}



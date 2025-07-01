import hashlib
from bisect import bisect_right

class HashRing:
    """Consistent hashing ring."""

    def __init__(self) -> None:
        self._ring = []  # list of (hash_int, node_id)
        self._nodes = {}

    def _hash(self, value: str, replica: int = 0) -> int:
        h = hashlib.sha1(f"{value}:{replica}".encode("utf-8")).hexdigest()
        return int(h, 16)

    def add_node(self, node_id: str, weight: int = 1) -> None:
        """Add a node with optional weight (virtual nodes)."""
        replicas = []
        for i in range(weight):
            h = self._hash(node_id, i)
            replicas.append((h, node_id))
            self._ring.append((h, node_id))
        self._nodes.setdefault(node_id, []).extend(replicas)
        self._ring.sort(key=lambda x: x[0])

    def remove_node(self, node_id: str) -> None:
        """Remove node and all its replicas from the ring."""
        if node_id not in self._nodes:
            return
        replicas = set(self._nodes.pop(node_id))
        self._ring = [entry for entry in self._ring if entry not in replicas]
        self._ring.sort(key=lambda x: x[0])

    def get_preference_list(self, key: str, n: int) -> list[str]:
        """Return next ``n`` unique nodes responsible for ``key``."""
        if not self._ring or n <= 0:
            return []
        key_hash = self._hash(key)
        hashes = [h for h, _ in self._ring]
        idx = bisect_right(hashes, key_hash) % len(self._ring)
        result = []
        seen = set()
        i = idx
        while len(result) < n and len(seen) < len(self._nodes):
            node_id = self._ring[i][1]
            if node_id not in seen:
                result.append(node_id)
                seen.add(node_id)
            i = (i + 1) % len(self._ring)
        return result

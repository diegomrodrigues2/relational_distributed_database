"""Merkle tree utilities for SimpleLSMDB."""
from __future__ import annotations
import os
import json
import hashlib
from typing import List, Tuple, Dict


def _hash(data: str) -> str:
    return hashlib.sha256(data.encode('utf-8')).hexdigest()


def merkle_root(items: List[Tuple[str, str]]) -> str:
    """Compute Merkle root hash for given key/value pairs."""
    leaves = [_hash(f"{k}:{v}") for k, v in sorted(items)]
    if not leaves:
        return _hash("")
    while len(leaves) > 1:
        if len(leaves) % 2 == 1:
            leaves.append(leaves[-1])
        next_level = []
        for i in range(0, len(leaves), 2):
            next_level.append(_hash(leaves[i] + leaves[i + 1]))
        leaves = next_level
    return leaves[0]


def compute_segment_hashes(db) -> Dict[str, str]:
    """Return merkle root for memtable and each SSTable segment."""
    hashes: Dict[str, str] = {}

    # memtable
    if hasattr(db, "memtable"):
        items = []
        for k, versions in db.memtable.get_sorted_items():
            for val, _vc in versions:
                if val != "__TOMBSTONE__":
                    items.append((k, val))
        hashes["memtable"] = merkle_root(items)

    if hasattr(db, "sstable_manager"):
        for _, path, _ in db.sstable_manager.sstable_segments:
            seg_id = os.path.basename(path)
            seg_items: List[Tuple[str, str]] = []
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                            k = data.get("key")
                            v = data.get("value")
                            if v != "__TOMBSTONE__":
                                seg_items.append((k, v))
                        except json.JSONDecodeError:
                            continue
            except FileNotFoundError:
                continue
            hashes[seg_id] = merkle_root(seg_items)
    return hashes


class MerkleNode:
    """Simple binary Merkle tree node."""

    def __init__(self, key: str | None, hash_val: str, left: "MerkleNode" | None = None, right: "MerkleNode" | None = None):
        self.key = key
        self.hash = hash_val
        self.left = left
        self.right = right

    def to_dict(self) -> Dict:
        data = {"hash": self.hash}
        if self.key is not None:
            data["key"] = self.key
        if self.left:
            data["left"] = self.left.to_dict()
        if self.right:
            data["right"] = self.right.to_dict()
        return data

    @staticmethod
    def from_dict(data: Dict | None) -> "MerkleNode | None":
        if not data:
            return None
        left = MerkleNode.from_dict(data.get("left"))
        right = MerkleNode.from_dict(data.get("right"))
        return MerkleNode(data.get("key"), data.get("hash", ""), left, right)

    @staticmethod
    def from_proto(msg) -> "MerkleNode | None":
        if msg is None:
            return None
        left = MerkleNode.from_proto(msg.left) if msg.HasField("left") else None
        right = MerkleNode.from_proto(msg.right) if msg.HasField("right") else None
        key = msg.key if msg.key else None
        return MerkleNode(key, msg.hash, left, right)

    def to_proto(self):
        from replica import replication_pb2

        def _to_proto(node: "MerkleNode"):
            m = replication_pb2.MerkleNodeMsg()
            if node.key is not None:
                m.key = node.key
            m.hash = node.hash
            if node.left:
                m.left.CopyFrom(_to_proto(node.left))
            if node.right:
                m.right.CopyFrom(_to_proto(node.right))
            return m

        return _to_proto(self)


def build_merkle_tree(items: List[Tuple[str, str]]) -> MerkleNode:
    """Construct a Merkle tree for the given ``items`` sorted by key."""

    def _build(sorted_items: List[Tuple[str, str]]) -> MerkleNode:
        if not sorted_items:
            return MerkleNode(None, _hash(""))
        if len(sorted_items) == 1:
            k, v = sorted_items[0]
            return MerkleNode(k, _hash(f"{k}:{v}"))
        mid = len(sorted_items) // 2
        left = _build(sorted_items[:mid])
        right = _build(sorted_items[mid:])
        node_hash = _hash(left.hash + right.hash)
        return MerkleNode(None, node_hash, left, right)

    return _build(sorted(items))


def diff_trees(local: MerkleNode, remote: MerkleNode | None) -> List[str]:
    """Return list of keys whose hashes differ between the trees."""

    def flatten(node: MerkleNode) -> Dict[str, str]:
        if node.left is None and node.right is None:
            return {node.key: node.hash} if node.key is not None else {}
        data: Dict[str, str] = {}
        if node.left:
            data.update(flatten(node.left))
        if node.right:
            data.update(flatten(node.right))
        return data

    remote_map = flatten(remote) if remote else {}

    diffs: List[str] = []

    def compare(node: MerkleNode) -> None:
        if node.left is None and node.right is None:
            if remote_map.get(node.key) != node.hash:
                diffs.append(node.key)
            return
        if node.left:
            compare(node.left)
        if node.right:
            compare(node.right)

    compare(local)
    return diffs

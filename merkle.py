"""Merkle tree utilities for SimpleLSMDB."""
import os
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
        items = [(k, v[0]) for k, v in db.memtable.get_sorted_items()]
        hashes["memtable"] = merkle_root(items)

    if hasattr(db, "sstable_manager"):
        for _, path, _ in db.sstable_manager.sstable_segments:
            seg_id = os.path.basename(path)
            seg_items: List[Tuple[str, str]] = []
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        if ":" not in line:
                            continue
                        k, v = line.strip().split(":", 1)
                        seg_items.append((k, v))
            except FileNotFoundError:
                continue
            hashes[seg_id] = merkle_root(seg_items)
    return hashes

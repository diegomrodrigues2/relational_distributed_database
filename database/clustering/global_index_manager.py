import os
import threading
from typing import Iterable, Any
from ..lsm.sstable import TOMBSTONE


class GlobalIndexManager:
    """Simple global secondary index stored in memory."""

    def __init__(self, fields: Iterable[str]) -> None:
        self.fields = list(fields)
        self.indexes: dict[str, dict[Any, set[str]]] = {
            f: {} for f in self.fields
        }
        self._lock = threading.Lock()

    def add_entry(self, field: str, value, pk: str) -> None:
        """Add mapping field/value -> pk."""
        if field not in self.fields:
            return
        with self._lock:
            idx = self.indexes.setdefault(field, {})
            idx.setdefault(value, set()).add(pk)

    def remove_entry(self, field: str, value, pk: str) -> None:
        """Remove mapping if present."""
        with self._lock:
            idx = self.indexes.get(field)
            if not idx:
                return
            keys = idx.get(value)
            if not keys:
                return
            keys.discard(pk)
            if not keys:
                idx.pop(value, None)

    def query(self, field: str, value) -> list[str]:
        """Return list of primary keys for field/value."""
        with self._lock:
            return list(self.indexes.get(field, {}).get(value, set()))

    def rebuild(self, db) -> None:
        """Rebuild indexes scanning all segments for ``idx:`` keys."""
        with self._lock:
            self.indexes = {f: {} for f in self.fields}

        def _process(key: str, value: str) -> None:
            if not key.startswith("idx:"):
                return
            parts = key.split(":", 3)
            if len(parts) < 4:
                return
            _, field, val, pk = parts[:4]
            if field in self.fields and value != TOMBSTONE:
                self.add_entry(field, val, pk)

        for key, value, _ in db.get_segment_items("memtable"):
            _process(key, value)

        for _, path, _ in db.sstable_manager.sstable_segments:
            segment_id = os.path.basename(path)
            for key, value, _ in db.get_segment_items(segment_id):
                _process(key, value)


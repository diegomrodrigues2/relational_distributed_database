import json
import os
import threading
import base64
from typing import Iterable, Any
from ..lsm.sstable import TOMBSTONE
from ..sql.serialization import RowSerializer


class IndexManager:
    """In-memory secondary index manager with thread safety."""

    def __init__(self, fields: Iterable[str]) -> None:
        self.fields = list(fields)
        self.indexes: dict[str, dict[Any, set[str]]] = {
            f: {} for f in self.fields
        }
        self._lock = threading.Lock()

    def add_record(self, key: str, value: str) -> None:
        try:
            data = json.loads(value)
        except Exception:
            try:
                data = RowSerializer.loads(base64.b64decode(value))
            except Exception:
                return
        with self._lock:
            for field in self.fields:
                if field in data:
                    idx = self.indexes.setdefault(field, {})
                    idx.setdefault(data[field], set()).add(key)

    def remove_record(self, key: str, value: str) -> None:
        try:
            data = json.loads(value)
        except Exception:
            try:
                data = RowSerializer.loads(base64.b64decode(value))
            except Exception:
                return
        with self._lock:
            for field in self.fields:
                if field in data:
                    val = data[field]
                    idx = self.indexes.get(field)
                    if not idx:
                        continue
                    keys = idx.get(val)
                    if not keys:
                        continue
                    keys.discard(key)
                    if not keys:
                        idx.pop(val, None)

    def query(self, field: str, value) -> list[str]:
        with self._lock:
            return list(self.indexes.get(field, {}).get(value, set()))

    def rebuild(self, db) -> None:
        """Rebuild indexes scanning all DB segments and the memtable."""
        with self._lock:
            self.indexes = {f: {} for f in self.fields}

        # Iterate over memtable items
        for key, value, _ in db.get_segment_items("memtable"):
            if value != TOMBSTONE:
                self.add_record(key, value)

        # Iterate over SSTable segments
        for _, path, _ in db.sstable_manager.sstable_segments:
            segment_id = os.path.basename(path)
            for key, value, _ in db.get_segment_items(segment_id):
                if value != TOMBSTONE:
                    self.add_record(key, value)

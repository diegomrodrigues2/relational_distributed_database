import json
import threading
from typing import Iterable, Any


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

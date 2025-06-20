import json
from typing import Iterable, Dict, Set, Any, List

class IndexManager:
    """Simple in-memory secondary index manager."""

    def __init__(self, fields: Iterable[str] | None = None) -> None:
        self.fields = list(fields) if fields else []
        self.indexes: Dict[str, Dict[Any, Set[str]]] = {f: {} for f in self.fields}

    def add_record(self, key: str, value: str) -> None:
        if not self.fields:
            return
        try:
            data = json.loads(value)
        except Exception:
            return
        for field in self.fields:
            if field in data:
                val = data[field]
                idx = self.indexes.setdefault(field, {})
                idx.setdefault(val, set()).add(key)

    def remove_record(self, key: str, value: str) -> None:
        if not self.fields:
            return
        try:
            data = json.loads(value)
        except Exception:
            return
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

    def query(self, field: str, value: Any) -> List[str]:
        return list(self.indexes.get(field, {}).get(value, set()))

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
        """Initialize manager with list of indexed columns."""
        self.fields = list(fields)
        # Structure: {table: {column: {value: {rids}}}}
        self.indexes: dict[str, dict[str, dict[Any, set[str]]]] = {}
        self._lock = threading.Lock()

    def _parse_row(self, value: str) -> dict | None:
        """Decode ``value`` from JSON or MsgPack, return dict or ``None``."""
        try:
            return json.loads(value)
        except Exception:
            try:
                return RowSerializer.loads(base64.b64decode(value))
            except Exception:
                return None

    def _split_key(self, key: str) -> tuple[str, str]:
        """Return ``(table, rid)`` from storage ``key``."""
        if "||" in key:
            table, rid = key.split("||", 1)
        else:
            table, rid = "", key
        return table, rid

    def add_record(self, key: str, value: str) -> None:
        data = self._parse_row(value)
        if not isinstance(data, dict):
            return
        table, rid = self._split_key(key)
        with self._lock:
            table_idx = self.indexes.setdefault(table, {})
            for field in self.fields:
                if field in data:
                    col_idx = table_idx.setdefault(field, {})
                    col_idx.setdefault(data[field], set()).add(rid)

    def remove_record(self, key: str, value: str) -> None:
        data = self._parse_row(value)
        if not isinstance(data, dict):
            return
        table, rid = self._split_key(key)
        with self._lock:
            table_idx = self.indexes.get(table)
            if not table_idx:
                return
            for field in self.fields:
                if field not in data:
                    continue
                col_idx = table_idx.get(field)
                if not col_idx:
                    continue
                rids = col_idx.get(data[field])
                if not rids:
                    continue
                rids.discard(rid)
                if not rids:
                    col_idx.pop(data[field], None)
            # Clean up empty structures
            for fld in list(table_idx.keys()):
                if not table_idx[fld]:
                    table_idx.pop(fld)
            if not table_idx:
                self.indexes.pop(table, None)

    def query(self, field: str, value, table: str | None = None) -> list[str]:
        """Return full keys matching ``field``/``value``.

        If ``table`` is provided, only that table is searched. Otherwise results
        from all tables are merged.
        """
        with self._lock:
            tables = [table] if table is not None else list(self.indexes.keys())
            result: list[str] = []
            for tbl in tables:
                t_idx = self.indexes.get(tbl, {})
                rids = t_idx.get(field, {}).get(value, set())
                for rid in rids:
                    result.append(f"{tbl}||{rid}" if tbl else rid)
            return result

    def rebuild(self, db) -> None:
        """Rebuild indexes scanning all DB segments and the memtable."""
        with self._lock:
            self.indexes = {}

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

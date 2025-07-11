from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict

from ..utils.vector_clock import VectorClock


@dataclass
class ColumnDefinition:
    """Represents a table column."""

    name: str
    data_type: str
    primary_key: bool = False
    nullable: bool = True
    default: object | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str | dict) -> "ColumnDefinition":
        if isinstance(data, str):
            data = json.loads(data)
        return cls(**data)


@dataclass
class IndexDefinition:
    """Definition of a secondary index."""

    name: str
    columns: list[str]
    unique: bool = False

    def to_json(self) -> str:
        return json.dumps({"name": self.name, "columns": self.columns, "unique": self.unique})

    @classmethod
    def from_json(cls, data: str | dict) -> "IndexDefinition":
        if isinstance(data, str):
            data = json.loads(data)
        return cls(**data)


@dataclass
class TableSchema:
    """Schema for a single table."""

    name: str
    columns: list[ColumnDefinition]
    indexes: list[IndexDefinition] | None = None

    def to_json(self) -> str:
        return json.dumps(
            {
                "name": self.name,
                "columns": [asdict(c) for c in self.columns],
                "indexes": [asdict(i) for i in self.indexes] if self.indexes else [],
            }
        )

    @classmethod
    def from_json(cls, data: str | dict) -> "TableSchema":
        if isinstance(data, str):
            data = json.loads(data)
        cols = [ColumnDefinition(**c) for c in data.get("columns", [])]
        indexes = [IndexDefinition(**i) for i in data.get("indexes", [])]
        return cls(name=data["name"], columns=cols, indexes=indexes)


class CatalogManager:
    """Manages table schemas stored in the database."""

    def __init__(self, node) -> None:
        self.node = node
        self.schemas: dict[str, TableSchema] = {}
        self._load_schemas()

    # internal helpers -------------------------------------------------
    def _iter_schema_keys(self) -> set[str]:
        prefix = "_meta:table:"
        keys: set[str] = set()
        for k, _ in self.node.db.memtable.get_sorted_items():
            if k.startswith(prefix):
                keys.add(k)
        with self.node.db.sstable_manager._segments_lock:
            segments = list(self.node.db.sstable_manager.sstable_segments)
        for _, path, _ in segments:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                        except Exception:
                            continue
                        key = data.get("key")
                        if key and key.startswith(prefix):
                            keys.add(key)
            except FileNotFoundError:
                continue
        return keys

    def _load_schemas(self) -> None:
        for key in self._iter_schema_keys():
            val = self.node.db.get(key)
            if isinstance(val, list):
                val = val[0] if val else None
            if not val:
                continue
            try:
                schema = TableSchema.from_json(val)
            except Exception:
                continue
            name = key.split(":", 2)[2]
            self.schemas[name] = schema

    # public API -------------------------------------------------------
    def get_schema(self, table: str) -> TableSchema | None:
        return self.schemas.get(table)

    def reload_schema(self, name: str) -> None:
        key = f"_meta:table:{name}"
        val = self.node.db.get(key)
        if isinstance(val, list):
            val = val[-1] if val else None
        if not val:
            self.schemas.pop(name, None)
            return
        try:
            schema = TableSchema.from_json(val)
        except Exception:
            return
        self.schemas[name] = schema

    def save_schema(self, schema: TableSchema) -> None:
        key = f"_meta:table:{schema.name}"
        value = schema.to_json()
        ts = int(time.time() * 1000)
        vc = VectorClock({"ts": ts})
        self.node.db.put(key, value, vector_clock=vc)
        op_id = self.node.next_op_id()
        self.node.replication_log[op_id] = (key, value, ts)
        self.node.save_replication_log()
        self.node.replicate("PUT", key, value, ts, op_id=op_id, vector=vc.clock)
        self.schemas[schema.name] = schema

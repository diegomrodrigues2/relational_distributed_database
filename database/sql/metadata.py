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



    def validate_row(self, row: dict) -> None:
        """Validate ``row`` against this table schema.

        Raises ``ValueError`` if the row does not conform to the schema.
        """
        if not isinstance(row, dict):
            raise ValueError("Row must be a dictionary")

        type_map = {
            "int": int,
            "integer": int,
            "str": str,
            "string": str,
            "float": float,
            "double": float,
            "bool": bool,
            "boolean": bool,
        }

        cols_by_name = {c.name: c for c in self.columns}

        # check for missing required columns
        for col in self.columns:
            required = col.primary_key or (not col.nullable and col.default is None)
            if required:
                if col.name not in row or row[col.name] is None:
                    raise ValueError(f"Missing value for column '{col.name}'")

        # check for extra columns
        for name in row.keys():
            if name not in cols_by_name:
                raise ValueError(f"Unknown column '{name}'")

        # check types
        for name, value in row.items():
            col_def = cols_by_name[name]
            if value is None:
                if col_def.primary_key or (not col_def.nullable and col_def.default is None):
                    raise ValueError(f"Column '{name}' cannot be null")
                continue
            expected = type_map.get(col_def.data_type.lower())
            if expected is None:
                continue
            if expected is float:
                if not isinstance(value, (float, int)):
                    raise ValueError(
                        f"Column '{name}' expects float but got {type(value).__name__}"
                    )
            else:
                if not isinstance(value, expected):
                    raise ValueError(
                        f"Column '{name}' expects {expected.__name__} but got {type(value).__name__}"
                    )


@dataclass
class TableStats:
    table_name: str
    num_rows: int

    def to_json(self) -> str:
        return json.dumps({"table_name": self.table_name, "num_rows": self.num_rows})

    @classmethod
    def from_json(cls, data: str | dict) -> "TableStats":
        if isinstance(data, str):
            data = json.loads(data)
        return cls(table_name=data["table_name"], num_rows=int(data["num_rows"]))


@dataclass
class ColumnStats:
    table_name: str
    col_name: str
    num_distinct: int

    def to_json(self) -> str:
        return json.dumps(
            {
                "table_name": self.table_name,
                "col_name": self.col_name,
                "num_distinct": self.num_distinct,
            }
        )

    @classmethod
    def from_json(cls, data: str | dict) -> "ColumnStats":
        if isinstance(data, str):
            data = json.loads(data)
        return cls(
            table_name=data["table_name"],
            col_name=data["col_name"],
            num_distinct=int(data["num_distinct"]),
        )


class CatalogManager:
    """Manages table schemas stored in the database."""

    def __init__(self, node) -> None:
        self.node = node
        self.schemas: dict[str, TableSchema] = {}
        self.table_stats: dict[str, TableStats] = {}
        self.column_stats: dict[tuple[str, str], ColumnStats] = {}
        self._load_schemas()
        self._load_stats()

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

    def _iter_table_stats_keys(self) -> set[str]:
        prefix = "_meta:tblstats:"
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

    def _iter_col_stats_keys(self) -> set[str]:
        prefix = "_meta:colstats:"
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

    def _load_stats(self) -> None:
        for key in self._iter_table_stats_keys():
            val = self.node.db.get(key)
            if isinstance(val, list):
                val = val[0] if val else None
            if not val:
                continue
            try:
                stats = TableStats.from_json(val)
            except Exception:
                continue
            name = key.split(":", 2)[2]
            self.table_stats[name] = stats

        for key in self._iter_col_stats_keys():
            val = self.node.db.get(key)
            if isinstance(val, list):
                val = val[0] if val else None
            if not val:
                continue
            try:
                stats = ColumnStats.from_json(val)
            except Exception:
                continue
            parts = key.split(":", 3)
            if len(parts) >= 4:
                self.column_stats[(parts[2], parts[3])] = stats

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

    def add_column_to_table(self, table_name: str, column_def: ColumnDefinition) -> None:
        """Add ``column_def`` to ``table_name`` and persist the updated schema."""
        schema = self.schemas.get(table_name)
        if schema is None:
            raise KeyError("Unknown table")
        if any(c.name == column_def.name for c in schema.columns):
            raise ValueError("Column already exists")
        schema.columns.append(column_def)
        self.save_schema(schema)

    def save_table_stats(self, stats: TableStats) -> None:
        key = f"_meta:tblstats:{stats.table_name}"
        value = stats.to_json()
        ts = int(time.time() * 1000)
        vc = VectorClock({"ts": ts})
        self.node.db.put(key, value, vector_clock=vc)
        op_id = self.node.next_op_id()
        self.node.replication_log[op_id] = (key, value, ts)
        self.node.save_replication_log()
        self.node.replicate("PUT", key, value, ts, op_id=op_id, vector=vc.clock)
        self.table_stats[stats.table_name] = stats

    def save_column_stats(self, stats: ColumnStats) -> None:
        key = f"_meta:colstats:{stats.table_name}:{stats.col_name}"
        value = stats.to_json()
        ts = int(time.time() * 1000)
        vc = VectorClock({"ts": ts})
        self.node.db.put(key, value, vector_clock=vc)
        op_id = self.node.next_op_id()
        self.node.replication_log[op_id] = (key, value, ts)
        self.node.save_replication_log()
        self.node.replicate("PUT", key, value, ts, op_id=op_id, vector=vc.clock)
        self.column_stats[(stats.table_name, stats.col_name)] = stats

    def get_table_stats(self, table: str) -> TableStats | None:
        return self.table_stats.get(table)

    def get_column_stats(self, table: str, column: str) -> ColumnStats | None:
        return self.column_stats.get((table, column))

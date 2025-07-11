from __future__ import annotations

from .ast import SelectQuery, BinOp, Column, Literal
from .execution import SeqScanNode, IndexScanNode
from .metadata import CatalogManager
from ..clustering.index_manager import IndexManager
from ..lsm.lsm_db import SimpleLSMDB


class QueryPlanner:
    """Simple rule-based query planner."""

    def __init__(self, db: SimpleLSMDB, catalog: CatalogManager, index_manager: IndexManager) -> None:
        self.db = db
        self.catalog = catalog
        self.index_manager = index_manager

    def create_plan(self, query: SelectQuery):
        """Return a plan node for ``query`` based on simple rules."""
        table = query.from_clause.table
        where = query.where_clause

        indexed_cols: set[str] = set()
        schema = self.catalog.get_schema(table)
        if schema and schema.indexes:
            for idx in schema.indexes:
                indexed_cols.update(idx.columns)

        if (
            isinstance(where, BinOp)
            and where.op == "EQ"
            and isinstance(where.left, Column)
            and isinstance(where.right, Literal)
            and where.left.name in indexed_cols
        ):
            return IndexScanNode(
                self.db,
                self.index_manager,
                table,
                where.left.name,
                where.right.value,
            )

        return SeqScanNode(self.db, table, where_clause=where)

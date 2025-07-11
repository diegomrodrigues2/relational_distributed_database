from __future__ import annotations

from typing import List, Optional, Tuple

from .ast import SelectQuery, Expression, BinOp, Column, Literal
from .execution import SeqScanNode, IndexScanNode
from .metadata import CatalogManager


class QueryPlanner:
    """Very small rule-based query planner."""

    def __init__(self, db, catalog: CatalogManager, index_manager) -> None:
        self.db = db
        self.catalog = catalog
        self.index_manager = index_manager

    # internal helpers -------------------------------------------------
    def _get_index_columns(self, table: str) -> set[str]:
        schema = self.catalog.get_schema(table)
        cols: set[str] = set()
        if schema and schema.indexes:
            for idx in schema.indexes:
                cols.update(idx.columns)
        return cols

    def _find_eq_predicate(self, expr: Optional[Expression]) -> Optional[Tuple[str, Expression]]:
        if expr is None:
            return None
        if isinstance(expr, BinOp):
            if expr.op == "EQ":
                if isinstance(expr.left, Column):
                    return expr.left.name, expr.right
                if isinstance(expr.right, Column):
                    return expr.right.name, expr.left
            left = self._find_eq_predicate(expr.left)
            if left:
                return left
            return self._find_eq_predicate(expr.right)
        return None

    # public API -------------------------------------------------------
    def create_plan(self, query: SelectQuery):
        table = query.from_clause.table
        indexed_columns = self._get_index_columns(table)
        eq_pred = self._find_eq_predicate(query.where_clause)

        if eq_pred and eq_pred[0] in indexed_columns:
            column, value_expr = eq_pred
            lookup_value = None
            if isinstance(value_expr, Literal):
                lookup_value = value_expr.value
            return IndexScanNode(
                self.db,
                self.index_manager,
                table,
                column,
                lookup_value,
            )
        return SeqScanNode(self.db, table, where_clause=query.where_clause)

from __future__ import annotations

from typing import List, Optional, Tuple

from .ast import (
    SelectQuery,
    InsertQuery,
    UpdateQuery,
    DeleteQuery,
    AnalyzeQuery,
    Expression,
    BinOp,
    Column,
    Literal,
)
from .execution import (
    SeqScanNode,
    IndexScanNode,
    NestedLoopJoinNode,
    InsertPlanNode,
    DeletePlanNode,
    UpdatePlanNode,
    AnalyzePlanNode,
)
from .metadata import CatalogManager


class QueryPlanner:
    """Very small rule-based query planner."""

    def __init__(self, db, catalog: CatalogManager, index_manager, service=None) -> None:
        self.db = db
        self.catalog = catalog
        self.index_manager = index_manager
        self.service = service

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

    def _plan_table(self, table: str, where_clause: Expression | None):
        indexed_columns = self._get_index_columns(table)
        eq_pred = self._find_eq_predicate(where_clause)

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
                catalog=self.catalog,
            )
        return SeqScanNode(
            self.db,
            table,
            where_clause=where_clause,
            catalog=self.catalog,
        )

    # public API -------------------------------------------------------
    def create_plan(self, query):
        if isinstance(query, SelectQuery):
            if query.join_clause:
                outer_plan = self._plan_table(query.from_clause.table, query.where_clause)

                def _inner_plan():
                    return self._plan_table(query.join_clause.table, None)

                pred = query.join_clause.on
                if not isinstance(pred, BinOp) or pred.op != "EQ":
                    raise ValueError("Only EQ join supported")
                if not isinstance(pred.left, Column) or not isinstance(pred.right, Column):
                    raise ValueError("Join predicate must compare columns")

                outer_alias = query.from_clause.alias or query.from_clause.table
                if pred.left.table == outer_alias:
                    outer_key = pred.left.name
                    inner_key = pred.right.name
                elif pred.right.table == outer_alias:
                    outer_key = pred.right.name
                    inner_key = pred.left.name
                else:
                    outer_key = pred.left.name
                    inner_key = pred.right.name

                return NestedLoopJoinNode(outer_plan, _inner_plan, outer_key, inner_key)

            return self._plan_table(query.from_clause.table, query.where_clause)

        if isinstance(query, InsertQuery):
            if not self.service:
                raise ValueError("service required for INSERT")
            return InsertPlanNode(self.service, self.catalog, query.table, query.columns, query.values)

        if isinstance(query, DeleteQuery):
            if not self.service:
                raise ValueError("service required for DELETE")
            return DeletePlanNode(self.service, self, query.table, query.where_clause)

        if isinstance(query, UpdateQuery):
            if not self.service:
                raise ValueError("service required for UPDATE")
            return UpdatePlanNode(self.service, self, query.table, query.assignments, query.where_clause)

        if isinstance(query, AnalyzeQuery):
            return AnalyzePlanNode(self.db, self.catalog, query.table)

        raise ValueError("Unsupported query type")

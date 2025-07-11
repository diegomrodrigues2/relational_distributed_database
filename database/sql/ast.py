from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union, List


@dataclass
class Column:
    """Reference to a column."""

    name: str
    table: Optional[str] = None


@dataclass
class Literal:
    """Literal value in a query."""

    value: object


@dataclass
class BinOp:
    """Binary operation (e.g. comparisons, logical ops)."""

    left: "Expression"
    op: str
    right: "Expression"


Expression = Union[Column, Literal, BinOp]


@dataclass
class SelectItem:
    expression: Expression
    alias: Optional[str] = None


@dataclass
class FromClause:
    table: str
    alias: Optional[str] = None


@dataclass
class SelectQuery:
    select_items: List[SelectItem]
    from_clause: FromClause
    where_clause: Optional[Expression] = None

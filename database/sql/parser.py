import re
from .metadata import ColumnDefinition, TableSchema


def parse_create_table(sql_string: str) -> TableSchema:
    """Parse a very small subset of ``CREATE TABLE`` statements.

    Supported syntax::
        CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
    """
    ddl = sql_string.strip().rstrip(";")
    m = re.match(r"CREATE\s+TABLE\s+(\w+)\s*\((.*)\)\s*$", ddl, re.IGNORECASE)
    if not m:
        raise ValueError("Invalid CREATE TABLE syntax")
    name = m.group(1)
    cols_part = m.group(2).strip()
    if not cols_part:
        raise ValueError("No columns defined")
    columns = []
    for col_def in cols_part.split(','):
        col_def = col_def.strip()
        if not col_def:
            continue
        parts = col_def.split()
        if len(parts) < 2:
            raise ValueError("Invalid column definition")
        col_name = parts[0]
        col_type = parts[1]
        rest = " ".join(parts[2:]).upper()
        pk = "PRIMARY KEY" in rest
        columns.append(ColumnDefinition(col_name, col_type.lower(), primary_key=pk))
    return TableSchema(name=name, columns=columns)

import sqlglot
from sqlglot import expressions as exp

from .ast import (
    Column,
    Literal,
    BinOp,
    SelectItem,
    FromClause,
    SelectQuery,
    Expression,
    JoinClause,
)


def _map_literal(lit: exp.Literal) -> Literal:
    value: object
    if lit.is_string:
        value = lit.this
    else:
        # try to parse as int or float
        try:
            value = int(lit.this)
        except ValueError:
            try:
                value = float(lit.this)
            except ValueError:
                value = lit.this
    return Literal(value)


def _map_expression(node: exp.Expression) -> Expression:
    if isinstance(node, exp.Column):
        return Column(name=node.name, table=node.table)
    if isinstance(node, exp.Literal):
        return _map_literal(node)
    if isinstance(node, (exp.And, exp.Or, exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        left = _map_expression(node.args['this'])
        right = _map_expression(node.args['expression'])
        op = node.__class__.__name__.upper()
        return BinOp(left=left, op=op, right=right)
    raise ValueError(f"Unsupported expression: {type(node).__name__}")


def parse_sql(sql_string: str) -> SelectQuery:
    """Parse a simple SELECT statement into the internal AST."""
    try:
        parsed = sqlglot.parse_one(sql_string)
    except Exception as e:  # pragma: no cover - ensure any parsing error is surfaced
        raise ValueError("Invalid SQL") from e

    if not isinstance(parsed, exp.Select):
        raise ValueError("Only SELECT statements are supported")

    # SELECT items
    select_items: list[SelectItem] = []
    for item in parsed.expressions:
        alias: str | None = None
        expr = item
        if isinstance(item, exp.Alias):
            alias = item.alias
            expr = item.this
        select_items.append(SelectItem(expression=_map_expression(expr), alias=alias))

    from_exp = parsed.args.get("from")
    if not from_exp or not isinstance(from_exp.this, exp.Table):
        raise ValueError("FROM clause required")

    table_expr: exp.Table = from_exp.this
    table_name = table_expr.name
    alias = None
    alias_exp = table_expr.args.get("alias")
    if alias_exp is not None and isinstance(alias_exp.this, exp.Identifier):
        alias = alias_exp.this.this

    from_clause = FromClause(table=table_name, alias=alias)

    join_clause = None
    joins = parsed.args.get("joins")
    if joins:
        join_exp = joins[0]
        if isinstance(join_exp, exp.Join) and isinstance(join_exp.this, exp.Table):
            jtable = join_exp.this.name
            jalias = None
            alias_exp = join_exp.this.args.get("alias")
            if alias_exp is not None and isinstance(alias_exp.this, exp.Identifier):
                jalias = alias_exp.this.this
            on_expr = join_exp.args.get("on")
            on_mapped = _map_expression(on_expr) if on_expr is not None else None
            join_clause = JoinClause(table=jtable, alias=jalias, on=on_mapped)
        else:
            raise ValueError("Unsupported JOIN clause")

    where_clause = None
    if parsed.args.get("where") is not None:
        where_clause = _map_expression(parsed.args["where"].this)

    return SelectQuery(
        select_items=select_items,
        from_clause=from_clause,
        join_clause=join_clause,
        where_clause=where_clause,
    )

import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from database.sql.parser import parse_sql
from database.sql.ast import Column, Literal, BinOp, SelectQuery


def test_parse_simple_select():
    q = parse_sql("SELECT id FROM users")
    assert isinstance(q, SelectQuery)
    assert len(q.select_items) == 1
    col = q.select_items[0].expression
    assert isinstance(col, Column)
    assert col.name == "id"
    assert q.from_clause.table == "users"
    assert q.where_clause is None


def test_parse_where_clause():
    q = parse_sql("SELECT id FROM users WHERE city = 'NY'")
    assert isinstance(q.where_clause, BinOp)
    assert q.where_clause.op == "EQ"
    left = q.where_clause.left
    right = q.where_clause.right
    assert isinstance(left, Column)
    assert left.name == "city"
    assert isinstance(right, Literal)
    assert right.value == "NY"


def test_parse_multiple_predicates():
    q = parse_sql("SELECT id FROM users WHERE city = 'NY' AND status = 1")
    assert isinstance(q.where_clause, BinOp)
    assert q.where_clause.op == "AND"
    left = q.where_clause.left
    right = q.where_clause.right
    assert isinstance(left, BinOp) and left.op == "EQ"
    assert isinstance(right, BinOp) and right.op == "EQ"


def test_parse_invalid_syntax():
    with pytest.raises(ValueError):
        parse_sql("SELECT FROM WHERE")

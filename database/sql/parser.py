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

# -*- coding: utf-8 -*-
"""
Created on Thu Apr  1 16:27:08 2021

@author: nicol
"""

QUERY = "SELECT {field} from {table}"
COND = " WHERE {table}.length=?"
LIMIT = " LIMIT {num}"
OFFSET = " OFFSET {num}"
TABLES = "SELECT name FROM sqlite_master WHERE type='table'"
FIELDS = "PRAGMA table_info({table})"
DROP = "DROP TABLE {table}"
CREATE = "CREATE TABLE {table}({field})"
ALTER = "ALTER TABLE {table} ADD {field};"
INSERT = "INSERT INTO {table}({fields}) VALUES({values});"
UPDATE = "UPDATE {table} SET {fields} WHERE rowid = ?;"
AGGREGATE = '''CREATE TABLE {new_t}
            AS SELECT {fields}, {sum_}
            FROM {old_t}'''
SUM = "SUM({field}) AS {field}"
DELETE = "DELETE FROM {table} WHERE freq=?"
ROW_COUNT = """SELECT COUNT(*) FROM {table} WHERE length=?"""
MAX_LEN = "SELECT MAX(length) FROM {table}"


def format_query(table: str, fields: list, cond=True, limit=None, offset=None):
    field = ', '.join(fields)
    query = QUERY.format(table=table, field=field)
    if cond:
        query += COND.format(table=table)
    if limit:
        query += LIMIT.format(num=limit)
    if offset:
        query += OFFSET.format(num=offset)
    return query

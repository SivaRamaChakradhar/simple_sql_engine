# parser.py
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

@dataclass
class WhereClause:
    column: str
    operator: str
    value: Union[str, float, int]

@dataclass
class Query:
    select: List[str]               # list of columns or ['*'] or ['COUNT(*)'] or ['COUNT(col)']
    from_table: str                 # table name, mapped to csv filename without .csv
    where: Optional[WhereClause]

class SQLParseError(Exception):
    pass

# Simple regex-based parser for limited grammar:
# SELECT <cols> FROM <table> [WHERE <col> <op> <value>];
SELECT_FROM_WHERE_RE = re.compile(
    r'^\s*SELECT\s+(?P<select>.+?)\s+FROM\s+(?P<from>\S+)(?:\s+WHERE\s+(?P<where>.+?))?\s*;?\s*$',
    re.IGNORECASE
)

WHERE_CONDITION_RE = re.compile(
    r'^\s*(?P<col>\w+)\s*(?P<op>=|!=|<>|>=|<=|>|<)\s*(?P<val>.+)\s*$',
    re.IGNORECASE
)

def parse_value(raw: str):
    raw = raw.strip()
    # if quoted string (single quotes)
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        return raw[1:-1]
    # try int
    try:
        if '.' in raw:
            f = float(raw)
            return f
        i = int(raw)
        return i
    except ValueError:
        # as fallback treat as string (unquoted)
        return raw

def normalize_count_token(token: str):
    # normalize forms like COUNT(*), count(col)
    t = token.strip()
    m = re.match(r'(?i)^COUNT\s*\(\s*(\*|[A-Za-z_]\w*)\s*\)$', t)
    if m:
        inside = m.group(1)
        return f'COUNT({inside})'
    return None

def parse(sql: str) -> Query:
    m = SELECT_FROM_WHERE_RE.match(sql)
    if not m:
        raise SQLParseError("Invalid SQL. Make sure it matches: SELECT ... FROM <table> [WHERE ...];")

    raw_select = m.group('select')
    raw_from = m.group('from').rstrip(';')
    raw_where = m.group('where')

    # parse select list
    # split by comma outside parentheses
    tokens = [t.strip() for t in re.split(r',(?![^()]*\))', raw_select)]
    select = []
    for t in tokens:
        # support COUNT(...) token
        t_up = normalize_count_token(t)
        if t_up:
            select.append(t_up)
        elif t == '*':
            select.append('*')
        else:
            # simple column name verify
            if not re.match(r'^[A-Za-z_]\w*$', t):
                raise SQLParseError(f"Invalid column token in SELECT: '{t}'")
            select.append(t)

    # parse from: allow table or table.csv (we'll strip extension)
    from_table = raw_from.strip().rstrip(';')
    if from_table.lower().endswith('.csv'):
        from_table = from_table[:-4]
    if not re.match(r'^[A-Za-z0-9_\-]+$', from_table):
        raise SQLParseError(f"Invalid table name: '{raw_from}'")

    # parse where if present
    where_clause = None
    if raw_where:
        wm = WHERE_CONDITION_RE.match(raw_where)
        if not wm:
            raise SQLParseError("Invalid WHERE clause. Expected format: column op value (e.g. age > 30 or country = 'USA').")
        col = wm.group('col')
        op = wm.group('op')
        val_raw = wm.group('val').strip()
        val = parse_value(val_raw)
        # normalize operator: accept <> as !=
        if op == '<>':
            op = '!='
        where_clause = WhereClause(column=col, operator=op, value=val)

    return Query(select=select, from_table=from_table, where=where_clause)

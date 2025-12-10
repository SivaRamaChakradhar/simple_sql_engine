# engine.py
import csv
import os
from typing import List, Dict, Any, Optional
from parser import Query, WhereClause, SQLParseError

class ExecutionError(Exception):
    pass

def load_table(table_name: str, data_dir: str = "sample_data") -> List[Dict[str, Any]]:
    path = os.path.join(data_dir, f"{table_name}.csv")
    if not os.path.exists(path):
        raise ExecutionError(f"Table/file not found: {path}")
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        # Basic type inference for numbers
        for r in reader:
            newr = {}
            for k, v in r.items():
                # strip whitespace
                if v is None:
                    newr[k] = None
                    continue
                s = v.strip()
                if s == '':
                    newr[k] = None
                    continue
                # try int/float conversion
                try:
                    if '.' in s:
                        newr[k] = float(s)
                    else:
                        newr[k] = int(s)
                except ValueError:
                    newr[k] = s
            rows.append(newr)
    return rows

def apply_where(rows: List[Dict[str, Any]], where: Optional[WhereClause]) -> List[Dict[str, Any]]:
    if where is None:
        return rows
    col = where.column
    op = where.operator
    val = where.value

    if not rows:
        return []

    if col not in rows[0]:
        # try case-insensitive match
        lowered = {c.lower(): c for c in rows[0].keys()}
        if col.lower() in lowered:
            col = lowered[col.lower()]
        else:
            raise ExecutionError(f"Column '{where.column}' not found in table.")

    def cmp(a):
        # a may be None
        if a is None:
            return False
        # try numeric comparison when both numeric
        if isinstance(a, (int, float)) and isinstance(val, (int, float)):
            if op == '=':
                return a == val
            if op == '!=':
                return a != val
            if op == '>':
                return a > val
            if op == '<':
                return a < val
            if op == '>=':
                return a >= val
            if op == '<=':
                return a <= val
        else:
            # compare as strings (case-sensitive)
            aval = str(a)
            vval = str(val)
            if op == '=':
                return aval == vval
            if op == '!=':
                return aval != vval
            if op == '>':
                return aval > vval
            if op == '<':
                return aval < vval
            if op == '>=':
                return aval >= vval
            if op == '<=':
                return aval <= vval
        raise ExecutionError(f"Unsupported comparison between {type(a)} and {type(val)}")

    filtered = [r for r in rows if cmp(r.get(col))]
    return filtered

def evaluate_count(rows: List[Dict[str, Any]], token: str) -> int:
    # token is like COUNT(*) or COUNT(col)
    m_all = token.upper() == 'COUNT(*)'
    if m_all:
        return len(rows)
    m = None
    import re
    mm = re.match(r'COUNT\(\s*([A-Za-z_]\w*)\s*\)', token, re.IGNORECASE)
    if mm:
        col = mm.group(1)
        count = 0
        for r in rows:
            v = r.get(col)
            if v is not None and v != '':
                count += 1
        return count
    raise ExecutionError(f"Invalid COUNT token: {token}")

def project(rows: List[Dict[str, Any]], select: List[str]):
    # if select contains a COUNT(...) and is single token -> return aggregation result
    if len(select) == 1 and select[0].upper().startswith("COUNT"):
        c = evaluate_count(rows, select[0])
        return [{"COUNT": c}]

    # project columns
    if select == ['*']:
        return rows

    result = []
    for r in rows:
        newr = {}
        for col in select:
            if col not in r:
                # try case-insensitive match
                lower_map = {k.lower(): k for k in r.keys()}
                if col.lower() in lower_map:
                    actual = lower_map[col.lower()]
                    newr[col] = r.get(actual)
                else:
                    raise ExecutionError(f"Column '{col}' not found in table.")
            else:
                newr[col] = r.get(col)
        result.append(newr)
    return result

def execute(query: Query, data_dir: str = "sample_data"):
    # Load data
    rows = load_table(query.from_table, data_dir=data_dir)

    # Apply WHERE (filter)
    rows = apply_where(rows, query.where)

    # If any aggregation COUNT tokens in select, handle as aggregation
    # For simplicity we support single-aggregate SELECT or normal projection
    # (i.e., either SELECT COUNT(...) or SELECT col1, col2)
    # Mixed queries like SELECT COUNT(*), col are NOT supported in this minimal engine.
    if any(token.upper().startswith("COUNT(") for token in query.select):
        # only allow single COUNT token
        if len(query.select) != 1:
            raise ExecutionError("Only single COUNT(...) aggregate is supported (e.g., SELECT COUNT(*) FROM table WHERE ...).")
        rows = project(rows, query.select)
        return rows

    # Otherwise projection
    rows = project(rows, query.select)
    return rows

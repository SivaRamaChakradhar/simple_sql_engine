# Simple In-Memory SQL Engine (Python)

## Overview
A minimal SQL engine that loads CSV files into memory and supports a small subset of SQL:
- `SELECT` projection (columns or `*`)
- `FROM` (CSV file; just use table name or `table.csv`)
- single-condition `WHERE` with one of `=, !=, >, <, >=, <=`
- aggregation: `COUNT(*)` and `COUNT(col)`

This project is educational — it demonstrates how a database performs selection, filtering and simple aggregation on small datasets.

## Files
- `parser.py` — SQL parser (limited grammar)
- `engine.py` — data loader and query execution
- `cli.py` — simple REPL CLI
- `sample_data/people.csv` and `orders.csv` — example datasets

## Supported SQL grammar (precise)
- `SELECT <cols> FROM <table> [WHERE <col> <op> <value>];`
- `<cols>` is `*`, a comma-separated list of column names (`name, country`) or a single aggregate `COUNT(*)` or `COUNT(column)`.
- `<table>` is the CSV file name without `.csv` or with `.csv` (matching a file in the `sample_data` directory).
- `<op>` one of: `=`, `!=`, `<>` (treated as `!=`), `>`, `<`, `>=`, `<=`.
- `<value>` number (e.g., `30`) or string quoted with single or double quotes (e.g., `'USA'`).

Examples:
- `SELECT * FROM people;`
- `SELECT name, country FROM people WHERE age > 30;`
- `SELECT COUNT(*) FROM people WHERE country = 'USA';`
- `SELECT COUNT(age) FROM people;` — counts non-null ages.

Limitations:
- Only a single `WHERE` condition (no AND/OR, no parentheses).
- Only one aggregate allowed and only `COUNT`.
- No JOINs, GROUP BY, ORDER BY, or LIMIT.
- Mixed queries like `SELECT COUNT(*), country` are not supported.

## How to run
1. Ensure Python 3.8+ installed.
2. Clone repo or copy files.
3. Run the REPL:
```bash
python cli.py

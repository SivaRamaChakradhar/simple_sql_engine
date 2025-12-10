# cli.py
from parser import parse, SQLParseError
from engine import execute, ExecutionError
import traceback

PROMPT = "simple-sql> "

def print_rows(rows):
    if not rows:
        print("(no rows)")
        return
    # compute columns from first row
    cols = list(rows[0].keys())
    # simple table printing
    widths = {c: max(len(str(c)), max((len(str(r.get(c, ''))) for r in rows), default=0)) for c in cols}
    # header
    header = " | ".join(c.ljust(widths[c]) for c in cols)
    sep = "-+-".join('-' * widths[c] for c in cols)
    print(header)
    print(sep)
    for r in rows:
        print(" | ".join(str(r.get(c, '')).ljust(widths[c]) for c in cols))

def repl(data_dir="sample_data"):
    print("Simple in-memory SQL engine REPL. Type 'exit' or 'quit' to stop.")
    print("Supported SQL: SELECT <cols|*> FROM <table.csv|table> [WHERE col op value];")
    while True:
        try:
            s = input(PROMPT)
        except (EOFError, KeyboardInterrupt):
            print("\nbye")
            break
        if not s:
            continue
        if s.strip().lower() in ("exit", "quit"):
            print("bye")
            break
        try:
            query = parse(s)
            rows = execute(query, data_dir=data_dir)
            print_rows(rows)
        except SQLParseError as e:
            print(f"SQL parse error: {e}")
        except ExecutionError as e:
            print(f"Execution error: {e}")
        except Exception as e:
            print("Unexpected error:")
            traceback.print_exc()

if __name__ == "__main__":
    repl()

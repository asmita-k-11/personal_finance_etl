"""
run_analysis.py
===============
Phase 4 - Personal Finance ETL Pipeline
-----------------------------------------
Reads analysis.sql, splits it into individual queries, runs each one
against finance.db, and prints beautifully formatted results.

Usage
-----
    python run_analysis.py
"""

import re
import sys
import sqlite3
from pathlib import Path

import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH  = Path("finance.db")
SQL_PATH = Path("analysis.sql")

pd.set_option("display.max_columns",  20)
pd.set_option("display.width",        120)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def split_queries(sql_text: str) -> list[tuple[str, str]]:
    """
    Extract labelled queries from the SQL file.
    Each query block starts with a comment block whose first line contains
    'QUERY N: <title>' and ends at the next QUERY block or EOF.

    Returns a list of (title, sql_body) tuples.
    """
    # Find all query header positions
    pattern = re.compile(r"--\s*(QUERY \d+:[^\n]+)", re.MULTILINE)
    matches = list(pattern.finditer(sql_text))

    queries = []
    for i, match in enumerate(matches):
        title = match.group(1).strip()
        start = match.start()
        end   = matches[i + 1].start() if i + 1 < len(matches) else len(sql_text)
        block = sql_text[start:end]

        # Strip comment lines to get executable SQL
        sql_lines = [
            line for line in block.splitlines()
            if not line.strip().startswith("--") and line.strip()
        ]
        body = "\n".join(sql_lines).strip()

        if body:
            queries.append((title, body))

    return queries


def print_header(title: str, query_num: int) -> None:
    width = 70
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def run_query(conn: sqlite3.Connection, title: str, sql: str, num: int) -> None:
    """Execute one query and print results as a formatted table."""
    print_header(title, num)
    try:
        df = pd.read_sql_query(sql, conn)
        if df.empty:
            print("  (No rows returned)")
        else:
            print(df.to_string(index=False))
            print(f"\n  [{len(df)} row(s) returned]")
    except Exception as e:
        print(f"  ERROR: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at '{DB_PATH}'.")
        print("       Run load_to_db.py first.")
        sys.exit(1)

    if not SQL_PATH.exists():
        print(f"ERROR: SQL file not found at '{SQL_PATH}'.")
        sys.exit(1)

    print("=" * 70)
    print("  Personal Finance ETL - Phase 4: SQL Analytics")
    print("=" * 70)

    sql_text = SQL_PATH.read_text(encoding="utf-8")
    queries  = split_queries(sql_text)

    if not queries:
        print("No QUERY blocks found in analysis.sql.")
        sys.exit(1)

    print(f"\n  Found {len(queries)} analytical queries. Running...\n")

    conn = sqlite3.connect(DB_PATH)

    for i, (title, sql) in enumerate(queries, start=1):
        run_query(conn, title, sql, i)

    conn.close()

    print("\n" + "=" * 70)
    print("  Phase 4 complete. All queries executed successfully.")
    print("  Project complete - all 4 phases done!")
    print("=" * 70)


if __name__ == "__main__":
    main()

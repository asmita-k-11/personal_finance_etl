"""
load_to_db.py
=============
Phase 3 - Personal Finance ETL Pipeline
-----------------------------------------
Reads the cleaned CSV produced by pipeline.py and loads it into a
SQLite database using SQLAlchemy.

Steps
-----
  1. Apply schema.sql  →  create tables, indexes, and pre-seed lookups
  2. Read cleaned CSV  →  pandas DataFrame
  3. Resolve FKs       →  map category/account names → integer IDs
  4. Upsert            →  INSERT OR REPLACE so re-runs are safe (idempotent)
  5. Verify            →  run a quick SQL sanity-check and print row counts

Database
--------
  finance.db  (SQLite file in project root — excluded from Git via .gitignore)

Portability note
----------------
  SQLite is used for zero-setup local development.
  To switch to PostgreSQL, update the ENGINE_URL and change
  "INSERT OR REPLACE" to "INSERT ... ON CONFLICT (transaction_id) DO UPDATE SET ..."
  (or use pandas df.to_sql with method='multi' + a custom upsert).
"""

import sys
import logging
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Paths & config ────────────────────────────────────────────────────────────
DB_PATH       = Path("finance.db")
SCHEMA_PATH   = Path("schema.sql")
CLEANED_CSV   = Path("data") / "processed" / "cleaned_transactions.csv"

# SQLAlchemy connection URL for SQLite
ENGINE_URL = f"sqlite:///{DB_PATH}"


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 — Apply Schema
# ══════════════════════════════════════════════════════════════════════════════

def apply_schema(engine: sa.Engine) -> None:
    """
    Execute schema.sql against the database.
    Uses CREATE TABLE IF NOT EXISTS and INSERT OR IGNORE so it is safe
    to call on an existing database (idempotent).
    """
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_PATH}")

    ddl = SCHEMA_PATH.read_text(encoding="utf-8")

    # Strip comment lines before splitting on ';'
    # This prevents comment text (e.g. "-- FKs to ...") being sent as SQL.
    clean_lines = [
        line for line in ddl.splitlines()
        if not line.strip().startswith("--")
    ]
    clean_ddl = "\n".join(clean_lines)

    statements = [s.strip() for s in clean_ddl.split(";") if s.strip()]

    with engine.begin() as conn:
        # Enable FK enforcement for this session (SQLite-specific)
        conn.execute(text("PRAGMA foreign_keys = ON"))
        for stmt in statements:
            if stmt.upper().startswith("PRAGMA"):
                continue                    # already applied above
            conn.execute(text(stmt))

    log.info(f"  Schema applied from {SCHEMA_PATH}")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — Read Cleaned CSV
# ══════════════════════════════════════════════════════════════════════════════

def read_cleaned_csv() -> pd.DataFrame:
    """Load the cleaned CSV. Raise if pipeline.py hasn't been run yet."""
    if not CLEANED_CSV.exists():
        raise FileNotFoundError(
            f"Cleaned CSV not found: {CLEANED_CSV}\n"
            "Run pipeline.py first."
        )

    df = pd.read_csv(CLEANED_CSV, dtype=str)
    df["amount"] = df["amount"].astype(float)

    # Parse date as proper date type
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    log.info(f"  Loaded {len(df)} rows from {CLEANED_CSV}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 3 — Resolve Foreign Keys
# ══════════════════════════════════════════════════════════════════════════════

def fetch_lookup(engine: sa.Engine, table: str, id_col: str, name_col: str) -> dict:
    """
    Return a {name: id} dict from a lookup table.
    Used to map text category/account names → integer FK IDs.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT {id_col}, {name_col} FROM {table}")
        ).fetchall()
    return {name: pk for pk, name in rows}


def resolve_fks(df: pd.DataFrame, engine: sa.Engine) -> pd.DataFrame:
    """
    Replace string category/account columns with integer FK IDs.
    Raises ValueError if any unmapped value is found.
    """
    df = df.copy()

    category_map = fetch_lookup(engine, "categories", "category_id", "name")
    account_map  = fetch_lookup(engine, "accounts",   "account_id",  "name")

    # Validate — every value in the data must exist in the lookup
    unknown_cats = set(df["category"]) - set(category_map)
    unknown_accs = set(df["account"])  - set(account_map)
    if unknown_cats:
        raise ValueError(f"Unmapped categories found: {unknown_cats}")
    if unknown_accs:
        raise ValueError(f"Unmapped accounts found:   {unknown_accs}")

    df["category_id"] = df["category"].map(category_map)
    df["account_id"]  = df["account"].map(account_map)

    log.info(f"  FK mapping done. Categories: {category_map}")
    log.info(f"  FK mapping done. Accounts:   {account_map}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 4 — Upsert into Database
# ══════════════════════════════════════════════════════════════════════════════

# Columns to insert into the transactions table (matches schema.sql exactly)
DB_COLUMNS = [
    "transaction_id",
    "date",
    "description",
    "amount",
    "transaction_type",
    "category_id",
    "location",
    "account_id",
    "source_month",
    "date_raw",
    "amount_raw",
    "source_file",
]


def upsert_transactions(df: pd.DataFrame, engine: sa.Engine) -> int:
    """
    Upsert cleaned transactions into the database.

    SQLite strategy: INSERT OR REPLACE INTO transactions (...)
      - If transaction_id already exists  → replaces the row (safe re-run)
      - If transaction_id is new          → inserts fresh row

    Returns the number of rows upserted.
    """
    df_db = df[DB_COLUMNS].copy()

    # Build the INSERT OR REPLACE statement
    col_names    = ", ".join(DB_COLUMNS)
    placeholders = ", ".join([f":{c}" for c in DB_COLUMNS])
    stmt = text(
        f"INSERT OR REPLACE INTO transactions ({col_names}) "
        f"VALUES ({placeholders})"
    )

    records = df_db.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))
        conn.execute(stmt, records)

    log.info(f"  Upserted {len(records)} rows into 'transactions'.")
    return len(records)


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 5 — Verify
# ══════════════════════════════════════════════════════════════════════════════

def verify(engine: sa.Engine) -> None:
    """
    Run quick SQL sanity checks and print a summary directly from the DB.
    This proves the data landed correctly and the schema / FKs are working.
    """
    checks = {
        "Total transactions": "SELECT COUNT(*) FROM transactions",
        "Total categories":   "SELECT COUNT(*) FROM categories",
        "Total accounts":     "SELECT COUNT(*) FROM accounts",
        "Debits":             "SELECT COUNT(*) FROM transactions WHERE transaction_type = 'debit'",
        "Credits":            "SELECT COUNT(*) FROM transactions WHERE transaction_type = 'credit'",
        "Total spend ($)":    "SELECT ROUND(ABS(SUM(amount)), 2) FROM transactions WHERE transaction_type = 'debit'",
        "Total income ($)":   "SELECT ROUND(SUM(amount), 2) FROM transactions WHERE transaction_type = 'credit'",
    }

    print("\n  --- DB Verification ---")
    with engine.connect() as conn:
        for label, sql in checks.items():
            result = conn.execute(text(sql)).scalar()
            print(f"  {label:<22}: {result}")

    # Category breakdown from DB (joins across tables)
    breakdown_sql = """
        SELECT
            c.name          AS category,
            COUNT(*)        AS txns,
            ROUND(ABS(SUM(t.amount)), 2) AS total_spent
        FROM transactions t
        JOIN categories   c ON t.category_id = c.category_id
        GROUP BY c.name
        ORDER BY total_spent DESC
    """
    print("\n  --- Category Breakdown (from DB JOIN) ---")
    with engine.connect() as conn:
        rows = conn.execute(text(breakdown_sql)).fetchall()
    print(f"  {'Category':<18} {'Txns':>5}  {'Total':>10}")
    print(f"  {'-'*18} {'-'*5}  {'-'*10}")
    for cat, count, total in rows:
        print(f"  {cat:<18} {count:>5}  ${total:>10,.2f}")

    # Month-over-month spend (preview for Phase 4)
    mom_sql = """
        SELECT source_month, ROUND(ABS(SUM(amount)), 2) AS total_spend
        FROM transactions
        WHERE transaction_type = 'debit'
        GROUP BY source_month
        ORDER BY MIN(date)
    """
    print("\n  --- Monthly Spend (from DB) ---")
    with engine.connect() as conn:
        rows = conn.execute(text(mom_sql)).fetchall()
    for month, spend in rows:
        print(f"  {month:<20}  ${spend:,.2f}")


# ══════════════════════════════════════════════════════════════════════════════
#  ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def run_load() -> None:
    print("=" * 60)
    print("  Personal Finance ETL - Phase 3: Load to Database")
    print("=" * 60)

    # Create engine (creates finance.db if it doesn't exist)
    engine = sa.create_engine(ENGINE_URL, echo=False)
    log.info(f"  Connected to: {ENGINE_URL}")

    # 1 — Schema
    print("\n[SCHEMA]")
    apply_schema(engine)

    # 2 — Read CSV
    print("\n[READ]")
    df = read_cleaned_csv()

    # 3 — Resolve FKs
    print("\n[RESOLVE FKs]")
    df = resolve_fks(df, engine)

    # 4 — Upsert
    print("\n[UPSERT]")
    n = upsert_transactions(df, engine)

    # 5 — Verify
    print("\n[VERIFY]")
    verify(engine)

    print("\n" + "=" * 60)
    print(f"  {n} rows loaded into finance.db")
    print("  Phase 3 complete. Ready for Phase 4 -> analysis.sql")
    print("=" * 60)


if __name__ == "__main__":
    run_load()

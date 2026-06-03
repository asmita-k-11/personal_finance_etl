"""
pipeline.py
===========
Phase 2 - Personal Finance ETL Pipeline
-----------------------------------------
Implements a full Extract → Transform → Categorise pipeline using Pandas.

EXTRACT
  Reads all monthly CSV files from  data/raw/

TRANSFORM
  1. Standardise dates  →  all formats parsed to YYYY-MM-DD
  2. Parse currency     →  messy strings like "$1,250.50" / "($45.00)" → float
  3. Clean text         →  strip whitespace, normalise casing on descriptions
  4. Handle nulls       →  sensible defaults for missing description / location
  5. Deduplication      →  remove duplicate transaction_id rows (keep first)
  6. Derive columns     →  transaction_type (debit / credit), source_month

CATEGORISE
  Keyword-matching function maps each description to a business category:
    Groceries | Dining | Transport | Subscriptions | Shopping
    Utilities | Income | Uncategorised

OUTPUT
  Writes cleaned DataFrame to  data/processed/cleaned_transactions.csv
  Prints a detailed run-log and summary statistics.
"""

import os
import re
import sys
import logging
from pathlib import Path

import numpy as np
import pandas as pd

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── Directory paths ───────────────────────────────────────────────────────────
RAW_DIR       = Path("data") / "raw"
PROCESSED_DIR = Path("data") / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE   = PROCESSED_DIR / "cleaned_transactions.csv"


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 — EXTRACT
# ══════════════════════════════════════════════════════════════════════════════

def extract(raw_dir: Path) -> pd.DataFrame:
    """
    Read every *.csv file in raw_dir and concatenate into one DataFrame.
    Also derives a 'source_month' column from the filename.
    """
    csv_files = sorted(raw_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in '{raw_dir}'. "
            "Run generate_mock_data.py first."
        )

    frames = []
    for path in csv_files:
        log.info(f"  Extracting: {path.name}")
        df = pd.read_csv(path, dtype=str)           # read everything as str to preserve formatting
        df["source_file"] = path.name               # track origin file
        # derive a clean month label from the filename (e.g. "january_2026" → "January 2026")
        stem = path.stem.replace("_", " ").title()  # "January 2026"
        df["source_month"] = stem
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    log.info(f"  Total rows extracted: {len(combined)}")
    return combined


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════

# ── 2a. Date standardisation ──────────────────────────────────────────────────

DATE_FORMATS = [
    "%Y-%m-%d",         # 2026-01-15        (ISO)
    "%d/%m/%Y",         # 15/01/2026        (UK slash)
    "%B %d, %Y",        # January 15, 2026  (long-form, zero-padded day)
    "%B %-d, %Y",       # January 5, 2026   (Linux: non-padded)
    "%B %#d, %Y",       # January 5, 2026   (Windows: non-padded)
]


def parse_date(raw: str) -> pd.Timestamp | None:
    """
    Try each known date format in turn. Returns a Timestamp or NaT.
    Using pandas 'dayfirst' inference as a fallback for ambiguous cases.
    """
    if pd.isna(raw) or not str(raw).strip():
        return pd.NaT

    raw = str(raw).strip()

    # Try all explicit formats first
    for fmt in DATE_FORMATS:
        try:
            return pd.to_datetime(raw, format=fmt)
        except (ValueError, TypeError):
            continue

    # Fallback: let pandas infer (day-first for slash-separated)
    try:
        return pd.to_datetime(raw, dayfirst=True)
    except Exception:
        log.warning(f"    Could not parse date: '{raw}' — setting to NaT")
        return pd.NaT


def transform_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse raw date strings and emit a clean 'date' column (YYYY-MM-DD)."""
    df = df.copy()
    df["date_raw"] = df["date"]                         # preserve original
    df["date"]     = df["date_raw"].apply(parse_date)   # parse to Timestamp
    unparseable    = df["date"].isna().sum()
    if unparseable:
        log.warning(f"    {unparseable} date(s) could not be parsed → NaT")
    log.info(f"  Dates standardised. Sample: {df['date'].dropna().head(3).dt.strftime('%Y-%m-%d').tolist()}")
    return df


# ── 2b. Currency → float ──────────────────────────────────────────────────────

# Matches optional leading '-', optional '$', digits/commas, optional decimals
_CURRENCY_RE = re.compile(r"[^\d.\-]")     # strip everything except digits, dot, minus


def parse_amount(raw: str) -> float | None:
    """
    Convert messy currency strings to signed float.

    Handles:
      $1,250.50   →  1250.50
      -$45.00     → -45.00
      ($45.00)    → -45.00      ← bracket = negative (accounting notation)
      -45.00      → -45.00
      45.00       →  45.00
      1,250.50    →  1250.50
    """
    if pd.isna(raw) or not str(raw).strip():
        return np.nan

    raw = str(raw).strip()

    # Bracket notation means negative: ($45.00) → -45.00
    is_bracket_negative = raw.startswith("(") and raw.endswith(")")
    if is_bracket_negative:
        raw = "-" + raw[1:-1]

    # Remove all non-numeric characters except '-' and '.'
    cleaned = _CURRENCY_RE.sub("", raw)

    try:
        return float(cleaned)
    except ValueError:
        log.warning(f"    Could not parse amount: '{raw}' → NaN")
        return np.nan


def transform_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """Parse 'amount' strings to float and add 'transaction_type' column."""
    df = df.copy()
    df["amount_raw"] = df["amount"]
    df["amount"]     = df["amount_raw"].apply(parse_amount)

    # Derive transaction_type from sign of amount
    df["transaction_type"] = np.where(df["amount"] >= 0, "credit", "debit")

    log.info(f"  Amounts parsed.  Credits: {(df['transaction_type'] == 'credit').sum()}, "
             f"Debits: {(df['transaction_type'] == 'debit').sum()}")
    return df


# ── 2c. Text cleaning ─────────────────────────────────────────────────────────

def clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise 'description' and 'location':
      - Strip leading/trailing whitespace
      - Collapse multiple internal spaces to one
      - Title-case descriptions for consistency
    """
    df = df.copy()

    def normalise(val: str) -> str:
        if pd.isna(val):
            return np.nan
        return " ".join(str(val).strip().split()).title()

    df["description"] = df["description"].apply(normalise)
    df["location"]    = df["location"].apply(normalise)

    log.info("  Text normalised (whitespace stripped, title-cased).")
    return df


# ── 2d. Null handling ─────────────────────────────────────────────────────────

def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing values with sensible defaults:
      description  → "Unknown"
      location     → "Online / Unknown"
      amount       → 0.0  (flag edge case rather than drop)
    """
    df = df.copy()

    desc_nulls = df["description"].isna().sum()
    loc_nulls  = df["location"].isna().sum()

    df["description"] = df["description"].fillna("Unknown")
    df["location"]    = df["location"].fillna("Online / Unknown")
    df["amount"]      = df["amount"].fillna(0.0)

    log.info(f"  Nulls filled — description: {desc_nulls}, location: {loc_nulls}")
    return df


# ── 2e. Deduplication ─────────────────────────────────────────────────────────

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with duplicate transaction_id.
    Keeps the first occurrence; logs how many were dropped.
    """
    before = len(df)
    df = df.drop_duplicates(subset=["transaction_id"], keep="first").copy()
    dropped = before - len(df)
    log.info(f"  Deduplication: removed {dropped} duplicate transaction_id(s).")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 3 — CATEGORISE
# ══════════════════════════════════════════════════════════════════════════════

# Keyword mapping: order matters — first match wins.
# Each entry: (category_name, [list_of_keywords_lowercase])
CATEGORY_RULES: list[tuple[str, list[str]]] = [
    ("Income",        ["payroll", "salary", "tax refund", "zelle transfer received",
                       "interest payment", "interest"]),
    ("Groceries",     ["walmart", "target", "whole foods", "kroger", "safeway",
                       "trader joe", "aldi", "publix"]),
    ("Dining",        ["starbucks", "mcdonald", "chipotle", "dominos", "pizza",
                       "cheesecake factory", "restaurant", "cafe", "diner",
                       "subway", "taco bell", "chick-fil"]),
    ("Transport",     ["shell", "exxon", "uber", "lyft", "gas station",
                       "fuel", "bp oil", "chevron", "transport",
                       "shell oil", "shell gas"]),
    ("Subscriptions", ["netflix", "spotify", "hulu", "amazon prime",
                       "adobe", "creative cloud", "subscription",
                       "disney", "youtube premium", "apple music"]),
    ("Shopping",      ["amazon", "ebay", "best buy", "walmart.com",
                       "target.com", "etsy", "shein", "mktplace"]),
    ("Utilities",     ["electric", "internet", "phone bill", "at&t",
                       "spectrum", "oncor", "water bill", "gas bill",
                       "utility", "comcast", "verizon"]),
]


def categorise(description: str) -> str:
    """
    Match a cleaned description against CATEGORY_RULES.
    Returns the first matching category, or 'Uncategorised'.

    The function is case-insensitive and checks if any keyword
    appears as a substring of the description.
    """
    if pd.isna(description) or description.strip().lower() == "unknown":
        return "Uncategorised"

    desc_lower = description.lower()

    for category, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw in desc_lower:
                return category

    return "Uncategorised"


def apply_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the categorise() function and report category distribution."""
    df = df.copy()
    df["category"] = df["description"].apply(categorise)

    dist = df["category"].value_counts().to_dict()
    log.info(f"  Categories assigned: {dist}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 4 — LOAD (to CSV, ready for Phase 3 DB load)
# ══════════════════════════════════════════════════════════════════════════════

# Final column order for the cleaned output
OUTPUT_COLUMNS = [
    "transaction_id",
    "date",
    "description",
    "amount",
    "transaction_type",
    "category",
    "location",
    "account",
    "source_month",
    # Audit / debug columns (raw originals — useful during development)
    "date_raw",
    "amount_raw",
    "source_file",
]


def load(df: pd.DataFrame, output_path: Path) -> None:
    """Write cleaned DataFrame to CSV. Date formatted as YYYY-MM-DD string."""
    df_out = df[OUTPUT_COLUMNS].copy()
    df_out["date"] = pd.to_datetime(df_out["date"]).dt.strftime("%Y-%m-%d")
    df_out.to_csv(output_path, index=False)
    log.info(f"  Cleaned data saved to: {output_path}  ({len(df_out)} rows)")


# ══════════════════════════════════════════════════════════════════════════════
#  PIPELINE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline() -> pd.DataFrame:
    """
    Orchestrates all ETL stages in order and returns the cleaned DataFrame.
    """
    print("=" * 60)
    print("  Personal Finance ETL - Phase 2: Pipeline")
    print("=" * 60)

    # ── EXTRACT ───────────────────────────────────────────────────────────────
    print("\n[EXTRACT]")
    df = extract(RAW_DIR)

    # ── TRANSFORM ─────────────────────────────────────────────────────────────
    print("\n[TRANSFORM]")
    df = transform_dates(df)
    df = transform_amounts(df)
    df = clean_text(df)
    df = handle_nulls(df)
    df = deduplicate(df)

    # ── CATEGORISE ────────────────────────────────────────────────────────────
    print("\n[CATEGORISE]")
    df = apply_categories(df)

    # ── LOAD ──────────────────────────────────────────────────────────────────
    print("\n[LOAD]")
    load(df, OUTPUT_FILE)

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  PIPELINE SUMMARY")
    print("=" * 60)
    print(f"  Total clean transactions : {len(df)}")
    print(f"  Date range               : {df['date'].min().date()} to {df['date'].max().date()}")
    print(f"  Total debits             : {(df['transaction_type'] == 'debit').sum()}")
    print(f"  Total credits            : {(df['transaction_type'] == 'credit').sum()}")
    print(f"  Total spend (debits)     : ${abs(df[df['transaction_type'] == 'debit']['amount'].sum()):,.2f}")
    print(f"  Total income (credits)   : ${df[df['transaction_type'] == 'credit']['amount'].sum():,.2f}")
    print()
    print("  Category Breakdown:")
    cat_summary = df.groupby("category")["amount"].agg(
        count="count",
        total=lambda x: x.abs().sum()
    ).sort_values("total", ascending=False)
    for cat, row in cat_summary.iterrows():
        print(f"    {cat:<18} {int(row['count']):>3} txns   ${row['total']:>10,.2f}")

    print()
    print("  Month-wise Row Counts:")
    for month, count in df["source_month"].value_counts().sort_index().items():
        print(f"    {month:<20} {count} rows")

    print()
    print("  Sample Cleaned Rows (first 5):")
    preview_cols = ["transaction_id", "date", "description", "amount",
                    "transaction_type", "category", "location"]
    print(df[preview_cols].head(5).to_string(index=False))
    print("\n" + "=" * 60)
    print("  Phase 2 complete. Output: data/processed/cleaned_transactions.csv")
    print("  Ready for Phase 3 -> Database schema & loading")
    print("=" * 60)

    return df


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_pipeline()

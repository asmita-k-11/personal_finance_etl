"""
generate_mock_data.py
=====================
Phase 1 - Personal Finance ETL Pipeline
----------------------------------------
Generates three "messy" monthly bank statement CSV files (January, February,
March 2026) that intentionally contain real-world data quality issues:

  • Inconsistent date formats  (YYYY-MM-DD  /  DD/MM/YYYY  /  Month D, YYYY)
  • Currency amounts as strings (e.g.  "$1,250.50"  /  "-$45.00")
  • Missing / NaN values in 'description' and 'location' columns
  • Mixed-case vendor names and trailing/leading whitespace in descriptions
  • Duplicate transaction IDs (one per month) to test deduplication logic
  • Transactions that contain keywords used for downstream categorisation:
        Walmart, Target, Shell, Starbucks, Netflix, Amazon, Uber, etc.

Output
------
  data/raw/january_2026.csv
  data/raw/february_2026.csv
  data/raw/march_2026.csv
"""

import os
import random
import numpy as np
import pandas as pd

# ── Reproducibility ──────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# ── Output directory ─────────────────────────────────────────────────────────
RAW_DIR = os.path.join("data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)


# ════════════════════════════════════════════════════════════════════════════
#  Helper utilities
# ════════════════════════════════════════════════════════════════════════════

def fmt_currency(amount: float) -> str:
    """Format a float as a messy currency string the way bank exports do."""
    style = random.choice(["dollar_sign", "negative_bracket", "plain_negative"])
    if amount >= 0:
        # Credits can come back as plain positive strings too
        if random.random() < 0.3:
            return f"{amount:,.2f}"          # e.g. 1250.50  (no symbol)
        return f"${amount:,.2f}"             # e.g. $1,250.50
    else:
        abs_amt = abs(amount)
        if style == "dollar_sign":
            return f"-${abs_amt:,.2f}"       # e.g. -$45.00
        elif style == "negative_bracket":
            return f"(${abs_amt:,.2f})"      # e.g. ($45.00)
        else:
            return f"-{abs_amt:,.2f}"        # e.g. -45.00


def fmt_date(date: pd.Timestamp, style: int) -> str:
    """Return the same date in one of three inconsistent string formats."""
    if style == 0:
        return date.strftime("%Y-%m-%d")          # ISO: 2026-01-15
    elif style == 1:
        return date.strftime("%d/%m/%Y")          # UK:  15/01/2026
    else:
        return date.strftime("%B %-d, %Y") if os.name != "nt" else date.strftime("%B %#d, %Y")
        # Long-form: January 15, 2026  (%-d on Unix, %#d on Windows)


def inject_nulls(series: pd.Series, null_rate: float = 0.15) -> pd.Series:
    """Randomly replace values with NaN to simulate missing export fields."""
    mask = np.random.random(len(series)) < null_rate
    series = series.copy().astype(object)
    series[mask] = np.nan
    return series


# ════════════════════════════════════════════════════════════════════════════
#  Transaction template catalogue
#  Each entry: (description_template, location, amount_range, type)
#  type → 'debit' | 'credit'
# ════════════════════════════════════════════════════════════════════════════

TRANSACTIONS = [
    # ── Groceries ────────────────────────────────────────────────────────────
    ("Walmart Supercenter",           "Dallas, TX",       (15.0,  250.0),  "debit"),
    ("WALMART #4821",                 "Dallas, TX",       (10.0,  180.0),  "debit"),
    ("Target Store Purchase",         "Austin, TX",       (20.0,  300.0),  "debit"),
    ("TARGET  #0095  ",               "Austin, TX",       (5.0,   120.0),  "debit"),   # trailing space
    ("Whole Foods Market",            "Houston, TX",      (30.0,  200.0),  "debit"),
    ("Kroger Grocery",                "San Antonio, TX",  (25.0,  175.0),  "debit"),

    # ── Dining & Coffee ──────────────────────────────────────────────────────
    ("Starbucks Coffee #3421",        "Dallas, TX",       (4.5,   18.0),   "debit"),
    ("STARBUCKS STORE 14122",         "Austin, TX",       (3.5,   15.0),   "debit"),
    ("McDonald's",                    "Dallas, TX",       (5.0,   25.0),   "debit"),
    ("Chipotle Mexican Grill",        "Austin, TX",       (8.0,   22.0),   "debit"),
    ("Dominos Pizza",                 "Houston, TX",      (12.0,  45.0),   "debit"),
    ("The Cheesecake Factory",        "Dallas, TX",       (30.0,  90.0),   "debit"),

    # ── Transport & Fuel ─────────────────────────────────────────────────────
    ("Shell Gas Station",             "Dallas, TX",       (25.0,  80.0),   "debit"),
    ("SHELL OIL 57444006200",         "Austin, TX",       (30.0,  90.0),   "debit"),
    ("Uber Trip",                     None,               (5.0,   55.0),   "debit"),   # location=None → NaN
    ("Lyft Ride",                     None,               (5.0,   50.0),   "debit"),
    ("ExxonMobil",                    "Houston, TX",      (20.0,  75.0),   "debit"),

    # ── Subscriptions ────────────────────────────────────────────────────────
    ("Netflix.com",                   None,               (15.49, 15.49),  "debit"),
    ("NETFLIX SUBSCRIPTION",          None,               (15.49, 15.49),  "debit"),
    ("Spotify USA",                   None,               (9.99,  9.99),   "debit"),
    ("Amazon Prime Membership",       None,               (14.99, 14.99),  "debit"),
    ("Adobe Creative Cloud",          None,               (54.99, 54.99),  "debit"),
    ("Hulu",                          None,               (7.99,  17.99),  "debit"),

    # ── Shopping (Online) ────────────────────────────────────────────────────
    ("Amazon.com Purchase",           None,               (10.0,  350.0),  "debit"),
    ("amazon mktplace pmts",          None,               (8.0,   200.0),  "debit"),
    ("eBay Purchase",                 None,               (15.0,  180.0),  "debit"),
    ("Best Buy",                      "Dallas, TX",       (50.0,  600.0),  "debit"),

    # ── Utilities & Bills ────────────────────────────────────────────────────
    ("AT&T Phone Bill",               None,               (55.0,  120.0),  "debit"),
    ("Electric Bill - Oncor",         None,               (60.0,  200.0),  "debit"),
    ("Internet - Spectrum",           None,               (49.99, 79.99),  "debit"),

    # ── Income / Credits ─────────────────────────────────────────────────────
    ("Payroll Deposit - ACME Corp",   None,               (2200.0,3500.0), "credit"),
    ("Zelle Transfer Received",       None,               (20.0,  500.0),  "credit"),
    ("Tax Refund",                    None,               (150.0, 800.0),  "credit"),
    ("Interest Payment",              None,               (0.10,  5.0),    "credit"),
]


# ════════════════════════════════════════════════════════════════════════════
#  Core generator
# ════════════════════════════════════════════════════════════════════════════

def generate_month(
    year: int,
    month: int,
    n_rows: int,
    start_txn_id: int,
) -> pd.DataFrame:
    """
    Build a single month's messy DataFrame.

    Parameters
    ----------
    year, month : Calendar period.
    n_rows      : Number of transactions to generate.
    start_txn_id: First transaction ID for this batch (ensures globally unique IDs
                  across files, except for the deliberate duplicate we inject).
    """
    month_start = pd.Timestamp(year=year, month=month, day=1)
    month_end   = month_start + pd.offsets.MonthEnd(0)
    date_range  = pd.date_range(month_start, month_end, freq="D")

    rows = []
    for i in range(n_rows):
        txn_id      = start_txn_id + i
        txn_date    = random.choice(date_range)
        template    = random.choice(TRANSACTIONS)
        desc, loc, (lo, hi), txn_type = template

        amount_raw  = round(random.uniform(lo, hi), 2)
        amount      = amount_raw if txn_type == "credit" else -amount_raw

        # Randomise date format per row
        date_style  = random.randint(0, 2)
        date_str    = fmt_date(txn_date, date_style)

        # Randomise some description casing messiness
        casing = random.choice(["asis", "upper", "title"])
        if casing == "upper":
            desc = desc.upper()
        elif casing == "title":
            desc = desc.title()

        rows.append({
            "transaction_id": f"TXN{txn_id:06d}",
            "date":           date_str,
            "description":    desc,
            "amount":         fmt_currency(amount),
            "location":       loc,
            "account":        random.choice(["Checking", "Savings"]),
        })

    df = pd.DataFrame(rows)

    # ── Inject missing values ────────────────────────────────────────────────
    df["description"] = inject_nulls(df["description"], null_rate=0.08)
    df["location"]    = inject_nulls(df["location"],    null_rate=0.25)

    # ── Inject one deliberate duplicate transaction_id ───────────────────────
    dup_idx = random.randint(0, n_rows - 2)
    df.iloc[dup_idx + 1, df.columns.get_loc("transaction_id")] = (
        df.iloc[dup_idx]["transaction_id"]
    )

    # ── Shuffle rows so dates aren't sorted ─────────────────────────────────
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)

    return df


# ════════════════════════════════════════════════════════════════════════════
#  Main
# ════════════════════════════════════════════════════════════════════════════

MONTHS = [
    (2026, 1, "january_2026",  45, 1),
    (2026, 2, "february_2026", 38, 46),
    (2026, 3, "march_2026",    50, 84),
]

if __name__ == "__main__":
    print("=" * 60)
    print("  Personal Finance ETL — Phase 1: Mock Data Generator")
    print("=" * 60)

    for year, month, filename, n_rows, start_id in MONTHS:
        df        = generate_month(year, month, n_rows, start_id)
        out_path  = os.path.join(RAW_DIR, f"{filename}.csv")
        df.to_csv(out_path, index=False)

        print(f"\n[OK] Generated: {out_path}")
        print(f"   Rows      : {len(df)}")
        print(f"   Columns   : {list(df.columns)}")
        print(f"   Date formats found   : {df['date'].apply(lambda d: 'ISO' if '-' in str(d) else ('Slash' if '/' in str(d) else 'Long')).value_counts().to_dict()}")
        print(f"   Null descriptions    : {df['description'].isna().sum()}")
        print(f"   Null locations       : {df['location'].isna().sum()}")
        dup_count = df['transaction_id'].duplicated().sum()
        print(f"   Duplicate txn IDs    : {dup_count}  {'[!] (intentional)' if dup_count else ''}")
        print(f"   Sample rows:")
        print(df.head(4).to_string(index=False))

    print("\n" + "=" * 60)
    print("  All files written to ./data/raw/")
    print("  Phase 1 complete. Ready for Phase 2 -> pipeline.py")
    print("=" * 60)

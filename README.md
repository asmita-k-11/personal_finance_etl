# Personal Finance ETL Pipeline

A end-to-end **Data Engineering portfolio project** built with Python and SQL — demonstrating raw data ingestion, ETL pipeline design, relational database modelling, and advanced analytical querying.

---

## Project Roadmap

| Phase | Description | Key Skills | Status |
|-------|-------------|------------|--------|
| Phase 1 | Mock Data Generation | Python, NumPy, CSV | ✅ Complete |
| Phase 2 | ETL & Cleaning Pipeline | Pandas, Data Quality | ✅ Complete |
| Phase 3 | Database Schema & Loading | SQLite, SQLAlchemy, Relational Modelling | ✅ Complete |
| Phase 4 | SQL Analytics Portfolio | CTEs, Window Functions, Aggregations | ✅ Complete |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.13 |
| Data Manipulation | Pandas, NumPy |
| Database | SQLite (portable to PostgreSQL) |
| ORM / DB Access | SQLAlchemy |
| SQL Concepts | CTEs, `LAG()`, `RANK()`, `ROW_NUMBER() PARTITION BY` |

---

## Project Structure

```
personal-finance-etl/
│
├── data/
│   ├── raw/                        # Phase 1 output — messy CSVs
│   │   ├── january_2026.csv
│   │   ├── february_2026.csv
│   │   └── march_2026.csv
│   └── processed/
│       └── cleaned_transactions.csv  # Phase 2 output — cleaned data
│
├── generate_mock_data.py           # Phase 1 — Messy mock data generator
├── pipeline.py                     # Phase 2 — ETL cleaning pipeline
├── schema.sql                      # Phase 3 — Relational DB schema (DDL)
├── load_to_db.py                   # Phase 3 — SQLAlchemy data loader
├── analysis.sql                    # Phase 4 — Analytical SQL queries
├── run_analysis.py                 # Phase 4 — Query runner & report printer
│
├── requirements.txt
├── .gitignore
└── README.md
```

> `finance.db` is generated locally and excluded from version control via `.gitignore`.

---

## How to Run (End-to-End)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Phase 1 — Generate messy mock data
python generate_mock_data.py

# 3. Phase 2 — Run the ETL cleaning pipeline
python pipeline.py

# 4. Phase 3 — Load cleaned data into the SQLite database
python load_to_db.py

# 5. Phase 4 — Run analytical SQL queries
python run_analysis.py
```

---

## Phase 1 — Mock Data Generation

**Script:** `generate_mock_data.py`

Generates 3 months of intentionally "messy" bank statement CSVs (Jan–Mar 2026) to simulate real-world data quality challenges.

**Injected data quality issues:**

| Issue | Example |
|---|---|
| Mixed date formats | `2026-01-15` vs `15/01/2026` vs `January 15, 2026` |
| Currency as strings | `$1,250.50` / `($45.00)` / `-45.00` |
| Missing values | `NaN` in `description` (8%) and `location` (25%) |
| Duplicate transaction IDs | 1 intentional duplicate per file |
| Inconsistent casing | `WALMART SUPERCENTER` vs `Walmart Supercenter` |
| Trailing whitespace | `"TARGET  #0095  "` |

**Output:** `data/raw/january_2026.csv`, `february_2026.csv`, `march_2026.csv`

---

## Phase 2 — ETL & Cleaning Pipeline

**Script:** `pipeline.py`

A modular Extract → Transform → Categorise pipeline built with Pandas.

| Stage | Function | What it solves |
|---|---|---|
| Extract | `extract()` | Reads all CSVs, tags `source_month` |
| Transform | `transform_dates()` | Parses all 3 date formats → `YYYY-MM-DD` |
| Transform | `transform_amounts()` | Converts messy strings → signed `float` |
| Transform | `clean_text()` | Strips whitespace, normalises casing |
| Transform | `handle_nulls()` | Fills missing values with sensible defaults |
| Transform | `deduplicate()` | Removes duplicate `transaction_id` rows |
| Categorise | `categorise()` | Keyword-maps descriptions → 8 categories |
| Load | `load()` | Writes `data/processed/cleaned_transactions.csv` |

**Categories assigned:** Groceries, Dining, Transport, Subscriptions, Shopping, Utilities, Income, Uncategorised

**Output:** `data/processed/cleaned_transactions.csv` (130 clean rows from 133 raw)

---

## Phase 3 — Database Schema & Data Loading

**Scripts:** `schema.sql`, `load_to_db.py`

### Relational Schema (3 tables)

```
categories                 accounts
──────────────────         ──────────────────
category_id  PK INT        account_id  PK INT
name         TEXT UNIQUE   name        TEXT UNIQUE
      │                          │
      └──────────┐  ┌────────────┘
                 ▼  ▼
              transactions
              ───────────────────────────────
              transaction_id   TEXT  PRIMARY KEY
              date             DATE  NOT NULL
              description      TEXT
              amount           REAL  NOT NULL
              transaction_type TEXT  CHECK (debit|credit)
              category_id      INT   FK → categories
              location         TEXT
              account_id       INT   FK → accounts
              source_month     TEXT
              loaded_at        TIMESTAMP
```

**Key design decisions:**
- Surrogate integer PKs for lookup tables (easy to rename without cascade updates)
- `transaction_id` as natural business key (guaranteed unique after Phase 2)
- Signed `amount` (negative = debit) with a separate `transaction_type` column for easy filtering
- 4 indexes on `date`, `category_id`, `transaction_type`, `source_month` for query performance
- Upsert strategy (`INSERT OR REPLACE`) makes re-runs fully idempotent

---

## Phase 4 — SQL Analytics Portfolio

**Files:** `analysis.sql`, `run_analysis.py`

Four analytical queries, each targeting a real business question:

### Query 1 — Monthly Spending & Income Summary
> *"How much did I spend and earn each month?"*

**Concepts:** `JOIN`, `GROUP BY`, `CASE` aggregation

| Month | Spend | Income | Net Balance |
|---|---|---|---|
| January 2026 | $2,571.56 | $7,092.62 | +$4,521.06 |
| February 2026 | $2,369.19 | $237.13 | -$2,132.06 |
| March 2026 | $2,643.00 | $5,991.46 | +$3,348.46 |

---

### Query 2 — Month-over-Month Spending Change
> *"Did I spend more or less this month vs last month?"*

**Concepts:** `CTE (WITH)`, `LAG()` window function

| Month | Spend | Change | % Change |
|---|---|---|---|
| January 2026 | $2,571.56 | — | First month |
| February 2026 | $2,369.19 | -$202.37 | **-7.9%** |
| March 2026 | $2,643.00 | +$273.81 | **+11.6%** |

---

### Query 3 — Top Expense Categories
> *"Which categories cost the most? What share of my spending?"*

**Concepts:** `CTE`, `RANK()` window function, `SUM() OVER()` for percentage share

| Rank | Category | Transactions | Total | Share |
|---|---|---|---|---|
| 1 | Shopping | 14 | $3,058.01 | 40.3% |
| 2 | Groceries | 19 | $1,534.33 | 20.2% |
| 3 | Utilities | 11 | $950.28 | 12.5% |
| 4 | Transport | 17 | $745.11 | 9.8% |

---

### Query 4 — Top 3 Transactions per Category
> *"What are the biggest individual purchases in each category?"*

**Concepts:** `CTE`, `ROW_NUMBER() OVER (PARTITION BY ...)` window function

Resets ranking per category — the core use case for `PARTITION BY`.

---

## Key Results

- **130 transactions** processed across 3 months
- **$7,583** total spend | **$13,321** total income
- **Shopping** is the largest expense category at 40.3% of total spend
- Spending **dropped 7.9%** in February, then **rose 11.6%** in March

---

## What I Learned

This was my first end-to-end data engineering project and a few things surprised me along the way:

- **Real data is messier than you expect.** Even with mock data I deliberately made messy, parsing dates and currency strings still required more edge-case handling than I anticipated — especially bracket notation like `($45.00)`.

- **Schema design decisions have downstream consequences.** I initially planned to store category as plain text in the transactions table. Switching to a FK lookup table felt like extra work at first, but it made the JOIN queries in Phase 4 much cleaner.

- **Idempotency matters from day one.** Making `load_to_db.py` safe to re-run (using `INSERT OR REPLACE`) saved me time when I had to re-generate data mid-project. Build for re-runs early.

- **Window functions are powerful once it clicks.** `LAG()` and `ROW_NUMBER() PARTITION BY` felt confusing until I visualised the data as a sorted list and understood that the window is just "which rows does this function see." After that, Query 2 and 4 wrote themselves.

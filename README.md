# Personal Finance ETL Pipeline

A portfolio project demonstrating core **Data Engineering** skills built with Python (Pandas) and SQL.

## Project Roadmap

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | Mock Data Generation | ✅ Complete |
| Phase 2 | Python ETL & Cleaning Pipeline | 🔜 Upcoming |
| Phase 3 | Database Schema & Data Loading | 🔜 Upcoming |
| Phase 4 | SQL Analytics Portfolio | 🔜 Upcoming |

## Tech Stack
- **Python** — Pandas, NumPy, SQLAlchemy
- **Database** — PostgreSQL / SQLite
- **SQL** — CTEs, Window Functions, Aggregations

## Phase 1 — Mock Data Generation

Generates 3 months of intentionally "messy" bank statement CSVs to simulate real-world ETL challenges:

- Mixed date formats (`YYYY-MM-DD`, `DD/MM/YYYY`, `Month D, YYYY`)
- Currency amounts as strings (`$1,250.50`, `($45.00)`, `-45.00`)
- Missing values in `description` and `location` columns
- Duplicate transaction IDs
- Inconsistent text casing and whitespace

### Run

```bash
pip install -r requirements.txt
python generate_mock_data.py
```

Output CSVs are written to `data/raw/`.

## Project Structure

```
personal-finance-etl/
├── data/
│   └── raw/                  # Messy input CSVs (Phase 1 output)
├── generate_mock_data.py     # Phase 1 — Mock data generator
├── requirements.txt
└── README.md
```

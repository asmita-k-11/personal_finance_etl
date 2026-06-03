-- =============================================================================
-- schema.sql
-- Personal Finance ETL Pipeline - Phase 3
-- Relational Database Schema (SQLite-compatible, portable to PostgreSQL)
-- =============================================================================
--
-- Entity Relationship Overview:
--
--   categories ──┐
--                ├──► transactions ◄──── accounts
--
--   Three tables:
--     1. categories    — lookup table for transaction categories (8 values)
--     2. accounts      — lookup table for account types (Checking / Savings)
--     3. transactions  — central fact table; FKs to categories + accounts
--
-- Design decisions:
--   • category_id / account_id are surrogate integer PKs (not text) so that
--     renaming a category later is a single-table update, not a cascade.
--   • transaction_id is the natural business key (TXN000001) — used as PK
--     because it is guaranteed unique after Phase 2 deduplication.
--   • amount is stored as REAL (float); sign encodes direction (negative=debit)
--     and transaction_type is stored separately for easy filtering.
--   • Raw/audit columns (date_raw, amount_raw, source_file) are kept in the
--     DB for lineage / debugging — they can be dropped after validation.
-- =============================================================================


-- ── Enable foreign key enforcement (SQLite only; PostgreSQL does this by default)
PRAGMA foreign_keys = ON;


-- =============================================================================
-- TABLE 1: categories
-- Lookup table for spending categories.
-- =============================================================================
CREATE TABLE IF NOT EXISTS categories (
    category_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL UNIQUE    -- e.g. 'Groceries', 'Dining'
);

-- Pre-seed all known categories so FK references work on first load
INSERT OR IGNORE INTO categories (name) VALUES
    ('Groceries'),
    ('Dining'),
    ('Transport'),
    ('Subscriptions'),
    ('Shopping'),
    ('Utilities'),
    ('Income'),
    ('Uncategorised');


-- =============================================================================
-- TABLE 2: accounts
-- Lookup table for bank account types.
-- =============================================================================
CREATE TABLE IF NOT EXISTS accounts (
    account_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL UNIQUE    -- e.g. 'Checking', 'Savings'
);

-- Pre-seed known account types
INSERT OR IGNORE INTO accounts (name) VALUES
    ('Checking'),
    ('Savings');


-- =============================================================================
-- TABLE 3: transactions
-- Central fact table. One row per unique financial transaction.
-- =============================================================================
CREATE TABLE IF NOT EXISTS transactions (
    -- Business key (natural PK from source data, deduped in Phase 2)
    transaction_id      TEXT    PRIMARY KEY,

    -- When the transaction occurred
    date                DATE    NOT NULL,

    -- Cleaned merchant / description (title-cased, trimmed)
    description         TEXT    NOT NULL DEFAULT 'Unknown',

    -- Signed numeric amount:
    --   negative → debit (money leaving account)
    --   positive → credit (money entering account)
    amount              REAL    NOT NULL,

    -- Derived from sign of amount for easy filtering
    transaction_type    TEXT    NOT NULL
                        CHECK (transaction_type IN ('debit', 'credit')),

    -- FK to categories lookup table
    category_id         INTEGER NOT NULL
                        REFERENCES categories (category_id)
                        ON UPDATE CASCADE
                        ON DELETE RESTRICT,

    -- Where the transaction happened (cleaned or 'Online / Unknown')
    location            TEXT    NOT NULL DEFAULT 'Online / Unknown',

    -- FK to accounts lookup table
    account_id          INTEGER NOT NULL
                        REFERENCES accounts (account_id)
                        ON UPDATE CASCADE
                        ON DELETE RESTRICT,

    -- Human-readable month label, e.g. 'January 2026'
    source_month        TEXT    NOT NULL,

    -- ── Audit / lineage columns ──────────────────────────────────────────────
    -- Original raw values preserved for traceability
    date_raw            TEXT,
    amount_raw          TEXT,
    source_file         TEXT,

    -- Timestamp of when this row was loaded into the DB
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =============================================================================
-- INDEXES
-- Speed up the analytical queries we will write in Phase 4.
-- =============================================================================

-- Queries that filter / group by date range
CREATE INDEX IF NOT EXISTS idx_txn_date
    ON transactions (date);

-- Queries that group by category (most common in analytics)
CREATE INDEX IF NOT EXISTS idx_txn_category
    ON transactions (category_id);

-- Queries that filter by transaction type (debit vs credit)
CREATE INDEX IF NOT EXISTS idx_txn_type
    ON transactions (transaction_type);

-- Queries that group by month
CREATE INDEX IF NOT EXISTS idx_txn_month
    ON transactions (source_month);

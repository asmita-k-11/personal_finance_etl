-- =============================================================================
-- analysis.sql
-- Personal Finance ETL Pipeline - Phase 4: SQL Analytics Portfolio
-- =============================================================================
--
-- Four analytical queries, each targeting a real business question and
-- demonstrating a different advanced SQL concept:
--
--   Query 1 — Monthly Spending Summary      (GROUP BY + JOIN)
--   Query 2 — Month-over-Month Change       (CTE + LAG window function)
--   Query 3 — Top Expense Categories        (CTE + RANK window function)
--   Query 4 — Biggest Transactions per Cat  (CTE + ROW_NUMBER window function)
--
-- All queries run against the 3-table schema created in Phase 3:
--   transactions  ──FK──►  categories
--   transactions  ──FK──►  accounts
-- =============================================================================


-- =============================================================================
-- QUERY 1: Monthly Spending & Income Summary
-- -----------------------------------------------------------------------------
-- Business question:
--   "How much did I spend and earn each month, and what is the net balance?"
--
-- SQL concepts: JOIN, GROUP BY, CASE / FILTER aggregation, ROUND
-- =============================================================================

SELECT
    t.source_month                                           AS month,

    -- Total amount spent (debits only — stored as negative, so use ABS)
    ROUND(ABS(SUM(CASE WHEN t.transaction_type = 'debit'
                       THEN t.amount ELSE 0 END)), 2)       AS total_spend,

    -- Total income received (credits only)
    ROUND(SUM(CASE WHEN t.transaction_type = 'credit'
                   THEN t.amount ELSE 0 END), 2)            AS total_income,

    -- Net balance for the month (income - spend)
    ROUND(
        SUM(CASE WHEN t.transaction_type = 'credit' THEN  t.amount ELSE 0 END)
      + SUM(CASE WHEN t.transaction_type = 'debit'  THEN  t.amount ELSE 0 END),
    2)                                                       AS net_balance,

    COUNT(*)                                                 AS total_transactions

FROM transactions t
GROUP BY t.source_month
ORDER BY MIN(t.date);   -- chronological order


-- =============================================================================
-- QUERY 2: Month-over-Month Spending Change  (CTE + LAG)
-- -----------------------------------------------------------------------------
-- Business question:
--   "Did I spend more or less this month compared to last month?"
--
-- SQL concepts: CTE (WITH clause), LAG() window function, ROUND, CASE
--
-- LAG(col, 1) OVER (ORDER BY ...) fetches the value from the *previous* row,
-- allowing us to compute the delta without a self-join.
-- =============================================================================

WITH monthly_spend AS (
    -- Step 1: Aggregate total spend per month (debits only)
    SELECT
        source_month,
        MIN(date)                                           AS first_date,
        ROUND(ABS(SUM(amount)), 2)                         AS total_spend
    FROM transactions
    WHERE transaction_type = 'debit'
    GROUP BY source_month
),

spend_with_lag AS (
    -- Step 2: Use LAG() to pull the previous month's spend into the same row
    SELECT
        source_month,
        total_spend                                         AS this_month_spend,
        LAG(total_spend, 1) OVER (ORDER BY first_date)     AS prev_month_spend
    FROM monthly_spend
)

-- Step 3: Calculate the absolute and percentage change
SELECT
    source_month                                            AS month,
    this_month_spend,
    COALESCE(prev_month_spend, 0)                          AS prev_month_spend,

    -- Absolute change (positive = spent more, negative = spent less)
    ROUND(this_month_spend - COALESCE(prev_month_spend, 0), 2)
                                                            AS change_amount,

    -- Percentage change vs previous month
    CASE
        WHEN prev_month_spend IS NULL THEN 'N/A (first month)'
        WHEN prev_month_spend = 0     THEN 'N/A (no prior spend)'
        ELSE ROUND(
            ((this_month_spend - prev_month_spend) / prev_month_spend) * 100,
        1) || '%'
    END                                                     AS pct_change

FROM spend_with_lag
ORDER BY month;


-- =============================================================================
-- QUERY 3: Top Expense Categories by Total Spend  (CTE + RANK)
-- -----------------------------------------------------------------------------
-- Business question:
--   "Which spending categories cost the most? How do they rank?"
--
-- SQL concepts: CTE, JOIN, GROUP BY, RANK() window function, ROUND
--
-- RANK() assigns a rank to each category based on total spend.
-- Ties receive the same rank (1, 1, 3 ...) — unlike ROW_NUMBER which is strict.
-- =============================================================================

WITH category_totals AS (
    -- Step 1: Sum spend per category (debits only, excludes Income)
    SELECT
        c.name                                              AS category,
        COUNT(*)                                            AS num_transactions,
        ROUND(ABS(SUM(t.amount)), 2)                       AS total_spend,
        ROUND(ABS(AVG(t.amount)), 2)                       AS avg_transaction
    FROM transactions t
    JOIN categories   c ON t.category_id = c.category_id
    WHERE t.transaction_type = 'debit'
    GROUP BY c.name
),

ranked AS (
    -- Step 2: Rank categories from highest to lowest spend
    SELECT
        category,
        num_transactions,
        total_spend,
        avg_transaction,
        RANK() OVER (ORDER BY total_spend DESC)            AS spend_rank,

        -- What share of total debit spend does this category represent?
        ROUND(
            total_spend * 100.0
            / SUM(total_spend) OVER (),                    -- window = all rows
        1)                                                  AS pct_of_total
    FROM category_totals
)

SELECT
    spend_rank,
    category,
    num_transactions,
    total_spend,
    avg_transaction,
    pct_of_total || '%'                                    AS share_of_spend
FROM ranked
ORDER BY spend_rank;


-- =============================================================================
-- QUERY 4: Top 3 Largest Transactions per Category  (CTE + ROW_NUMBER)
-- -----------------------------------------------------------------------------
-- Business question:
--   "What are the biggest individual purchases in each category?"
--
-- SQL concepts: CTE, ROW_NUMBER() PARTITION BY, JOIN, filtering on window result
--
-- ROW_NUMBER() PARTITION BY category restarts the counter for each category,
-- letting us pick the top N rows within each group — a classic pattern that
-- would require complex subqueries without window functions.
-- =============================================================================

WITH ranked_transactions AS (
    SELECT
        c.name                                              AS category,
        t.transaction_id,
        t.date,
        t.description,
        ABS(t.amount)                                       AS amount,
        t.location,
        a.name                                              AS account,

        -- Restart numbering for each category, largest amount = rank 1
        ROW_NUMBER() OVER (
            PARTITION BY t.category_id
            ORDER BY ABS(t.amount) DESC
        )                                                   AS row_num

    FROM transactions t
    JOIN categories   c ON t.category_id = c.category_id
    JOIN accounts     a ON t.account_id  = a.account_id
    WHERE t.transaction_type = 'debit'   -- spending only, not income
)

SELECT
    row_num     AS rank_in_category,
    category,
    transaction_id,
    date,
    description,
    amount,
    location
FROM ranked_transactions
WHERE row_num <= 3           -- keep only the top 3 per category
ORDER BY category, row_num;

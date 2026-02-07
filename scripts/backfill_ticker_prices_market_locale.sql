-- Backfill market and locale columns for ticker_prices records
-- 
-- Purpose: Add market='stocks' and locale='us' to all existing records that don't have these values.
--          All historical data in ticker_prices was fetched from US stock markets.
--
-- Run this AFTER the schema evolution has been applied to the table.
--
-- Usage:
--   aws athena start-query-execution \
--     --query-string "$(cat scripts/backfill_ticker_prices_market_locale.sql)" \
--     --work-group petals \
--     --profile personal

UPDATE market_data.ticker_prices
SET 
    market = 'stocks',
    locale = 'us'
WHERE 
    market IS NULL OR locale IS NULL;

-- Note: This UPDATE operation in Iceberg will:
-- 1. Read the affected data files
-- 2. Write new data files with updated values
-- 3. Update table metadata to point to new files
-- 4. Mark old files for eventual deletion
--
-- For large tables, this may take several minutes.
-- Check execution progress in Athena console or via CLI.

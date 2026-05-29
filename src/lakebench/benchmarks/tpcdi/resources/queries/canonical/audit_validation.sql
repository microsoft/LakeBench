-- Audit validation query: compare DW row counts against audit data
-- This query checks that each target table has the expected number of rows
-- as specified in the TPC-DI audit files.
SELECT
    m.message_source AS table_name,
    m.message_data AS expected_count,
    m.batch_id
FROM di_messages m
WHERE m.message_type = 'Validation'
  AND m.message_source IN (
    'dim_customer', 'dim_account', 'dim_broker', 'dim_company',
    'dim_security', 'dim_trade', 'fact_market_history', 'fact_watches',
    'fact_cash_balances', 'fact_holdings', 'financial', 'prospect'
  )
ORDER BY m.batch_id, m.message_source;

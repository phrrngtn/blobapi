-- Merge pricing from a high-ceremony database (PG/SQL Server) into a
-- local DuckDB llm_pricing table via ODBC.
--
-- The high-ceremony llm_model_price_history table is append-only (TTST).
-- Current rows have sys_to IS NULL. This query fetches only current rows
-- and merges them into the local session table, which is a simple
-- (non-temporal) reference table used for cost accounting.
--
-- Prerequisites:
--   - blobodbc or nanodbc extension loaded (for odbc_query)
--   - DSN configured for the high-ceremony database
--   - Local llm_pricing table created (sql/create_llm_pricing.sql)
--
-- Usage:
--   SET VARIABLE pricing_dsn = 'my_pg_dsn';
--   .read sql/merge_pricing_from_ttst.sql
--
-- Or inline:
--   SELECT * FROM merge_pricing_from_ttst('my_pg_dsn');

-- Macro that fetches current prices from the TTST via ODBC and returns
-- them as a table. Use this for inspection before merging.
CREATE OR REPLACE MACRO ttst_current_pricing(dsn) AS TABLE (
    SELECT * FROM odbc_query(dsn, '
        SELECT model,
               input_per_mtok,
               output_per_mtok,
               cache_write_5m_per_mtok,
               cache_write_1h_per_mtok,
               cache_read_per_mtok,
               batch_input_per_mtok,
               batch_output_per_mtok
        FROM llm_model_price_history
        WHERE sys_to IS NULL
    ')
);

-- Merge: INSERT OR REPLACE current TTST rows into local llm_pricing.
-- This is a full refresh — all current rows are upserted.
-- The local table has no temporal columns (it's a session-scoped snapshot).
CREATE OR REPLACE MACRO merge_pricing_from_ttst(dsn) AS TABLE (
    WITH REMOTE_PRICING AS (
        SELECT * FROM ttst_current_pricing(dsn)
    )
    INSERT OR REPLACE INTO llm_pricing
    SELECT * FROM REMOTE_PRICING
    RETURNING *
);

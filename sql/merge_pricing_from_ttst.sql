-- Read current pricing from a high-ceremony database (PG/SQL Server) via blobodbc.
--
-- The high-ceremony llm_model_price_history table is append-only (TTST).
-- Current rows have sys_to IS NULL. This macro fetches them as JSON via
-- bo_query and returns a relational table.
--
-- Prerequisites:
--   - blobodbc extension loaded
--
-- Connection string formats:
--   PostgreSQL DSN:  'DSN=rule4_test'
--   SQL Server:      read from connections.toml, not hardcoded in SQL
--
-- Usage:
--   -- Read-only (inspect what's on the high-ceremony DB)
--   SELECT * FROM ttst_current_pricing('DSN=rule4_test');
--
--   -- Merge into local llm_pricing table
--   INSERT OR REPLACE INTO llm_pricing
--   SELECT * FROM ttst_current_pricing('DSN=rule4_test');

CREATE OR REPLACE MACRO ttst_current_pricing(conn_str) AS TABLE (
    WITH REMOTE_JSON AS (
        SELECT bo_query(conn_str,
            'SELECT model, input_per_mtok, output_per_mtok,
                    cache_write_5m_per_mtok, cache_write_1h_per_mtok,
                    cache_read_per_mtok, batch_input_per_mtok,
                    batch_output_per_mtok
             FROM llm_model_price_history
             WHERE sys_to IS NULL') AS j
    ),
    REMOTE_ROWS AS (
        SELECT unnest(from_json(CAST(j AS JSON), '["json"]')) AS r
        FROM REMOTE_JSON
    )
    SELECT
        r->>'$.model'                                  AS model,
        CAST(r->>'$.input_per_mtok' AS FLOAT)          AS input_per_mtok,
        CAST(r->>'$.output_per_mtok' AS FLOAT)         AS output_per_mtok,
        CAST(r->>'$.cache_write_5m_per_mtok' AS FLOAT) AS cache_write_5m_per_mtok,
        CAST(r->>'$.cache_write_1h_per_mtok' AS FLOAT) AS cache_write_1h_per_mtok,
        CAST(r->>'$.cache_read_per_mtok' AS FLOAT)     AS cache_read_per_mtok,
        CAST(r->>'$.batch_input_per_mtok' AS FLOAT)    AS batch_input_per_mtok,
        CAST(r->>'$.batch_output_per_mtok' AS FLOAT)   AS batch_output_per_mtok
    FROM REMOTE_ROWS
);

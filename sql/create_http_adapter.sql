-- Create the http_adapter table for DuckDB sessions.
--
-- This is the DuckDB-local equivalent of domain.http_adapter in PostgreSQL.
-- Use load_http_adapters.sql to populate from YAML, or copy from PG:
--
--   ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES);
--   INSERT OR REPLACE INTO http_adapter
--   SELECT * FROM pg.domain.http_adapter;

CREATE TABLE IF NOT EXISTS http_adapter (
    name              VARCHAR PRIMARY KEY,
    description       VARCHAR,
    method            VARCHAR DEFAULT 'get',
    url_template      VARCHAR NOT NULL,
    default_headers   JSON,
    default_params    JSON,
    rate_limit_profile VARCHAR,
    response_jmespath VARCHAR,
    response_notes    VARCHAR,
    source            VARCHAR
);

-- http_adapt.sql — DuckDB macros for calling registered HTTP adapters
--
-- Requires: blobhttp extension, blobtemplates extension, postgres scanner
-- Must be sourced AFTER attaching PG:
--   ATTACH 'host=/tmp dbname=rule4_test' AS pg (TYPE POSTGRES);
--
-- Usage:
--   .read sql/http_adapt.sql
--   SELECT * FROM http_fetch('edgar_company_tickers');
--   SELECT * FROM http_fetch('edgar_submission', json_object('cik_padded', '0000913144'));

-- ═══════════════════════════════════════════════════════════════════
-- Auto-configure rate limits from registered adapters
-- ═══════════════════════════════════════════════════════════════════

-- Build bh_http_config from all registered adapters' rate_limit_profile.
-- Groups by URL host so one rate limit applies per-host, not per-adapter.
-- Run once after loading adapters.
CREATE OR REPLACE MACRO http_adapt_init() AS TABLE (
    WITH ADAPTER_HOSTS AS (
        SELECT DISTINCT
               regexp_extract(url_template, 'https?://[^/]+/') AS host_prefix,
               rate_limit_profile
        FROM pg.domain.http_adapter
        WHERE rate_limit_profile IS NOT NULL
    )
    SELECT host_prefix, rate_limit_profile
    FROM ADAPTER_HOSTS
);

-- ═══════════════════════════════════════════════════════════════════
-- Core macro: call a registered HTTP adapter by name
--
-- Looks up the adapter definition in PG, renders the URL template
-- with the provided params, makes the HTTP call, and returns the
-- raw response body as JSON.
-- ═══════════════════════════════════════════════════════════════════

-- Generic raw adapter call.  Requires blobtemplates for URL template
-- rendering.  If blobtemplates is not loaded, use the concrete macros
-- (edgar_tickers, edgar_company, wikidata_query) which handle URL
-- construction inline.
--
-- CREATE OR REPLACE MACRO http_adapt_raw(adapter_name, params := '{}') AS (
--     SELECT (bh_http_get(
--         bt_template_render(a.url_template, params),
--         headers := a.default_headers::VARCHAR
--     )).response_body
--     FROM pg.domain.http_adapter AS a
--     WHERE a.name = adapter_name
-- );

-- ═══════════════════════════════════════════════════════════════════
-- Convenience macros for specific adapters
-- ═══════════════════════════════════════════════════════════════════

-- All SEC-registered companies (~10K rows: cik, ticker, name)
-- The JSON is {"0": {cik_str, ticker, title}, "1": {...}, ...} — an object
-- with numeric string keys, not an array. Iterate keys to extract values.
CREATE OR REPLACE MACRO edgar_tickers() AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            a.url_template,
            headers := a.default_headers::VARCHAR
        )).response_body::JSON AS doc
        FROM pg.domain.http_adapter AS a
        WHERE a.name = 'edgar_company_tickers'
    ),
    KEYS AS (
        SELECT unnest(json_keys(doc)) AS k, doc FROM RAW
    )
    SELECT doc->k->>'cik_str' AS cik,
           doc->k->>'ticker'  AS ticker,
           doc->k->>'title'   AS name
    FROM KEYS
);

-- Company details by CIK (name, SIC, tickers, state, filer category)
-- Uses REPLACE for URL template substitution (works without blobtemplates).
-- For complex templates with multiple params, use bt_template_render instead.
CREATE OR REPLACE MACRO edgar_company(cik_padded) AS TABLE (
    SELECT doc->>'cik'                  AS cik,
           doc->>'name'                 AS name,
           doc->>'sic'                  AS sic,
           doc->>'sicDescription'       AS sic_description,
           doc->>'stateOfIncorporation' AS state,
           doc->>'fiscalYearEnd'        AS fiscal_year_end,
           doc->>'category'             AS filer_category,
           doc->'tickers'               AS tickers_json,
           doc->'exchanges'             AS exchanges_json
    FROM (
        SELECT (bh_http_get(
            REPLACE(a.url_template, '{{ cik_padded }}', cik_padded),
            headers := a.default_headers::VARCHAR
        )).response_body::JSON AS doc
        FROM pg.domain.http_adapter AS a
        WHERE a.name = 'edgar_submission'
    )
);

-- Wikidata SPARQL query → label + alt_label rows
CREATE OR REPLACE MACRO wikidata_query(sparql) AS TABLE (
    WITH RAW AS (
        SELECT (bh_http_get(
            a.url_template,
            headers := a.default_headers::VARCHAR,
            params  := json_object('query', sparql, 'format', 'json')
        )).response_body::JSON AS doc
        FROM pg.domain.http_adapter AS a
        WHERE a.name = 'wikidata_sparql'
    ),
    BINDINGS AS (
        SELECT unnest(from_json(doc->'results'->'bindings', '["json"]')) AS b
        FROM RAW
    )
    SELECT b->'label'->>'value'    AS label,
           b->'altLabel'->>'value' AS alt_label,
           b->'item'->>'value'     AS item_uri
    FROM BINDINGS
    WHERE b->'label'->>'value' IS NOT NULL
);

-- ═══════════════════════════════════════════════════════════════════
-- Examples
-- ═══════════════════════════════════════════════════════════════════

-- List all registered adapters:
--   SELECT name, method, rate_limit_profile, description
--   FROM pg.domain.http_adapter;

-- All public companies:
--   SELECT * FROM edgar_tickers() LIMIT 10;

-- Look up a specific company:
--   SELECT * FROM edgar_company('0000913144');  -- RenaissanceRe
--   SELECT * FROM edgar_company('0000320193');  -- Apple

-- Find all insurance companies (SIC 6331):
--   SELECT t.ticker, t.name, c.sic, c.sic_description
--   FROM edgar_tickers() AS t,
--        LATERAL edgar_company(LPAD(t.cik, 10, '0')) AS c
--   WHERE c.sic = '6331'
--   LIMIT 20;

-- Fetch Wikidata domain members:
--   SELECT * FROM wikidata_query('
--       SELECT ?item ?label ?altLabel WHERE {
--         ?item wdt:P31 wd:Q3624078 .
--         ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
--         OPTIONAL { ?item skos:altLabel ?altLabel . FILTER(LANG(?altLabel) = "en") }
--       }
--   ') LIMIT 20;

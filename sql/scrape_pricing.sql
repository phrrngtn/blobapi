-- Pure-SQL pricing scrape: Jina Reader + LLM extraction via blobhttp.
--
-- Pipeline:
--   1. bh_http_get('https://r.jina.ai/{pricing_url}') → clean markdown
--   2. llm_adapt('llm_model_cost', {provider, pricing_page}) → structured JSON
--   3. Unnest and flatten into a table
--
-- Prerequisites:
--   - bhttp + blobtemplates extensions loaded
--   - llm_adapter table populated (llm_model_cost adapter)
--   - llm_adapt macro sourced
--
-- Usage (from blobapi directory):
--   duckdb -unsigned -init sql/scrape_pricing_init.sql
--
-- Or inline after loading extensions and macros:
--   SELECT * FROM scrape_pricing('anthropic',
--       'https://platform.claude.com/docs/en/about-claude/pricing');

-- Scrape a single provider's pricing page via Jina Reader + LLM extraction.
-- Returns one row per model with typed pricing columns.
-- target_selector: CSS selector for Jina X-Target-Selector header (e.g. 'table')
-- remove_selector: CSS selector for Jina X-Remove-Selector header (e.g. 'nav, footer')
CREATE OR REPLACE MACRO scrape_pricing(provider, pricing_url,
    model := 'anthropic/claude-haiku-4-5-20251001',
    target_selector := NULL::VARCHAR,
    remove_selector := NULL::VARCHAR) AS TABLE (
    WITH JINA_HEADERS AS (
        SELECT MAP_FROM_ENTRIES(
            list_filter([
                {'key': 'X-Target-Selector', 'value': target_selector},
                {'key': 'X-Remove-Selector', 'value': remove_selector}
            ], x -> x.value IS NOT NULL)
        ) AS hdrs
    ),
    JINA_FETCH AS (
        SELECT bh_http_get(
            'https://r.jina.ai/' || pricing_url,
            headers := (SELECT hdrs FROM JINA_HEADERS)
        ).response_body AS page_markdown
    ),
    LLM_RESULT AS MATERIALIZED (
        SELECT result::JSON AS r
        FROM llm_adapt('llm_model_cost',
            json_object(
                'provider', provider,
                'pricing_page', (SELECT page_markdown FROM JINA_FETCH),
                'model', model))
    )
    SELECT
        j->>'$.model_id'                          AS model_id,
        CAST(j->>'$.input_per_mtok' AS FLOAT)     AS input_per_mtok,
        CAST(j->>'$.output_per_mtok' AS FLOAT)    AS output_per_mtok,
        CAST(j->>'$.cache_write_5m_per_mtok' AS FLOAT) AS cache_write_5m_per_mtok,
        CAST(j->>'$.cache_write_1h_per_mtok' AS FLOAT) AS cache_write_1h_per_mtok,
        CAST(j->>'$.cache_read_per_mtok' AS FLOAT)     AS cache_read_per_mtok,
        CAST(j->>'$.batch_input_per_mtok' AS FLOAT)    AS batch_input_per_mtok,
        CAST(j->>'$.batch_output_per_mtok' AS FLOAT)   AS batch_output_per_mtok
    FROM (
        SELECT unnest(from_json(r->'data', '["json"]')) AS j
        FROM LLM_RESULT
    )
);

-- Scrape all providers that have a pricing URL in the llm_pricing_urls table.
-- Returns union of all provider results.
-- NOTE: this is a convenience view, not a macro — it cannot be parameterized.
-- For per-provider scraping, use scrape_pricing() directly.

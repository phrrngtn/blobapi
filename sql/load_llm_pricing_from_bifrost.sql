-- Load LLM pricing live from the Bifrost /v1/models endpoint.
-- Requires: bhttp extension (bh_http_get).
--
-- Bifrost returns per-token prices; we convert to per-MTok.
-- Cache and batch columns are derived using the standard multipliers:
--   cache_write_5m = 1.25x input, cache_write_1h = 2x input,
--   cache_read = 0.1x input, batch = 0.5x standard.
--
-- Models without pricing data are skipped.

INSERT OR REPLACE INTO llm_pricing
WITH MODELS_RAW AS (
    SELECT bh_http_get('http://localhost:8080/v1/models')::JSON AS resp
),
MODELS AS (
    SELECT unnest(from_json(resp->'data', '["json"]')) AS m
    FROM MODELS_RAW
),
BASE_PRICING AS (
    SELECT
        m->>'$.id'                                         AS model,
        CAST(m->>'$.pricing.prompt' AS DECIMAL) * 1e6      AS input_per_mtok,
        CAST(m->>'$.pricing.completion' AS DECIMAL) * 1e6  AS output_per_mtok
    FROM MODELS
    WHERE m->'$.pricing' IS NOT NULL
      AND m->>'$.pricing.prompt' IS NOT NULL
)
SELECT
    model,
    input_per_mtok,
    output_per_mtok,
    round(input_per_mtok * 1.25, 4)  AS cache_write_5m_per_mtok,
    round(input_per_mtok * 2.0, 4)   AS cache_write_1h_per_mtok,
    round(input_per_mtok * 0.1, 4)   AS cache_read_per_mtok,
    round(input_per_mtok * 0.5, 4)   AS batch_input_per_mtok,
    round(output_per_mtok * 0.5, 4)  AS batch_output_per_mtok
FROM BASE_PRICING;

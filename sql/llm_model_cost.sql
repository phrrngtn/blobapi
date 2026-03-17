-- llm_model_cost: read model pricing from Bifrost's /v1/models endpoint.
--
-- Bifrost returns per-token prices from its internal governance_model_pricing
-- table (compiled into each Docker image release). We convert to per-MTok.
--
-- Cache multipliers (standard Anthropic):
--   cache_write_5m = 1.25x input, cache_write_1h = 2x input,
--   cache_read = 0.1x input, batch = 0.5x standard.
--
-- Usage:
--   SELECT * FROM llm_model_cost();
--   SELECT * FROM llm_model_cost(endpoint := 'http://my-gateway:8080/v1/models');
--
-- Requires: bhttp extension (bh_http_get).
--
-- For web-scrape approach (LLM extracts pricing from provider page), see
-- the llm_model_cost adapter in adapters/llm_model_cost.yaml.

CREATE OR REPLACE MACRO llm_model_cost(
    endpoint := 'http://localhost:8080/v1/models') AS TABLE (
    WITH MODELS_RAW AS (
        SELECT bh_http_get(endpoint).response_body AS resp_text
    ),
    MODELS AS (
        SELECT unnest(from_json(
            json_extract(CAST(resp_text AS JSON), '$.data'),
            '["json"]'
        )) AS m
        FROM MODELS_RAW
    ),
    BASE_PRICING AS (
        SELECT
            m->>'$.id'                                            AS model_id,
            CAST(m->>'$.pricing.prompt' AS DOUBLE) * 1000000      AS input_per_mtok,
            CAST(m->>'$.pricing.completion' AS DOUBLE) * 1000000  AS output_per_mtok
        FROM MODELS
        WHERE m->>'$.pricing.prompt' IS NOT NULL
    )
    SELECT
        model_id,
        input_per_mtok,
        output_per_mtok,
        round(input_per_mtok * 1.25, 4)   AS cache_write_5m_per_mtok,
        round(input_per_mtok * 2.0, 4)    AS cache_write_1h_per_mtok,
        round(input_per_mtok * 0.1, 4)    AS cache_read_per_mtok,
        round(input_per_mtok * 0.5, 4)    AS batch_input_per_mtok,
        round(output_per_mtok * 0.5, 4)   AS batch_output_per_mtok
    FROM BASE_PRICING
);

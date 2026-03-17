-- Load LLM pricing from the static YAML file.
-- Requires: blobtemplates extension (bt_yaml_to_json).
-- All prices are USD per million tokens (MTok).

INSERT OR REPLACE INTO llm_pricing
SELECT
    j->>'$.model'                                  AS model,
    CAST(j->>'$.input_per_mtok' AS FLOAT)          AS input_per_mtok,
    CAST(j->>'$.output_per_mtok' AS FLOAT)         AS output_per_mtok,
    CAST(j->>'$.cache_write_5m_per_mtok' AS FLOAT) AS cache_write_5m_per_mtok,
    CAST(j->>'$.cache_write_1h_per_mtok' AS FLOAT) AS cache_write_1h_per_mtok,
    CAST(j->>'$.cache_read_per_mtok' AS FLOAT)     AS cache_read_per_mtok,
    CAST(j->>'$.batch_input_per_mtok' AS FLOAT)    AS batch_input_per_mtok,
    CAST(j->>'$.batch_output_per_mtok' AS FLOAT)   AS batch_output_per_mtok
FROM (
    SELECT unnest(from_json(
        bt_yaml_to_json(content),
        '["json"]'
    )) AS j
    FROM read_text('adapters/llm_pricing.yaml')
);

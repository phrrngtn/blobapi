-- DDL for the llm_pricing table.
--
-- Stores per-model token pricing for cost accounting.
-- All prices are USD per million tokens (MTok).
--
-- Cost for a query (standard, no caching or batch):
--   cost_usd = (prompt_tokens * input_per_mtok
--             + completion_tokens * output_per_mtok) / 1e6

CREATE TABLE IF NOT EXISTS llm_pricing (
    model                    VARCHAR PRIMARY KEY,
    input_per_mtok           FLOAT NOT NULL,
    output_per_mtok          FLOAT NOT NULL,
    cache_write_5m_per_mtok  FLOAT,
    cache_write_1h_per_mtok  FLOAT,
    cache_read_per_mtok      FLOAT,
    batch_input_per_mtok     FLOAT,
    batch_output_per_mtok    FLOAT
);

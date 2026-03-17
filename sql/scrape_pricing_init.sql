-- One-shot init for the pure-SQL pricing scrape pipeline.
--
-- Run from the blobapi directory:
--   duckdb -unsigned -init sql/scrape_pricing_init.sql

-- Load extensions
LOAD '../blobhttp/build/release/extension/bhttp/bhttp.duckdb_extension';
LOAD '../blobtemplates/build/duckdb/blobtemplates.duckdb_extension';

-- Source llm_complete macro
.read ../blobhttp/sql/llm_complete.sql

-- Create adapter table and load definitions (must precede llm_adapt)
.read sql/create_llm_adapter.sql
.read sql/load_llm_adapters.sql

-- Source macros that reference llm_adapter table
.read ../blobhttp/sql/llm_adapt.sql
.read sql/scrape_pricing.sql

.mode table
.timer on

-- Scrape Anthropic pricing (with Jina table selector)
SELECT * FROM scrape_pricing('anthropic',
    'https://platform.claude.com/docs/en/about-claude/pricing',
    target_selector := 'table',
    remove_selector := 'nav, footer, .sidebar')
ORDER BY input_per_mtok;

-- Physical properties demo: query Claude for material properties via blobhttp.
--
-- Prerequisites (see docs/llm-setup.md):
--   1. Bifrost LLM gateway running on localhost:8080 (Docker)
--   2. DuckDB with -unsigned flag
--   3. bhttp and blobtemplates extensions built
--
-- Run from the blobapi directory:
--   duckdb -unsigned -init sql/llm_demo.sql
--
-- The demo asks Claude for boiling/melting points of water, ethanol, and
-- mercury. The response is schema-validated (JSON Schema Draft 2020-12)
-- and reshaped via JMESPath into a list of {substance, metric, value,
-- unit_of_measure} dicts.

-- Load extensions (bh_http_get and other macros are embedded)
LOAD '../blobhttp/build/release/extension/bhttp/bhttp.duckdb_extension';
LOAD '../blobtemplates/build/duckdb/blobtemplates.duckdb_extension';

-- Source llm_complete macro (not embedded — no table dependency)
.read ../blobhttp/sql/llm_complete.sql

-- Create adapter table and load definitions (must precede llm_adapt macro)
.read sql/create_llm_adapter.sql
.read sql/load_llm_adapters.sql

-- Source the llm_adapt macro (references llm_adapter table)
.read ../blobhttp/sql/llm_adapt.sql

-- Defaults: endpoint=http://localhost:8080/v1/chat/completions
--           model=anthropic/claude-haiku-4-5-20251001
-- Override with: SET VARIABLE llm_endpoint = '...';
--                SET VARIABLE llm_model = '...';

-- Run the query
.mode json
.timer on
SELECT * FROM llm_adapt('physical_properties',
    json_object('substances', ['water', 'ethanol', 'mercury'],
                'metrics', ['boiling point', 'melting point', 'density at 25°C']));

-- DDL for the llm_adapter table.
--
-- This table drives the llm_adapt() macro from the bhttp extension.
-- Each row is a reified LLM function: a prompt template (inja/Jinja2),
-- a JSON Schema for output validation, and a JMESPath expression to
-- reshape the validated response into the desired structure.
--
-- Must be created BEFORE sourcing llm_adapt.sql, because the macro
-- references this table and DuckDB validates table references eagerly.

CREATE TABLE IF NOT EXISTS llm_adapter (
    name              VARCHAR PRIMARY KEY,
    prompt_template   VARCHAR NOT NULL,
    output_schema     VARCHAR,
    response_jmespath VARCHAR,
    max_tokens        INTEGER DEFAULT 4096
);

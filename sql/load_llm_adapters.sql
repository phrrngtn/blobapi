-- Load LLM adapter definitions from YAML files into the llm_adapter table.
--
-- Requires: blobtemplates extension (bt_yaml_to_json), bhttp extension.
-- Assumes create_llm_adapter.sql has already been run.
--
-- Each YAML file contains a list of adapter objects with keys:
--   name, prompt_template, output_schema, response_jmespath, max_tokens

INSERT OR REPLACE INTO llm_adapter
SELECT
    j->>'$.name'              AS name,
    j->>'$.prompt_template'   AS prompt_template,
    j->'$.output_schema'      AS output_schema,
    j->>'$.response_jmespath' AS response_jmespath,
    CAST(j->>'$.max_tokens' AS INTEGER) AS max_tokens
FROM (
    SELECT unnest(from_json(
        bt_yaml_to_json(content),
        '["json"]'
    )) AS j
    FROM read_text([
        'adapters/physical_properties.yaml',
        'adapters/domain_inference.yaml',
        'adapters/llm_model_cost.yaml',
        'adapters/column_classify_and_discover.yaml'
    ])
);

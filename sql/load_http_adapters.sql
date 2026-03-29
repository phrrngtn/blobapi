-- Load HTTP adapter definitions from YAML files into the http_adapter table.
--
-- Requires: blobtemplates extension (bt_yaml_to_json).
-- Assumes create_http_adapter.sql has already been run.
--
-- Each YAML file contains a list of adapter objects with keys:
--   name, description, method, url_template, default_headers,
--   default_params, rate_limit_profile, response_jmespath,
--   response_notes, source

INSERT OR REPLACE INTO http_adapter
SELECT
    j->>'$.name'               AS name,
    j->>'$.description'        AS description,
    COALESCE(j->>'$.method', 'get') AS method,
    j->>'$.url_template'       AS url_template,
    j->'$.default_headers'     AS default_headers,
    j->'$.default_params'      AS default_params,
    j->>'$.rate_limit_profile' AS rate_limit_profile,
    j->>'$.response_jmespath'  AS response_jmespath,
    j->>'$.response_notes'     AS response_notes,
    j->>'$.source'             AS source
FROM (
    SELECT unnest(from_json(
        bt_yaml_to_json(content),
        '["json"]'
    )) AS j
    FROM read_text([
        'adapters/google.yaml'
    ])
);

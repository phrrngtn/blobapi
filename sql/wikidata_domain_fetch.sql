-- Fetch members of a Wikidata domain via SPARQL and return as a table.
--
-- Usage:
--   SELECT * FROM wikidata_domain_members('Q6256');  -- all countries
--   SELECT * FROM wikidata_domain_members('Q11344'); -- chemical elements
--
-- Returns: label (VARCHAR), alt_labels (VARCHAR[])
--
-- Requires: blobhttp extension loaded, network access to query.wikidata.org

CREATE OR REPLACE MACRO wikidata_domain_members(qid) AS TABLE (
    WITH SPARQL_QUERY AS (
        SELECT 'SELECT ?label (GROUP_CONCAT(DISTINCT ?altLabel; separator="|") AS ?altLabels) '
            || 'WHERE { '
            || '  ?item wdt:P31 wd:' || qid || '. '
            || '  ?item rdfs:label ?label. FILTER(LANG(?label) = "en") '
            || '  OPTIONAL { ?item skos:altLabel ?altLabel. FILTER(LANG(?altLabel) = "en") } '
            || '} GROUP BY ?label ORDER BY ?label'
            AS query
    ),
    FETCH AS (
        SELECT bh_http_get(
            'https://query.wikidata.org/sparql',
            headers := MAP {
                'Accept': 'application/sparql-results+json',
                'User-Agent': 'blobapi-wikidata/0.1'
            },
            params := json_object('query', (SELECT query FROM SPARQL_QUERY))
        ).response_body AS body
    ),
    PARSED AS (
        SELECT unnest(from_json(body::JSON->'results'->'bindings', '["json"]')) AS binding
        FROM FETCH
    )
    SELECT binding->'label'->>'value' AS label,
           CASE WHEN binding->'altLabels'->>'value' != ''
                THEN string_split(binding->'altLabels'->>'value', '|')
                ELSE []::VARCHAR[]
           END AS alt_labels
    FROM PARSED
);

-- Convenience: fetch a domain and immediately build a blobfilter from all labels
CREATE OR REPLACE MACRO wikidata_domain_filter(qid) AS (
    WITH MEMBERS AS (
        SELECT * FROM wikidata_domain_members(qid)
    ),
    ALL_LABELS AS (
        SELECT lower(label) AS term FROM MEMBERS
        UNION
        SELECT lower(unnest(alt_labels)) FROM MEMBERS
    )
    SELECT bf_build_json(json_group_array(term)) AS filter
    FROM ALL_LABELS
);

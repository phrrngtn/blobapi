-- classify_and_bootstrap: the full self-bootstrapping classification loop.
--
-- Given a table's column names (and optionally sample data):
--   1. Probe against existing blobfilters (Layer 1: deterministic)
--   2. Send unmatched columns to LLM via column_classify_and_discover adapter
--   3. For each new Wikidata domain suggested by the LLM:
--      a. Fetch members via SPARQL
--      b. Build a blobfilter
--      c. Store in domain_enumerations for future use
--   4. Return the full classification result
--
-- This script demonstrates the loop. In production, steps 1-3 would be
-- a single macro call that returns the classification and has the side
-- effect of expanding the vocabulary.
--
-- Prerequisites:
--   LOAD blobhttp, blobtemplates, blobfilters, blobembed extensions
--   .read sql/create_llm_adapter.sql
--   .read sql/load_llm_adapters.sql
--   .read ../blobhttp/sql/llm_adapt.sql
--   .read sql/wikidata_domain_fetch.sql

-- ═══════════════════════════════════════════════════════════════════
-- STEP 1: Existing vocabulary — what do we already know?
-- ═══════════════════════════════════════════════════════════════════

-- Assumes domain_enumerations table exists from a prior load.
-- If not, this is the bootstrap case — all columns go to the LLM.

-- ═══════════════════════════════════════════════════════════════════
-- STEP 2: Classify via LLM (the expensive call)
-- ═══════════════════════════════════════════════════════════════════

-- Example: classify the NYC Elder Abuse table
-- In practice, the table_name, header, data_types, and body would
-- come from a Rule4 scrape.

-- SET VARIABLE target_table = 'Intimate Partner Elder Abuse in New York City';

-- SELECT result::JSON AS classification
-- FROM llm_adapt('column_classify_and_discover',
--     json_object(
--         'table_name', getvariable('target_table'),
--         'header', (SELECT json_group_array(field_name)
--                    FROM pg.resource_column
--                    WHERE domain = 'data.cityofnewyork.us'
--                      AND resource_id = 's67q-ee5u'
--                      AND tt_end = '9999-12-31'
--                    ORDER BY ordinal_position),
--         'data_types', (SELECT json_group_array(data_type)
--                        FROM pg.resource_column
--                        WHERE domain = 'data.cityofnewyork.us'
--                          AND resource_id = 's67q-ee5u'
--                          AND tt_end = '9999-12-31'
--                        ORDER BY ordinal_position),
--         'body', '[]',
--         'known_domains', (SELECT json_group_array(DISTINCT domain_label)
--                           FROM domain_enumerations)
--     )
-- );

-- ═══════════════════════════════════════════════════════════════════
-- STEP 3: For each new Wikidata domain, fetch and build filters
-- ═══════════════════════════════════════════════════════════════════

-- Parse the wikidata_domains array from the LLM response:
--
-- WITH NEW_DOMAINS AS (
--     SELECT j->>'wikidata_qid' AS qid,
--            j->>'domain_label' AS domain_label,
--            j->>'sparql_hint' AS sparql_hint
--     FROM (
--         SELECT unnest(from_json(
--             classification->'wikidata_domains', '["json"]'
--         )) AS j
--         FROM llm_result
--     )
-- )
-- For each new domain, fetch members and build a filter:
--
-- INSERT INTO domain_enumerations (domain_name, domain_label, source, filter)
-- SELECT nd.domain_label,
--        nd.domain_label,
--        'wikidata:' || nd.qid,
--        (SELECT * FROM wikidata_domain_filter(nd.qid))
-- FROM NEW_DOMAINS AS nd
-- WHERE nd.domain_label NOT IN (
--     SELECT domain_label FROM domain_enumerations
-- );

-- ═══════════════════════════════════════════════════════════════════
-- The above is commented because it requires a running Bifrost
-- gateway and network access. The pattern is:
--
--   1. llm_adapt() → structured JSON with wikidata_domains[]
--   2. For each QID: wikidata_domain_members(qid) → labels
--   3. bf_build_json() → blobfilter BLOB
--   4. INSERT INTO domain_enumerations
--   5. Next time: Layer 1 catches these columns deterministically
-- ═══════════════════════════════════════════════════════════════════

.print 'classify_and_bootstrap.sql loaded.'
.print 'The full loop requires Bifrost + network access.'
.print 'See comments in this file for the orchestration pattern.'

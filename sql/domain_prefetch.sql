-- Prefetch domain enumerations from PG into local DuckDB tables.
--
-- Call once at session start, before running classification queries.
-- Creates local tables: domain_enumerations, domain_members, domain_filters.
--
-- Prerequisites:
--   INSTALL postgres; LOAD postgres;
--   ATTACH 'dbname=rule4_test ...' AS pg (TYPE POSTGRES);
--   LOAD blobfilters extension

-- ── Materialize domain definitions ─────────────────────────────────
CREATE OR REPLACE TABLE domain_enumerations AS
SELECT domain_name, domain_label, source, wikidata_qid,
       member_count, filter_b64,
       bf_from_base64(filter_b64) AS filter
FROM pg.domain.enumeration;

-- ── Materialize domain members ─────────────────────────────────────
CREATE OR REPLACE TABLE domain_members AS
SELECT domain_name, label, alt_labels
FROM pg.domain.member;

.print 'Domain prefetch complete.'
SELECT count(*) AS n_domains FROM domain_enumerations;
SELECT count(*) AS n_members FROM domain_members;

-- ── Convenience: probe a table's columns against all domains ───────
-- Returns domains with containment > 0, sorted by match quality.
CREATE OR REPLACE MACRO probe_columns_against_domains(columns_json) AS TABLE (
    SELECT de.domain_name, de.domain_label,
           bf_containment_json(columns_json, de.filter) AS containment,
           bf_intersection_card(bf_build_json(columns_json), de.filter) AS n_matches,
           de.member_count
    FROM domain_enumerations AS de
    WHERE de.filter IS NOT NULL
      AND bf_containment_json(columns_json, de.filter) > 0
    ORDER BY n_matches DESC, containment DESC
);

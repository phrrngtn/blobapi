-- Write back newly discovered domains to PG.
--
-- After the LLM suggests Wikidata domains and we fetch + validate them,
-- this writes the results to the PG system of record.
--
-- Expects a temp table `new_domain_members` with:
--   domain_name, domain_label, source, wikidata_qid, label, alt_labels
--
-- Prerequisites:
--   PG attached as pg, blobfilters extension loaded

-- ── Insert domain definition ───────────────────────────────────────
INSERT INTO pg.domain.enumeration (domain_name, domain_label, source, wikidata_qid, member_count, filter_b64)
SELECT DISTINCT
    ndm.domain_name,
    ndm.domain_label,
    ndm.source,
    ndm.wikidata_qid,
    (SELECT count(*) FROM new_domain_members AS n2 WHERE n2.domain_name = ndm.domain_name),
    -- Build filter from all labels + alt_labels, encode as base64 for portability
    (WITH ALL_TERMS AS (
        SELECT lower(n3.label) AS term FROM new_domain_members AS n3 WHERE n3.domain_name = ndm.domain_name
        UNION
        SELECT lower(unnest(n3.alt_labels)) FROM new_domain_members AS n3 WHERE n3.domain_name = ndm.domain_name
    )
    SELECT bf_to_base64(bf_build_json(json_group_array(term))) FROM ALL_TERMS)
FROM new_domain_members AS ndm
ON CONFLICT (domain_name) DO UPDATE SET
    member_count = EXCLUDED.member_count,
    filter_b64 = EXCLUDED.filter_b64,
    updated_at = now();

-- ── Insert members ─────────────────────────────────────────────────
INSERT INTO pg.domain.member (domain_name, label, alt_labels)
SELECT domain_name, label, alt_labels
FROM new_domain_members
ON CONFLICT (domain_name, label) DO UPDATE SET
    alt_labels = EXCLUDED.alt_labels;

-- ── Refresh local materialized copy ────────────────────────────────
.read /Users/paulharrington/checkouts/blobapi/sql/domain_prefetch.sql

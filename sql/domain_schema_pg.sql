-- Domain enumerations schema for PostgreSQL (rule4_test).
--
-- Stores reference domain member lists (from Wikidata + curated sources)
-- and their precomputed blobfilters.
--
-- This is the system of record. DuckDB materializes a local copy at
-- session start for fast classification queries.

CREATE SCHEMA IF NOT EXISTS domain;

-- ── Domain definitions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS domain.enumeration (
    domain_name     TEXT PRIMARY KEY,
    domain_label    TEXT NOT NULL,
    source          TEXT NOT NULL,       -- 'wikidata:Q6256', 'curated', 'curated:us_census', 'llm_discovered'
    wikidata_qid    TEXT,                -- NULL for curated domains
    member_count    INTEGER NOT NULL DEFAULT 0,
    filter_b64      TEXT,                -- base64-encoded blobfilter (portable across PG/DuckDB/SQLite)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Domain members (the extensional representation) ────────────────
CREATE TABLE IF NOT EXISTS domain.member (
    domain_name     TEXT NOT NULL REFERENCES domain.enumeration(domain_name),
    label           TEXT NOT NULL,
    alt_labels      TEXT[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (domain_name, label)
);

CREATE INDEX IF NOT EXISTS idx_member_label ON domain.member (lower(label));

-- ── Discovery log: track what the LLM suggested and what we fetched ─
CREATE TABLE IF NOT EXISTS domain.discovery_log (
    id              SERIAL PRIMARY KEY,
    discovered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_table    TEXT,                -- e.g. 'data.cityofchicago.org/gy5m-7w2w'
    llm_model       TEXT,
    suggested_qid   TEXT,
    suggested_label TEXT,
    reason          TEXT,
    fetch_status    TEXT,                -- 'fetched', 'invalid_qid', 'no_match', 'pending'
    match_score     FLOAT,              -- how many fetched labels matched column names
    accepted        BOOLEAN DEFAULT FALSE
);

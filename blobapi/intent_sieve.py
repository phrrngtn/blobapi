"""Intent-driven bbox sieve: natural language → constrained classification.

Connects the intent expansion (LLM → IntentConstraints) with the bbox
sieve (blobfilter probing on document cells). The intent constrains
which domain filters to load and which embedding collection to search.

Usage:
    from blobapi.intent_sieve import IntentSieve

    sieve = IntentSieve(engine, duck)
    results = sieve.run(
        filepath="claims_bordereaux.pdf",
        intent="property cat claims, Q3 2025, US hurricanes",
    )
    # results.resolved_cells — cells classified without embeddings
    # results.unresolved_cells — cells that need embedding lookup
    # results.constraints — the LLM-expanded constraints
    # results.domain_matches — which domains matched which cells
"""
import json
import time
import logging
import subprocess
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

# ── Intent constraints ───────────────────────────────────────────

@dataclass
class IntentConstraints:
    """Structured constraints from natural language intent."""
    peril_types: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    domains: list[str] = field(default_factory=list)
    expected_columns: list[str] = field(default_factory=list)
    description: Optional[str] = None


KNOWN_PERIL_TYPES = [
    "Earthquake", "Tropical Cyclone", "Flood", "Volcanic Eruption",
    "Wildfire", "Tsunami", "Tornado",
]

EXPAND_PROMPT = """You are a structured data extraction tool for a catastrophe reinsurance system.

Given a natural language description of a document or query context, extract structured constraints.

Available peril types: {peril_types}
Available domains (for blobfilter loading): {domains}

Return ONLY a JSON object with these fields (omit empty/null fields):
{{
  "peril_types": ["list of matching peril types"],
  "countries": ["list of country names"],
  "regions": ["list of region/state names"],
  "date_from": "YYYY-MM-DD or null",
  "date_to": "YYYY-MM-DD or null",
  "domains": ["list of relevant domain names to load"],
  "expected_columns": ["list of likely column names in the document"],
  "description": "one-line summary"
}}

User intent: {intent}
"""


def expand_intent(intent: str, available_domains: list[str] | None = None) -> IntentConstraints:
    """Expand natural language intent into structured constraints via LLM."""
    domains_str = ", ".join(available_domains) if available_domains else "all available"
    prompt = EXPAND_PROMPT.format(
        peril_types=", ".join(KNOWN_PERIL_TYPES),
        domains=domains_str,
        intent=intent,
    )

    result = subprocess.run(
        ["claude", "-p", prompt, "--output-format", "json"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"LLM failed: {result.stderr}")

    text = json.loads(result.stdout).get("result", "")
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]

    data = json.loads(text.strip())
    return IntentConstraints(**{k: v for k, v in data.items()
                                if k in IntentConstraints.__dataclass_fields__})


# ── Sieve results ────────────────────────────────────────────────

@dataclass
class SieveResults:
    """Results from the intent-driven sieve."""
    constraints: IntentConstraints
    total_cells: int = 0
    resolved_cells: int = 0
    unresolved_cells: int = 0
    resolution_pct: float = 0.0
    role_counts: dict = field(default_factory=dict)
    domain_matches: dict = field(default_factory=dict)
    sieve_time_ms: float = 0.0
    n_domains_loaded: int = 0


# ── Intent-driven sieve ─────────────────────────────────────────

class IntentSieve:
    """Run the bbox sieve with intent-driven domain selection."""

    def __init__(self, duck):
        """
        Args:
            duck: duckdb connection with bboxes, blobfilters, postgres loaded.
                  Filters should NOT be pre-loaded — this class loads only
                  the domains specified by the intent constraints.
        """
        self.duck = duck

    def _load_filters(self, domain_names: list[str] | None = None):
        """Load domain filters from PG, optionally filtered by name."""
        self.duck.execute("DROP TABLE IF EXISTS _sieve_filters")
        if domain_names:
            placeholders = ", ".join(f"'{d}'" for d in domain_names)
            self.duck.execute(f"""
                CREATE TEMP TABLE _sieve_filters AS
                SELECT domain_name, bf_from_base64(filter_b64) AS filter_bitmap
                FROM pg.domain.enumeration
                WHERE filter_b64 IS NOT NULL
                  AND domain_name IN ({placeholders})
            """)
        else:
            self.duck.execute("""
                CREATE TEMP TABLE _sieve_filters AS
                SELECT domain_name, bf_from_base64(filter_b64) AS filter_bitmap
                FROM pg.domain.enumeration
                WHERE filter_b64 IS NOT NULL
            """)
        n = self.duck.execute("SELECT count(*) FROM _sieve_filters").fetchone()[0]
        return n

    def run(self, filepath: str, intent: str | None = None,
            constraints: IntentConstraints | None = None) -> SieveResults:
        """Run the sieve on a document with optional intent constraints.

        Either provide `intent` (natural language, LLM-expanded) or
        `constraints` (pre-built). If neither, runs with all domains.

        Args:
            filepath: path to PDF/XLSX document
            intent: natural language intent string
            constraints: pre-built IntentConstraints (skips LLM call)
        """
        t0 = time.perf_counter()

        # Expand intent if needed
        if constraints is None and intent:
            available = [r[0] for r in self.duck.execute(
                "SELECT domain_name FROM pg.domain.enumeration WHERE filter_b64 IS NOT NULL"
            ).fetchall()]
            constraints = expand_intent(intent, available)
            log.info(f"Intent: {constraints.description}")
            log.info(f"  Domains: {constraints.domains}")
            log.info(f"  Expected columns: {constraints.expected_columns}")
        elif constraints is None:
            constraints = IntentConstraints()

        # Load only the requested domain filters
        if constraints.domains:
            n_filters = self._load_filters(constraints.domains)
        else:
            n_filters = self._load_filters()
        log.info(f"  Loaded {n_filters} domain filters")

        # Run the sieve SQL
        sieve_df = self.duck.execute(f"""
            WITH
            RAW AS (
                SELECT b.page_id, b.style_id, b.x, b.y, b.w, b.h, b.text, s.weight
                FROM bb('{filepath}') AS b
                JOIN bb_styles('{filepath}') AS s USING (style_id)
            ),
            ROWS AS (
                SELECT *, DENSE_RANK() OVER (
                    ORDER BY page_id, round((y + h/2) / GREATEST(h, 1.0))
                ) AS rc FROM RAW
            ),
            IS_GRID AS (
                SELECT CASE WHEN COUNT(DISTINCT w) = 1 AND MIN(w) = 1.0
                            THEN true ELSE false END AS grid_mode
                FROM ROWS
            ),
            GAPS AS (
                SELECT ROWS.*,
                       x - LAG(x + w) OVER (PARTITION BY page_id, rc ORDER BY x) AS gap,
                       LAG(h) OVER (PARTITION BY page_id, rc ORDER BY x) AS ph,
                       grid_mode
                FROM ROWS, IS_GRID
            ),
            CELLS AS (
                SELECT *,
                       CASE WHEN grid_mode
                            THEN ROW_NUMBER() OVER (PARTITION BY page_id, rc ORDER BY x)
                            ELSE SUM(CASE WHEN gap IS NULL OR gap > GREATEST(ph * 0.8, 3.0)
                                          THEN 1 ELSE 0 END)
                                     OVER (PARTITION BY page_id, rc ORDER BY x)
                       END AS cid
                FROM GAPS
            ),
            MERGED AS (
                SELECT page_id, rc, cid,
                       STRING_AGG(text, ' ' ORDER BY x) AS text,
                       MODE(weight) AS weight
                FROM CELLS GROUP BY page_id, rc, cid
            ),
            ROW_STATS AS (
                SELECT rc,
                       SUM(CASE WHEN TRY_CAST(REPLACE(REPLACE(text, ',', ''), '$', '') AS DOUBLE) IS NOT NULL
                                THEN 1 ELSE 0 END) AS n_num,
                       COUNT(*) AS n_cells
                FROM MERGED GROUP BY rc
            ),
            BODY_START AS (
                SELECT COALESCE(MIN(rc), 1) AS fdr
                FROM ROW_STATS WHERE n_num > 0 AND n_cells >= 2
            ),
            TYPED AS (
                SELECT m.*, m.rc < bs.fdr AS is_pre,
                       TRY_CAST(REPLACE(REPLACE(m.text, ',', ''), '$', '') AS DOUBLE) IS NOT NULL AS is_num,
                       CASE
                           WHEN regexp_matches(m.text, '^[\d,.]+$') THEN 'number'
                           WHEN regexp_matches(m.text, '^\d{{4}}-\d{{2}}-\d{{2}}') THEN 'iso_date'
                           ELSE NULL
                       END AS regex_d
                FROM MERGED AS m, BODY_START AS bs
            ),
            UNRESOLVED AS (
                SELECT * FROM TYPED
                WHERE NOT is_pre AND NOT is_num AND regex_d IS NULL AND LENGTH(text) > 3
            ),
            PROBES AS (
                SELECT u.rc, u.cid, u.text, df.domain_name,
                       bf_containment_json_normalized(u.text, df.filter_bitmap) AS score
                FROM UNRESOLVED AS u CROSS JOIN _sieve_filters AS df
            ),
            BEST AS (
                SELECT rc, cid, text,
                       FIRST(domain_name ORDER BY score DESC) AS dom,
                       MAX(score) AS best
                FROM PROBES WHERE score > 0 GROUP BY rc, cid, text
            ),
            CLASSIFIED AS (
                SELECT t.text,
                       CASE
                           WHEN t.is_pre THEN 'header'
                           WHEN t.regex_d IS NOT NULL THEN t.regex_d
                           WHEN t.is_num THEN 'numeric'
                           WHEN b.best = 1.0 THEN 'domain:' || b.dom
                           WHEN LENGTH(t.text) <= 3 THEN 'code'
                           ELSE 'text'
                       END AS role,
                       b.dom AS matched_domain
                FROM TYPED AS t
                LEFT JOIN BEST AS b ON t.rc = b.rc AND t.cid = b.cid
            )
            SELECT role, COUNT(*) AS n,
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
            FROM CLASSIFIED GROUP BY role ORDER BY n DESC
        """).df()

        elapsed = (time.perf_counter() - t0) * 1000

        total = int(sieve_df["n"].sum())
        resolved = int(sieve_df[sieve_df["role"] != "text"]["n"].sum())

        results = SieveResults(
            constraints=constraints,
            total_cells=total,
            resolved_cells=resolved,
            unresolved_cells=total - resolved,
            resolution_pct=100 * resolved / total if total else 0,
            role_counts={r["role"]: int(r["n"]) for _, r in sieve_df.iterrows()},
            sieve_time_ms=elapsed,
            n_domains_loaded=n_filters,
        )

        log.info(f"  Sieve: {total} cells, {resolved}/{total} resolved "
                 f"({results.resolution_pct:.0f}%) in {elapsed:.0f}ms "
                 f"with {n_filters} domains")

        return results

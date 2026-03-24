"""Intent-to-constraints: natural language → structured filter data → SQLAlchemy query.

The LLM converts a natural language intent into structured constraint data.
SQLAlchemy converts the constraint data into parameterized SQL.
The LLM never sees or produces SQL text.

Usage:
    constraints = expand_intent("property cat claims, Q3 2025, US hurricanes")
    embeddings = query_disaster_embeddings(session, constraints)
    domains = constraints.domains  # which blobfilter domains to load
"""
import json
import subprocess
from dataclasses import dataclass, field, asdict
from typing import Optional

from sqlalchemy import create_engine, select, REAL, ARRAY
from sqlalchemy import String, Text, Integer, Float, Column
from sqlalchemy.orm import DeclarativeBase, Session, Mapped, mapped_column


# ── Constraint model ───────────────────────────────────────────────
# Each field is an AND clause.  Multiple values within a field are OR (IN).
# No cross-field OR.  If the user's intent requires it, the LLM returns
# multiple constraint sets and the caller UNIONs the results.

@dataclass
class IntentConstraints:
    """Structured constraints derived from a natural language intent.

    Fields with empty lists or None are unconstrained (no filter).
    Non-empty fields are ANDed together.
    Multiple values in a list field are ORed (SQL IN).
    """
    peril_types: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    regions: list[str] = field(default_factory=list)
    date_from: Optional[str] = None   # YYYY-MM-DD, >= comparison
    date_to: Optional[str] = None     # YYYY-MM-DD, <= comparison
    domains: list[str] = field(default_factory=list)
    expected_columns: list[str] = field(default_factory=list)
    description: Optional[str] = None  # LLM's one-line summary of the intent


# ── SQLAlchemy model for disaster embeddings ───────────────────────

class Base(DeclarativeBase):
    pass

class DisasterEventEmbedding(Base):
    __tablename__ = "disaster_event_embedding"
    __table_args__ = {"schema": "domain"}

    hierarchical_path: Mapped[str] = mapped_column(Text, primary_key=True)
    event_name: Mapped[str] = mapped_column(Text)
    peril_type: Mapped[str] = mapped_column(String(100))
    country: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    event_date: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    embedding = Column(ARRAY(REAL))


# ── Build SQLAlchemy query from constraints ────────────────────────

def build_embedding_query(constraints: IntentConstraints):
    """Convert IntentConstraints to a SQLAlchemy SELECT.

    Each non-empty constraint becomes a WHERE clause.  All clauses are
    ANDed.  The result is a parameterized query — no string interpolation.

    Returns a SQLAlchemy Select object (not executed yet).
    """
    query = select(DisasterEventEmbedding)

    if constraints.peril_types:
        query = query.where(
            DisasterEventEmbedding.peril_type.in_(constraints.peril_types)
        )

    if constraints.countries:
        query = query.where(
            DisasterEventEmbedding.country.in_(constraints.countries)
        )

    if constraints.regions:
        # Region is embedded in hierarchical_path, not a separate column.
        # Use LIKE for substring matching.
        from sqlalchemy import or_
        region_filters = [
            DisasterEventEmbedding.hierarchical_path.contains(r)
            for r in constraints.regions
        ]
        query = query.where(or_(*region_filters))

    if constraints.date_from:
        query = query.where(
            DisasterEventEmbedding.event_date >= constraints.date_from
        )

    if constraints.date_to:
        query = query.where(
            DisasterEventEmbedding.event_date <= constraints.date_to
        )

    return query


# ── LLM intent expansion ──────────────────────────────────────────

KNOWN_PERIL_TYPES = [
    "Earthquake", "Tropical Cyclone", "Flood", "Volcanic Eruption",
    "Wildfire", "Tsunami", "Tornado",
]

EXPAND_PROMPT_TEMPLATE = """You are a structured data extraction tool for a catastrophe reinsurance system.

Given a natural language description of a document or query context, extract structured constraints.

Available peril types: {peril_types}
Available domains (for blobfilter loading): insurance_reinsurance, disasters_enriched,
  countries, world_cities, us_states, currencies, financial_metrics, economic_indicators,
  naics_codes, public_company_names, insurance_companies, airports, admin_regions

Return ONLY a JSON object with these fields (omit empty/null fields):
{{
  "peril_types": ["list of matching peril types from the available list"],
  "countries": ["list of country names"],
  "regions": ["list of region/state names"],
  "date_from": "YYYY-MM-DD or null",
  "date_to": "YYYY-MM-DD or null",
  "domains": ["list of relevant domain names to load"],
  "expected_columns": ["list of likely column names in the document"],
  "description": "one-line summary of what the user is looking for"
}}

User intent: {intent}
"""


def expand_intent(intent: str) -> IntentConstraints:
    """Call LLM to expand a natural language intent into structured constraints.

    Uses Claude CLI for the LLM call.  Returns IntentConstraints dataclass.
    """
    prompt = EXPAND_PROMPT_TEMPLATE.format(
        peril_types=", ".join(KNOWN_PERIL_TYPES),
        intent=intent,
    )

    result = subprocess.run(
        ["claude", "-p", prompt, "--output-format", "json"],
        capture_output=True, text=True, timeout=30,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Claude CLI failed: {result.stderr}")

    response = json.loads(result.stdout)
    text = response.get("result", "")

    # Extract JSON from the response (may have markdown fences)
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]

    data = json.loads(text.strip())

    return IntentConstraints(
        peril_types=data.get("peril_types", []),
        countries=data.get("countries", []),
        regions=data.get("regions", []),
        date_from=data.get("date_from"),
        date_to=data.get("date_to"),
        domains=data.get("domains", []),
        expected_columns=data.get("expected_columns", []),
        description=data.get("description"),
    )


# ── Demo ──────────────────────────────────────────────────────────

def main():
    engine = create_engine(
        "postgresql+psycopg2:///rule4_test",
        connect_args={"host": "/tmp"},
    )

    intents = [
        "property cat claims from Q3 2025, US hurricanes",
        "Japanese earthquake losses, 2011",
        "California wildfire claims 2020, property damage",
        "Bangladesh flood exposure, recent years",
    ]

    for intent in intents:
        print(f"\n{'='*60}")
        print(f"  Intent: \"{intent}\"")
        print(f"{'='*60}")

        try:
            constraints = expand_intent(intent)
        except Exception as e:
            print(f"  LLM error: {e}")
            # Fallback: manual constraints for demo
            if "hurricane" in intent.lower():
                constraints = IntentConstraints(
                    peril_types=["Tropical Cyclone"],
                    countries=["United States"],
                    date_from="2025-07-01", date_to="2025-09-30",
                    domains=["insurance_reinsurance", "disasters_enriched", "us_states", "currencies"],
                    expected_columns=["event_name", "date_of_loss", "state", "gross_loss", "net_loss"],
                    description="US hurricane property cat claims Q3 2025",
                )
            elif "japan" in intent.lower():
                constraints = IntentConstraints(
                    peril_types=["Earthquake", "Tsunami"],
                    countries=["Japan"],
                    date_from="2011-01-01", date_to="2011-12-31",
                    domains=["disasters_enriched", "countries", "currencies"],
                    description="Japanese earthquake/tsunami losses 2011",
                )
            elif "wildfire" in intent.lower():
                constraints = IntentConstraints(
                    peril_types=["Wildfire"],
                    countries=["United States"],
                    regions=["California"],
                    date_from="2020-01-01", date_to="2020-12-31",
                    domains=["disasters_enriched", "us_states", "currencies"],
                    description="California wildfire property claims 2020",
                )
            elif "flood" in intent.lower():
                constraints = IntentConstraints(
                    peril_types=["Flood"],
                    countries=["Bangladesh"],
                    date_from="2020-01-01",
                    domains=["disasters_enriched", "countries", "currencies"],
                    description="Bangladesh flood exposure recent years",
                )
            else:
                constraints = IntentConstraints()

        print(f"\n  Constraints:")
        print(f"    description:      {constraints.description}")
        print(f"    peril_types:      {constraints.peril_types}")
        print(f"    countries:        {constraints.countries}")
        print(f"    regions:          {constraints.regions}")
        print(f"    date_from:        {constraints.date_from}")
        print(f"    date_to:          {constraints.date_to}")
        print(f"    domains:          {constraints.domains}")
        print(f"    expected_columns: {constraints.expected_columns}")

        # Build and execute the filtered query
        query = build_embedding_query(constraints)

        with Session(engine) as session:
            results = session.execute(query).scalars().all()
            print(f"\n  Matching disaster events: {len(results)}")
            for r in results[:5]:
                print(f"    {r.event_name:<45} {r.peril_type:>18} "
                      f"{r.country or '':>15} {r.event_date or '':>10}")
            if len(results) > 5:
                print(f"    ... ({len(results) - 5} more)")

        # Show what the sieve would do with these constraints
        print(f"\n  Sieve configuration:")
        print(f"    Load {len(constraints.domains)} domain filters: {', '.join(constraints.domains)}")
        print(f"    Search {len(results)} embeddings (vs 4,395 unfiltered)")
        if results:
            reduction = (1 - len(results) / 4395) * 100
            print(f"    Embedding search space reduced by {reduction:.0f}%")


if __name__ == "__main__":
    main()

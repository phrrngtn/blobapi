"""
HTTP adapter model and registration for data-fetch endpoints.

Extends the blobapi adapter pattern to simple REST endpoints (EDGAR,
Wikidata SPARQL, etc.) that don't need LLM prompt templates or full
OpenAPI specs — just a URL template, rate-limit config, headers, and
a response JMESPath expression.

This sits between LlmAdapter (prompt template + JSON Schema validation)
and ApiAdapter (full OpenAPI-derived call/response JMESPath). It's for
endpoints where you know the shape and just want to declare it.

Usage:
    uv run python experiments/http_adapter.py

Creates the http_adapter table in PG and registers EDGAR + Wikidata
endpoints. The adapters are then queryable from DuckDB via the
blobhttp macros.
"""

from sqlalchemy import String, Text, Integer, JSON, create_engine, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session


class Base(DeclarativeBase):
    pass


class HttpAdapter(Base):
    """
    Reified HTTP data-fetch endpoint.

    Each row declares a REST endpoint that can be called from SQL via
    blobhttp macros.  The adapter captures everything needed to make the
    call and reshape the response — no imperative code required.

    Fields:
        name:               Unique adapter name (used in SQL: http_fetch('edgar_tickers'))
        description:        Human-readable purpose
        method:             HTTP method (get, post)
        url_template:       URL with {param} placeholders (inja/Jinja2 syntax)
        default_headers:    JSON object of default headers (User-Agent, Accept, etc.)
        default_params:     JSON object of default query parameters
        rate_limit_profile: Rate limit string for bh_http_config ("10/s", "5/m", etc.)
        response_jmespath:  JMESPath expression to reshape response body into rows
        response_notes:     Human-readable description of the response shape
        source:             Provenance (who created this adapter)
    """

    __tablename__ = "http_adapter"
    __table_args__ = {"schema": "domain"}

    name: Mapped[str] = mapped_column(String(200), primary_key=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    method: Mapped[str] = mapped_column(String(10), default="get")
    url_template: Mapped[str] = mapped_column(Text)
    default_headers: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        doc="Default HTTP headers as JSON object",
    )
    default_params: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        doc="Default query parameters as JSON object",
    )
    rate_limit_profile: Mapped[str | None] = mapped_column(
        String(50), nullable=True,
        doc="Rate limit string for bh_http_config, e.g. '10/s'",
    )
    response_jmespath: Mapped[str | None] = mapped_column(
        Text, nullable=True,
        doc="JMESPath to reshape response body into rows",
    )
    response_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str | None] = mapped_column(String(200), nullable=True)


# ── Adapter definitions ────────────────────────────────────────────

SEC_HEADERS = {
    "User-Agent": "blobboxes-domain-builder/0.1 phrrngtn@panix.com",
    "Accept": "application/json",
}

WIKIDATA_HEADERS = {
    "User-Agent": "blobboxes-domain-builder/0.1 (https://github.com/phrrngtn/blobboxes)",
    "Accept": "application/sparql-results+json",
}

ADAPTERS = [
    HttpAdapter(
        name="edgar_company_tickers",
        description="All SEC-registered companies: CIK, ticker symbol, company name",
        method="get",
        url_template="https://www.sec.gov/files/company_tickers.json",
        default_headers=SEC_HEADERS,
        rate_limit_profile="10/s",
        response_jmespath="values(@)[].{cik: cik_str, ticker: ticker, name: title}",
        response_notes="Returns ~10K companies. JSON is {0: {cik_str, ticker, title}, 1: {...}, ...}",
        source="sec:edgar",
    ),
    HttpAdapter(
        name="edgar_submission",
        description="Company details by CIK: name, SIC, tickers, state, fiscal year",
        method="get",
        url_template="https://data.sec.gov/submissions/CIK{{ cik_padded }}.json",
        default_headers=SEC_HEADERS,
        rate_limit_profile="10/s",
        response_jmespath="{cik: cik, name: name, sic: sic, sic_description: sicDescription, "
                          "state: stateOfIncorporation, fiscal_year_end: fiscalYearEnd, "
                          "category: category, tickers: tickers, exchanges: exchanges}",
        response_notes="cik_padded must be 10-digit zero-padded CIK",
        source="sec:edgar",
    ),
    HttpAdapter(
        name="wikidata_sparql",
        description="Execute a SPARQL query against Wikidata and return label + altLabel rows",
        method="get",
        url_template="https://query.wikidata.org/sparql",
        default_headers=WIKIDATA_HEADERS,
        default_params={"format": "json"},
        rate_limit_profile="5/s",
        response_jmespath="results.bindings[].{label: label.value, alt_label: altLabel.value, "
                          "item_uri: item.value}",
        response_notes="Pass SPARQL query as 'query' parameter. Returns English labels + alt labels.",
        source="wikidata:sparql",
    ),
    HttpAdapter(
        name="wikidata_domain_members",
        description="Fetch domain members by Wikidata class QID (e.g., Q515 for cities)",
        method="get",
        url_template="https://query.wikidata.org/sparql",
        default_headers=WIKIDATA_HEADERS,
        default_params={"format": "json"},
        rate_limit_profile="5/s",
        response_jmespath="results.bindings[].{label: label.value, alt_label: altLabel.value}",
        response_notes="Use with a SPARQL query parameterized by QID. "
                       "The query template is in the caller, not here — this adapter "
                       "handles the HTTP mechanics and rate limiting.",
        source="wikidata:sparql",
    ),
]


def register_adapters(engine, adapters=None):
    """Register HTTP adapters in PG. Idempotent (uses merge).

    Args:
        engine: SQLAlchemy engine
        adapters: list of HttpAdapter instances (default: built-in ADAPTERS)
    """
    if adapters is None:
        adapters = ADAPTERS

    HttpAdapter.__table__.create(engine, checkfirst=True)

    with Session(engine) as session:
        for adapter in adapters:
            session.merge(adapter)
        session.commit()


if __name__ == "__main__":
    from sqlalchemy import create_engine
    engine = create_engine("postgresql+psycopg2:///rule4_test",
                           connect_args={"host": "/tmp"})
    register_adapters(engine)

    with Session(engine) as session:
        for a in session.query(HttpAdapter).all():
            print(f"  {a.name:<30} {a.method:>6} {a.rate_limit_profile or 'none':>6}")

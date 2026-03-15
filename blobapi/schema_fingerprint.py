"""
Schema fingerprinting and similarity for table-like API responses.

Two layers:
  Layer 1 — Structural: column name/type Jaccard similarity using
            blobfilters Roaring bitmaps for fast set operations.
  Layer 2 — Semantic: domain classification of columns by name patterns,
            OpenAPI format hints, enum values, and $ref schema names.

Both layers produce fingerprint vectors that can be compared across
API endpoints, local database tables, spreadsheet columns, etc.
"""

import json
import re
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Column extraction from OpenAPI response schemas
# ---------------------------------------------------------------------------

@dataclass
class Column:
    """A column extracted from a table-like API response schema."""
    name: str
    type: str = "unknown"
    format: str = ""
    enum: list = field(default_factory=list)
    description: str = ""
    ref_name: str = ""  # $ref schema name if present
    domains: list = field(default_factory=list)  # classified domains


@dataclass
class TableSignature:
    """A table-like API endpoint's column signature."""
    provider: str
    api_name: str
    path: str
    method: str
    title: str
    columns: list[Column]
    domains: set = field(default_factory=set)  # aggregate domains


def extract_columns_from_schema(schema: dict) -> list[Column]:
    """
    Extract columns from an OpenAPI response schema.

    Handles:
      - {type: "array", items: {type: "object", properties: {...}}}
      - {type: "array", items: {$ref: "#/components/schemas/Foo"}}
      - {type: "object", properties: {...}}  (single-row table)
    """
    if not isinstance(schema, dict):
        return []

    # Unwrap array → items
    if schema.get("type") == "array":
        items = schema.get("items", {})
        if not isinstance(items, dict):
            return []
        schema = items

    # Need properties
    props = schema.get("properties")
    if not isinstance(props, dict):
        return []

    columns = []
    for name, prop_schema in props.items():
        if not isinstance(prop_schema, dict):
            columns.append(Column(name=name))
            continue

        ref = prop_schema.get("$ref", "")
        ref_name = ref.rsplit("/", 1)[-1] if ref else ""

        columns.append(Column(
            name=name,
            type=prop_schema.get("type", "unknown"),
            format=prop_schema.get("format", ""),
            enum=prop_schema.get("enum", []),
            description=prop_schema.get("description", ""),
            ref_name=ref_name,
        ))

    return columns


# ---------------------------------------------------------------------------
# Layer 2: Semantic domain classification
# ---------------------------------------------------------------------------

# Domain patterns: (domain_name, name_patterns, format_hints, type_hints)
DOMAIN_RULES = [
    # Strong format-based signals
    ("email",       [],                                      ["email"],              []),
    ("uri",         [],                                      ["uri", "url"],         []),
    ("uuid",        [],                                      ["uuid"],               []),
    ("datetime",    [],                                      ["date-time"],          []),
    ("date",        [],                                      ["date"],               []),
    ("ipv4",        [],                                      ["ipv4"],               []),
    ("ipv6",        [],                                      ["ipv6"],               []),

    # Name-based signals (checked after format)
    ("identifier",  [r"(?:^|_)id$", r"_id$", r"^uuid$", r"^guid$", r"_key$"],  [], []),
    ("currency",    [r"currency", r"^ccy$", r"^iso_?4217"],                      [], []),
    ("country",     [r"country", r"^iso_?3166", r"^nation"],                     [], []),
    ("language",    [r"language", r"^lang$", r"locale", r"^iso_?639"],           [], []),
    ("latitude",    [r"^lat(?:itude)?$"],                                         [], []),
    ("longitude",   [r"^lo?ng(?:itude)?$"],                                       [], []),
    ("geolocation", [r"geo", r"location", r"coordinates", r"^place"],            [], []),
    ("timestamp",   [r"(?:^|_)(?:at|time|timestamp)$", r"created", r"updated",
                     r"modified", r"deleted", r"expires"],                        [], []),
    ("price",       [r"price", r"cost", r"fee", r"rate", r"amount",
                     r"balance", r"total", r"charge"],                            [], []),
    ("quantity",    [r"count", r"quantity", r"qty", r"num(?:ber)?_",
                     r"^total$", r"^size$"],                                      [], ["integer"]),
    ("name",        [r"(?:^|_)name$", r"^label$", r"^title$",
                     r"display_name", r"full_?name"],                             [], []),
    ("description", [r"description", r"summary", r"comment",
                     r"note", r"body", r"text", r"message"],                      [], []),
    ("status",      [r"status", r"state", r"phase", r"stage"],                   [], []),
    ("type",        [r"(?:^|_)type$", r"(?:^|_)kind$", r"category",
                     r"^class$", r"^group$"],                                     [], []),
    ("boolean",     [r"^is_", r"^has_", r"^can_", r"^allow",
                     r"enabled", r"active", r"visible", r"deleted"],              [], ["boolean"]),
    ("url",         [r"url$", r"uri$", r"href$", r"link$",
                     r"^endpoint$", r"^image$", r"^avatar"],                      [], []),
    ("email",       [r"email", r"e_?mail"],                                       [], []),
    ("phone",       [r"phone", r"tel(?:ephone)?", r"mobile", r"fax"],            [], []),
    ("address",     [r"address", r"street", r"city", r"zip",
                     r"postal", r"^state$", r"province"],                         [], []),
    ("color",       [r"colou?r", r"^rgb$", r"^hex$"],                             [], []),
    ("version",     [r"version", r"^v\d"],                                        [], []),
    ("tag",         [r"tag", r"label", r"keyword"],                               [], []),
]


def classify_column(col: Column) -> list[str]:
    """Classify a column into semantic domains based on name, format, type."""
    domains = []
    cn = col.name.lower()

    for domain, name_pats, format_hints, type_hints in DOMAIN_RULES:
        # Check format hints first (strongest signal)
        if col.format and col.format.lower() in format_hints:
            domains.append(domain)
            continue

        # Check name patterns
        if any(re.search(pat, cn) for pat in name_pats):
            domains.append(domain)
            continue

        # Check type hints (weakest, only if combined with name)
        if type_hints and col.type in type_hints:
            # Only apply if name also partially matches
            pass

    # Enum-based classification
    if col.enum:
        enum_vals = [str(v).upper() for v in col.enum[:20]]
        # Currency codes
        if all(len(v) == 3 and v.isalpha() for v in enum_vals[:5]):
            if any(v in ("USD", "EUR", "GBP", "JPY", "CAD") for v in enum_vals):
                domains.append("currency")
            elif any(v in ("USA", "GBR", "FRA", "DEU", "JPN") for v in enum_vals):
                domains.append("country")
            elif any(v in ("ENG", "FRA", "DEU", "SPA", "JPN") for v in enum_vals):
                domains.append("language")

    # $ref-based classification
    if col.ref_name:
        rn = col.ref_name.lower()
        for domain, name_pats, _, _ in DOMAIN_RULES:
            if any(re.search(pat, rn) for pat in name_pats):
                domains.append(domain)
                break

    return list(dict.fromkeys(domains))  # dedupe preserving order


def classify_table(sig: TableSignature) -> TableSignature:
    """Classify all columns and compute aggregate table domains."""
    for col in sig.columns:
        col.domains = classify_column(col)
    sig.domains = {d for col in sig.columns for d in col.domains}
    return sig


# ---------------------------------------------------------------------------
# Layer 1: Structural similarity
# ---------------------------------------------------------------------------

def column_name_set(sig: TableSignature) -> frozenset[str]:
    """Normalized column names for set comparison."""
    return frozenset(c.name.lower().strip("_") for c in sig.columns)


def jaccard_similarity(a: frozenset, b: frozenset) -> float:
    """Jaccard similarity between two sets."""
    if not a and not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union > 0 else 0.0


def domain_similarity(a: TableSignature, b: TableSignature) -> float:
    """Jaccard similarity on classified domains."""
    if not a.domains and not b.domains:
        return 0.0
    return jaccard_similarity(frozenset(a.domains), frozenset(b.domains))


def find_similar(target: TableSignature, candidates: list[TableSignature],
                 *, min_jaccard: float = 0.2, top_k: int = 20) -> list[dict]:
    """
    Find candidate endpoints with similar column signatures.

    Returns list of {candidate, name_jaccard, domain_jaccard, matched_columns}.
    """
    target_names = column_name_set(target)
    results = []

    for cand in candidates:
        cand_names = column_name_set(cand)
        name_sim = jaccard_similarity(target_names, cand_names)
        if name_sim < min_jaccard:
            continue

        dom_sim = domain_similarity(target, cand)
        matched = target_names & cand_names

        results.append({
            "candidate": cand,
            "name_jaccard": name_sim,
            "domain_jaccard": dom_sim,
            "matched_columns": sorted(matched),
            "combined_score": 0.6 * name_sim + 0.4 * dom_sim,
        })

    results.sort(key=lambda r: r["combined_score"], reverse=True)
    return results[:top_k]


# ---------------------------------------------------------------------------
# Holistic table-level domain inference
# ---------------------------------------------------------------------------

# Coarse domain clusters — groups of fine-grained domains that
# suggest a table's *subject area* when they co-occur.
TABLE_DOMAIN_CLUSTERS = {
    "financial":     {"price", "currency", "quantity", "balance", "identifier"},
    "geospatial":    {"latitude", "longitude", "geolocation", "address", "country"},
    "user/account":  {"email", "phone", "name", "address", "identifier", "boolean"},
    "e-commerce":    {"price", "quantity", "name", "identifier", "status", "currency"},
    "infrastructure":{"url", "ipv4", "ipv6", "status", "version", "identifier"},
    "content/media": {"url", "name", "description", "tag", "type", "timestamp"},
    "event/log":     {"timestamp", "status", "type", "identifier", "description"},
    "reference/code":{"country", "language", "currency", "identifier", "name"},
}


def infer_table_domain(sig: TableSignature) -> list[tuple[str, float]]:
    """
    Infer the table's subject area from the co-occurrence of column domains.

    Returns [(domain_name, score)] sorted by score descending.
    """
    if not sig.domains:
        return []

    scores = []
    for cluster_name, cluster_domains in TABLE_DOMAIN_CLUSTERS.items():
        overlap = sig.domains & cluster_domains
        if not overlap:
            continue
        # Weighted: how much of the cluster is covered + how much
        # of the table's domains belong to this cluster
        coverage = len(overlap) / len(cluster_domains)
        precision = len(overlap) / len(sig.domains) if sig.domains else 0
        score = 0.5 * coverage + 0.5 * precision
        scores.append((cluster_name, round(score, 3)))

    scores.sort(key=lambda x: x[1], reverse=True)
    return scores

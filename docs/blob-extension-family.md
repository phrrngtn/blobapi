# The blob* Extension Family

## Overview

The blob* projects are a family of C libraries with SQLite, DuckDB, and Python
bindings. Each handles one concern — document parsing, fingerprinting, ODBC
access — and exposes it as loadable database extensions. They share a common
build pattern (CMake + FetchContent) and a common structure (`src/`, `include/`,
`sqlite_ext/`, `duckdb_ext/`, `python/`, `demo/`, `test/`).

**rule4** is the orchestration layer that ties them together: it pulls metadata
from remote databases, feeds it through the computation engines, applies
classification policy, and writes results back.

## Architecture

```
blobboxes                    blobfilters                    blobodbc
───────────                  ───────────                    ──────────
Extracts structure           Computes fingerprints          ODBC pass-through
from documents               & compares them                queries from SQLite

PDF/Excel/Word/text    ──►   Roaring bitmap engine    ◄──   Remote catalog access
  │                            │                              │
  │ bounding boxes             │ containment scores           │ histogram data
  │ (text, position,           │ shape metrics                │ column samples
  │  style)                    │ Jaccard similarity           │
  ▼                            ▼                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         rule4                                       │
│  Orchestration: schema scraping, classification policy,             │
│  extended property write-back, PK/FK catalog analysis,              │
│  schema topology time-series                                        │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         blobapi                                     │
│  HTTP layer: OpenAPI-driven adapters, connects external APIs        │
│  to the catalog and classification pipeline                         │
└─────────────────────────────────────────────────────────────────────┘
```

## Projects

### blobboxes

Extracts bounding boxes from documents (PDF, Excel, Word, plain text). Each
text fragment becomes a row: `(page_id, x, y, w, h, text, style_id, ...)`.
Exposed as a SQLite virtual table (`bboxes`) so document content is queryable
with SQL.

- **Repo**: `~/checkouts/blobboxes`
- **Produces**: Tokenized document content with spatial coordinates
- **Consumed by**: blobfilters (tokens become probe values for domain detection)

### blobfilters

Roaring bitmap fingerprint engine for domain detection and column
classification. Hashes values via FNV-1a into CRoaring bitmaps, computes
containment and Jaccard similarity against stored domain fingerprints. Also
provides histogram fingerprints with shape metrics (cardinality ratio,
repeatability, discreteness, range density).

- **Repo**: `~/checkouts/blobfilters`
- **Core deps**: CRoaring, nlohmann/json, utf8proc (Unicode normalization)
- **Produces**: Containment scores, shape metrics, histogram fingerprints (JSON)
- **Consumed by**: rule4 (applies classification policy on top of these signals)

Key capabilities:
- Exact domain matching via roaring bitmap intersection (microseconds per comparison)
- Normalized matching (NFKD + casefold) for case/accent-insensitive domain detection
- Weighted containment using frequency information from histograms or samples
- Shape similarity for clustering columns by statistical profile

### blobodbc / sqlite-embedded-odbc

ODBC pass-through queries from SQLite. Executes SQL against remote databases
via ODBC connection strings and materializes results as SQLite tables.

- **Repo**: `github.com/phrrngtn/sqlite-embedded-odbc`
- **Produces**: Remote query results as local SQLite tables
- **Consumed by**: rule4 (pulls `sys.dm_db_stats_histogram`, catalog metadata)

### blobtemplates

(Sibling project — shared CMake + FetchContent build patterns.)

- **Repo**: `~/checkouts/blobtemplates`

### rule4

Orchestration layer. Not an extension itself — it's the workflow that connects
the others. Scrapes remote database catalogs (SQL Server, PostgreSQL) via ODBC,
feeds histogram data and column samples through blobfilters, applies
classification rules (PK/FK membership, data type heuristics, containment
thresholds), and writes results back as SQL Server extended properties.

- **Repo**: rule4 schema objects
- **Consumes**: blobfilters (fingerprints, shape metrics), blobodbc (remote access)
- **Produces**: Classification labels on columns (dimension, measure, degenerate dimension)

### blobapi

HTTP/API layer. OpenAPI-driven adapters that connect external APIs to the
catalog and classification pipeline. Sits on top of the other projects.

- **Repo**: `~/checkouts/blobapi`

## Data Flow: End-to-End Column Classification

1. **Catalog scrape** (rule4 + blobodbc): Query remote database metadata —
   `sys.columns`, `sys.stats`, `sys.dm_db_stats_histogram` — via ODBC.

2. **Histogram triage** (rule4): Classify columns by histogram shape into
   candidate keys, candidate FKs, and uninteresting columns.

3. **Fingerprint build** (blobfilters): Hash candidate key distinct values into
   domain bitmaps. Hash candidate FK samples into probe bitmaps.

4. **Domain matching** (blobfilters): Compute containment of probe against all
   stored domains. Both raw (exact) and normalized (NFKD + casefold) bitmaps
   are checked.

5. **Classification** (rule4): Combine containment scores with catalog signals
   (PK/FK membership, data types) to assign labels. Write back as extended
   properties.

## Data Flow: Document Classification

1. **Extract** (blobboxes): Parse PDF/Excel/Word into bounding boxes with text
   and spatial coordinates.

2. **Probe** (blobfilters): Group tokens by page, hash into probe bitmaps,
   compute containment against domain catalog.

3. **Segment** (blobfilters SQL recipes): Use gaps-and-islands on page-level
   domain signatures to identify logical table runs spanning multiple pages.

## Shared Build Pattern

All blob* C projects follow the same structure:

```
project/
├── CMakeLists.txt          # Root build, FetchContent for all deps
├── include/                # Public C API header
├── src/                    # Core implementation
│   └── CMakeLists.txt      # Static library target
├── sqlite_ext/             # SQLite loadable extension
├── duckdb_ext/             # DuckDB loadable extension
├── python/                 # nanobind bindings
│   └── projectname/        # Python package with __init__.py
├── demo/                   # Usage examples (C, SQL, Python)
├── test/                   # C test programs
├── .python-version         # Pins Python 3.14
└── pyproject.toml          # scikit-build-core + nanobind
```

Dependencies are managed via CMake FetchContent (never vendored). The core
implementation is shared source compiled into each extension — the SQLite
extension, DuckDB extension, and Python bindings each produce a self-contained
shared object with the core logic statically linked in. No separate runtime
library to deploy.

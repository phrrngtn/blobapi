"""
Log-oriented, set-oriented pipeline for the OpenAPI catalog.

Architecture:
  1. Dulwich walks git commits since floor (from DB) → events
  2. Dulwich reads unique blob contents
  3. DuckDB converts YAML→JSON in parallel via blobtemplates extension
  4. DuckDB shreds JSON into relational rows (paths, operations, etc.)
  5. SQLAlchemy bulk-writes to target TTST tables

Two modes:
  sync          — incremental from floor; HEAD snapshot on first run
  sync --full   — walk entire git history for complete TTST reconstruction

The spec identity is (provider, api_name). Each version has:
  sys_from = commit timestamp when this version appeared
  sys_to   = commit timestamp of the next version, or NULL if current
"""

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from dulwich.diff_tree import tree_changes as dulwich_tree_changes
from dulwich.repo import Repo
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from blobapi.models import (
    ApiOperation,
    ApiParameter,
    ApiPath,
    ApiRegistry,
    ApiResponse,
    ApiSchema,
    ApiSpec,
    GitSpecStaging,
)

log = logging.getLogger(__name__)

REPO_URL = "https://github.com/APIs-guru/openapi-directory.git"
DEFAULT_SUBMODULE_DIR = Path("openapi-directory")

# Regex patterns for fast metadata extraction (no YAML parse)
_TITLE_RE = re.compile(rb"^\s+title:\s*(.+)", re.MULTILINE)
_VERSION_RE = re.compile(rb"^\s+version:\s*(.+)", re.MULTILINE)
_OPENAPI_RE = re.compile(rb"^openapi:\s*(.+)", re.MULTILINE)
_SWAGGER_RE = re.compile(rb"^swagger:\s*(.+)", re.MULTILINE)

HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unquote(val: str) -> str:
    if len(val) >= 2 and (
        (val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'")
    ):
        return val[1:-1]
    return val


def _parse_api_path(rel_path: str) -> tuple[str, str] | None:
    """Parse APIs/{provider}/{version}/spec or APIs/{provider}/{service}/{version}/spec."""
    parts = rel_path.split("/")
    if len(parts) < 3 or parts[0] != "APIs":
        return None
    if len(parts) == 4:
        return (parts[1], parts[1])
    elif len(parts) == 5:
        return (parts[1], parts[2])
    return None


def _extract_metadata(data: bytes) -> dict:
    """Extract title, version, openapi_version from raw YAML bytes via regex."""
    result = {}
    m = _TITLE_RE.search(data)
    if m:
        result["title"] = _unquote(m.group(1).strip().decode(errors="replace"))
    m = _VERSION_RE.search(data)
    if m:
        result["spec_version"] = _unquote(m.group(1).strip().decode(errors="replace"))
    m = _OPENAPI_RE.search(data)
    if m:
        result["openapi_version"] = _unquote(m.group(1).strip().decode(errors="replace"))
    else:
        m = _SWAGGER_RE.search(data)
        if m:
            result["openapi_version"] = _unquote(
                m.group(1).strip().decode(errors="replace")
            )
    return result


def ensure_submodule(submodule_dir: Path = DEFAULT_SUBMODULE_DIR) -> Path:
    apis_dir = submodule_dir / "APIs"
    if apis_dir.exists():
        return submodule_dir
    import subprocess

    parent = submodule_dir.parent
    if (parent / ".gitmodules").exists():
        log.info("Initializing submodule at %s", submodule_dir)
        subprocess.run(
            ["git", "submodule", "update", "--init", str(submodule_dir.name)],
            cwd=parent,
            capture_output=True,
            text=True,
            check=True,
        )
        return submodule_dir
    raise FileNotFoundError(
        f"No openapi-directory found at {submodule_dir}. "
        f"Run: git submodule update --init openapi-directory"
    )


def _ensure_git_registry(session: Session) -> int:
    stmt = select(ApiRegistry).filter_by(name="apis.guru.git", sys_to=None)
    reg = session.execute(stmt).scalar_one_or_none()
    if reg is None:
        reg = ApiRegistry(
            name="apis.guru.git",
            base_url=REPO_URL,
            registry_type="git",
        )
        session.add(reg)
        session.flush()
    return reg.registry_id


def _find_blobtemplates_extension() -> str:
    """Discover the blobtemplates DuckDB extension from sibling checkouts."""
    # Check connections.toml for [extensions] section
    try:
        from blobapi.config import load_config

        config = load_config()
        ext_config = config.get("extensions", {})
        if "blobtemplates" in ext_config:
            p = Path(ext_config["blobtemplates"]).expanduser()
            if p.exists():
                return str(p)
    except Exception:
        pass

    # Auto-discover from sibling checkouts
    candidates = [
        Path.home() / "checkouts" / "blobtemplates" / "build" / "duckdb" / "blobtemplates.duckdb_extension",
        Path.cwd().parent / "blobtemplates" / "build" / "duckdb" / "blobtemplates.duckdb_extension",
    ]
    for c in candidates:
        if c.exists():
            return str(c)

    raise FileNotFoundError(
        "blobtemplates DuckDB extension not found. "
        "Build it: cd ~/checkouts/blobtemplates && cmake -B build && cmake --build build"
    )


# ---------------------------------------------------------------------------
# Git log walking (dulwich)
# ---------------------------------------------------------------------------


def _walk_tree(store, tree_sha, prefix=""):
    """Recursively walk a git tree, yielding (path, blob_sha_hex) for files."""
    tree = store[tree_sha]
    for item in tree.items():
        path = prefix + "/" + item.path.decode() if prefix else item.path.decode()
        if item.mode & 0o40000:
            yield from _walk_tree(store, item.sha, path)
        else:
            yield (path, item.sha.decode())


def _snapshot_head(repo, store):
    """Snapshot HEAD tree → list of (commit_sha, commit_ts, provider, api_name, file_path, blob_sha)."""
    head = repo.head()
    commit = store[head]
    commit_sha = head.decode()
    commit_ts = datetime.fromtimestamp(commit.author_time, tz=timezone.utc)

    events = []
    for path, blob_sha in _walk_tree(store, commit.tree):
        if not path.startswith("APIs/"):
            continue
        if not (path.endswith("/openapi.yaml") or path.endswith("/swagger.yaml")):
            continue
        identity = _parse_api_path(path)
        if identity is None:
            continue
        provider, api_name = identity
        events.append((commit_sha, commit_ts, provider, api_name, path, blob_sha))

    return events


def _walk_commits_since(repo, store, floor_ts):
    """Walk git commits since floor_ts, yielding spec change events."""
    events = []

    for entry in repo.get_walker():
        commit = entry.commit
        commit_ts = datetime.fromtimestamp(commit.author_time, tz=timezone.utc)

        if floor_ts and commit_ts <= floor_ts:
            break

        commit_sha = commit.id.decode()
        parent_tree = store[commit.parents[0]].tree if commit.parents else None

        for change in dulwich_tree_changes(store, parent_tree, commit.tree):
            if change.new is None or change.new.path is None:
                continue
            path = change.new.path.decode()
            if not path.startswith("APIs/"):
                continue
            if not (path.endswith("/openapi.yaml") or path.endswith("/swagger.yaml")):
                continue
            identity = _parse_api_path(path)
            if identity is None:
                continue
            provider, api_name = identity
            blob_sha = change.new.sha.decode()
            events.append((commit_sha, commit_ts, provider, api_name, path, blob_sha))

    # Reverse to chronological order (walker yields newest first)
    events.reverse()
    return events


def _read_blobs(store, blob_shas):
    """Read unique blob contents from git object store → list of (sha, yaml_text)."""
    result = []
    for sha in blob_shas:
        try:
            blob = store[sha.encode()]
            result.append((sha, blob.data.decode("utf-8", errors="replace")))
        except Exception:
            log.debug("Could not read blob %s", sha[:8])
    return result


# ---------------------------------------------------------------------------
# DuckDB pipeline: YAML→JSON + shred (set-oriented, parallel)
# ---------------------------------------------------------------------------


def _duckdb_yaml_to_json(blob_contents, extension_path):
    """
    Set-oriented YAML→JSON conversion via DuckDB + blobtemplates extension.

    DuckDB does the bt_yaml_to_json() in parallel (the 100x win over Python YAML).
    Returns dict: blob_sha → json_string.
    """
    import duckdb

    duck = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    duck.execute(f"LOAD '{extension_path}'")

    duck.execute(
        "CREATE TABLE blobs (blob_sha VARCHAR, yaml_content VARCHAR)"
    )
    duck.executemany("INSERT INTO blobs VALUES (?, ?)", blob_contents)

    t0 = time.monotonic()
    results = duck.execute(
        "SELECT blob_sha, bt_yaml_to_json(yaml_content) FROM blobs"
    ).fetchall()
    log.info("bt_yaml_to_json: %.2fs for %d blobs", time.monotonic() - t0, len(blob_contents))

    duck.close()
    return {sha: json_str for sha, json_str in results}


def _shred_specs(events, json_blobs):
    """
    Shred JSON specs into relational rows (Python — fast since JSON is pre-parsed).

    Returns dict with all table data ready for bulk insert.
    """
    # Deduplicate events: latest per (provider, api_name, commit_ts)
    seen = {}
    for commit_sha, commit_ts, provider, api_name, file_path, blob_sha in events:
        key = (provider, api_name, commit_ts)
        seen[key] = (commit_sha, file_path, blob_sha)

    # Build ordered version list per (provider, api_name)
    from collections import defaultdict
    versions = defaultdict(list)
    for (provider, api_name, commit_ts), (commit_sha, file_path, blob_sha) in seen.items():
        versions[(provider, api_name)].append(
            (commit_ts, commit_sha, file_path, blob_sha)
        )
    for k in versions:
        versions[k].sort()

    specs = []
    paths = []
    operations = []
    parameters = []
    responses = []
    schemas = []

    for (provider, api_name), version_list in versions.items():
        for i, (commit_ts, commit_sha, file_path, blob_sha) in enumerate(version_list):
            sys_from = commit_ts
            sys_to = version_list[i + 1][0] if i + 1 < len(version_list) else None

            json_str = json_blobs.get(blob_sha)
            if json_str is None:
                continue
            try:
                doc = json.loads(json_str)
            except (json.JSONDecodeError, TypeError):
                continue

            info = doc.get("info") or {}
            specs.append(dict(
                provider=provider,
                api_name=api_name,
                sys_from=sys_from,
                sys_to=sys_to,
                title=info.get("title"),
                spec_version=info.get("version"),
                description=info.get("description"),
                openapi_version=doc.get("openapi") or doc.get("swagger"),
                raw_spec=doc,
                source_url=f"git://{commit_sha}:{file_path}",
            ))

            # Shred paths → operations → parameters, responses
            for path_str, path_item in (doc.get("paths") or {}).items():
                if path_str.startswith("x-") or not isinstance(path_item, dict):
                    continue
                paths.append(dict(
                    provider=provider, api_name=api_name, sys_from=sys_from,
                    path=path_str,
                    summary=path_item.get("summary"),
                    description=path_item.get("description"),
                ))
                path_params = path_item.get("parameters") or []

                for method in HTTP_METHODS:
                    op = path_item.get(method)
                    if op is None or not isinstance(op, dict):
                        continue
                    operations.append(dict(
                        provider=provider, api_name=api_name, sys_from=sys_from,
                        path=path_str, method=method,
                        operation_name=op.get("operationId"),
                        summary=op.get("summary"),
                        description=op.get("description"),
                        deprecated=op.get("deprecated", False),
                        tags=op.get("tags"),
                    ))

                    # Parameters (operation-level overrides path-level)
                    all_params = path_params + (op.get("parameters") or [])
                    seen_params = set()
                    for param in all_params:
                        if not isinstance(param, dict):
                            continue
                        param_key = (param.get("name"), param.get("in"))
                        if param_key in seen_params:
                            continue
                        seen_params.add(param_key)
                        parameters.append(dict(
                            provider=provider, api_name=api_name, sys_from=sys_from,
                            path=path_str, method=method,
                            name=param.get("name", ""),
                            location=param.get("in", "query"),
                            required=param.get("required", False),
                            schema_json=param.get("schema"),
                            description=param.get("description"),
                        ))

                    # Responses
                    for status_code, resp in (op.get("responses") or {}).items():
                        if not isinstance(resp, dict):
                            continue
                        schema_json = None
                        media_type = None
                        for mt, mt_data in (resp.get("content") or {}).items():
                            media_type = mt
                            schema_json = mt_data.get("schema") if isinstance(mt_data, dict) else None
                            break
                        if schema_json is None:
                            schema_json = resp.get("schema")
                        responses.append(dict(
                            provider=provider, api_name=api_name, sys_from=sys_from,
                            path=path_str, method=method,
                            status_code=str(status_code),
                            description=resp.get("description"),
                            media_type=media_type,
                            schema_json=schema_json,
                        ))

            # Component schemas
            components = doc.get("components") or {}
            defs = components.get("schemas") or doc.get("definitions") or {}
            for schema_name, schema_body in defs.items():
                if not isinstance(schema_body, dict):
                    continue
                schemas.append(dict(
                    provider=provider, api_name=api_name, sys_from=sys_from,
                    schema_name=schema_name,
                    schema_json=schema_body,
                    description=schema_body.get("description"),
                ))

    return {
        "specs": specs,
        "paths": paths,
        "operations": operations,
        "parameters": parameters,
        "responses": responses,
        "schemas": schemas,
    }


# ---------------------------------------------------------------------------
# Target DB writer (SQLAlchemy bulk)
# ---------------------------------------------------------------------------


def _write_to_target(session, staged, registry_id):
    """
    Bulk-write staged data to the target TTST tables.

    Git is the store of record — just delete and reinsert.
    No careful version management needed; we can always rebuild from git.
    """
    # Clear all existing data (child tables first for FK constraints)
    session.execute(delete(ApiParameter))
    session.execute(delete(ApiResponse))
    session.execute(delete(ApiOperation))
    session.execute(delete(ApiPath))
    session.execute(delete(ApiSchema))
    session.execute(delete(ApiSpec))
    session.flush()

    # ── Specs ──
    spec_rows = [dict(registry_id=registry_id, **s) for s in staged["specs"]]
    if spec_rows:
        session.execute(ApiSpec.__table__.insert(), spec_rows)
        session.flush()

    # Build spec_id lookup: (provider, api_name, sys_from) → spec_id
    # Normalize sys_from to naive UTC — SQLite strips tzinfo on roundtrip
    def _naive(dt):
        if dt is None:
            return dt
        return dt.replace(tzinfo=None) if hasattr(dt, 'replace') else dt

    spec_id_map = {}
    for row in session.execute(
        select(ApiSpec.spec_id, ApiSpec.provider, ApiSpec.api_name, ApiSpec.sys_from)
    ):
        spec_id_map[(row.provider, row.api_name, _naive(row.sys_from))] = row.spec_id

    # ── Paths ──
    path_rows = []
    for p in staged["paths"]:
        spec_id = spec_id_map.get((p["provider"], p["api_name"], _naive(p["sys_from"])))
        if spec_id is None:
            continue
        path_rows.append(dict(
            spec_id=spec_id, path=p["path"][:500],
            summary=p["summary"], description=p["description"],
            sys_from=p["sys_from"],
        ))
    if path_rows:
        session.execute(ApiPath.__table__.insert(), path_rows)
        session.flush()

    # Build path_id lookup
    path_id_map = {}
    for row in session.execute(select(ApiPath.path_id, ApiPath.spec_id, ApiPath.path)):
        path_id_map[(row.spec_id, row.path)] = row.path_id

    # ── Operations ──
    op_rows = []
    for o in staged["operations"]:
        spec_id = spec_id_map.get((o["provider"], o["api_name"], _naive(o["sys_from"])))
        if spec_id is None:
            continue
        path_id = path_id_map.get((spec_id, o["path"][:500]))
        if path_id is None:
            continue
        op_rows.append(dict(
            path_id=path_id, method=o["method"],
            operation_name=o["operation_name"],
            summary=o["summary"], description=o["description"],
            deprecated=o["deprecated"] or False, tags=o["tags"],
            sys_from=o["sys_from"],
        ))
    if op_rows:
        session.execute(ApiOperation.__table__.insert(), op_rows)
        session.flush()

    # Build operation_id lookup
    op_id_map = {}
    for row in session.execute(
        select(ApiOperation.operation_id, ApiOperation.path_id, ApiOperation.method)
    ):
        op_id_map[(row.path_id, row.method)] = row.operation_id

    # ── Parameters ──
    param_rows = []
    for p in staged["parameters"]:
        spec_id = spec_id_map.get((p["provider"], p["api_name"], _naive(p["sys_from"])))
        if spec_id is None:
            continue
        path_id = path_id_map.get((spec_id, p["path"][:500]))
        if path_id is None:
            continue
        op_id = op_id_map.get((path_id, p["method"]))
        if op_id is None:
            continue
        param_rows.append(dict(
            operation_id=op_id, name=p["name"] or "",
            location=p["location"] or "query",
            required=p["required"] or False,
            schema_json=p["schema_json"], description=p["description"],
            sys_from=p["sys_from"],
        ))
    if param_rows:
        session.execute(ApiParameter.__table__.insert(), param_rows)

    # ── Responses ──
    resp_rows = []
    for r in staged["responses"]:
        spec_id = spec_id_map.get((r["provider"], r["api_name"], _naive(r["sys_from"])))
        if spec_id is None:
            continue
        path_id = path_id_map.get((spec_id, r["path"][:500]))
        if path_id is None:
            continue
        op_id = op_id_map.get((path_id, r["method"]))
        if op_id is None:
            continue
        resp_rows.append(dict(
            operation_id=op_id, status_code=r["status_code"],
            description=r["description"], media_type=r["media_type"],
            schema_json=r["schema_json"], sys_from=r["sys_from"],
        ))
    if resp_rows:
        session.execute(ApiResponse.__table__.insert(), resp_rows)

    # ── Schemas ──
    schema_rows = []
    for s in staged["schemas"]:
        spec_id = spec_id_map.get((s["provider"], s["api_name"], _naive(s["sys_from"])))
        if spec_id is None:
            continue
        schema_rows.append(dict(
            spec_id=spec_id, schema_name=s["schema_name"],
            schema_json=s["schema_json"] or {},
            description=s["description"], sys_from=s["sys_from"],
        ))
    if schema_rows:
        session.execute(ApiSchema.__table__.insert(), schema_rows)

    session.commit()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def catalog(
    session: Session,
    *,
    clone_dir: Path = DEFAULT_SUBMODULE_DIR,
) -> dict:
    """
    Fast metadata-only catalog: dulwich tree walk + regex, no YAML parse.
    Populates git_spec_staging. Completes in < 2s for 4K specs.
    """
    repo_path = ensure_submodule(clone_dir)
    repo = Repo(str(repo_path))
    store = repo.object_store

    head_sha = repo.head().decode()
    commit = store[repo.head()]
    commit_ts = datetime.fromtimestamp(commit.author_time, tz=timezone.utc)

    rows = []
    for path, blob_sha in _walk_tree(store, commit.tree):
        if not path.startswith("APIs/"):
            continue
        if not (path.endswith("/openapi.yaml") or path.endswith("/swagger.yaml")):
            continue
        identity = _parse_api_path(path)
        if identity is None:
            continue
        provider, api_name = identity
        blob = store[blob_sha.encode()]
        meta = _extract_metadata(blob.data)
        rows.append(dict(
            commit_sha=head_sha,
            commit_ts=commit_ts,
            provider=provider,
            api_name=api_name,
            file_path=path,
            blob_sha=blob_sha,
            title=meta.get("title"),
            spec_version=meta.get("spec_version"),
            openapi_version=meta.get("openapi_version"),
        ))

    session.execute(delete(GitSpecStaging))
    if rows:
        session.execute(GitSpecStaging.__table__.insert(), rows)
    session.commit()

    log.info("Catalog: %d specs from commit %s", len(rows), head_sha[:8])
    return {"specs": len(rows), "commit_sha": head_sha}


def sync(
    session: Session,
    *,
    clone_dir: Path = DEFAULT_SUBMODULE_DIR,
    full_history: bool = False,
) -> dict:
    """
    Log-oriented, set-oriented sync pipeline.

    Git is the store of record. We rebuild the TTST from git each time:
    - Default: snapshot HEAD (current state, one version per spec)
    - --full: walk entire commit history for complete temporal reconstruction

    All YAML→JSON conversion happens in DuckDB (parallel, C-speed via ryml).
    All JSON shredding happens in DuckDB SQL (set-oriented).
    Results are bulk-written to the target DB (delete + reinsert).
    """
    t_start = time.monotonic()

    repo_path = ensure_submodule(clone_dir)
    repo = Repo(str(repo_path))
    store = repo.object_store
    extension_path = _find_blobtemplates_extension()

    # Walk git
    t0 = time.monotonic()
    if full_history:
        events = _walk_commits_since(repo, store, floor_ts=None)
    else:
        events = _snapshot_head(repo, store)
    log.info("Git walk: %d events in %.2fs", len(events), time.monotonic() - t0)

    if not events:
        return {"events": 0, "specs": 0, "paths": 0, "operations": 0}

    # Read unique blob contents
    t0 = time.monotonic()
    unique_shas = list({e[5] for e in events})
    blob_contents = _read_blobs(store, unique_shas)
    log.info("Read %d unique blobs in %.2fs", len(blob_contents), time.monotonic() - t0)

    # DuckDB: YAML→JSON in parallel (the 100x win)
    json_blobs = _duckdb_yaml_to_json(blob_contents, extension_path)

    # Shred JSON into relational rows (Python — fast since JSON is pre-parsed)
    t0 = time.monotonic()
    staged = _shred_specs(events, json_blobs)
    log.info(
        "Shred: %d specs, %d paths, %d ops in %.2fs",
        len(staged["specs"]), len(staged["paths"]),
        len(staged["operations"]), time.monotonic() - t0,
    )

    # Ensure registry exists
    registry_id = _ensure_git_registry(session)

    # Write to target (git is authoritative — just replace)
    t0 = time.monotonic()
    _write_to_target(session, staged, registry_id)
    log.info("Target write: %.2fs", time.monotonic() - t0)

    elapsed = time.monotonic() - t_start
    counts = {
        "events": len(events),
        "specs": len(staged["specs"]),
        "paths": len(staged["paths"]),
        "operations": len(staged["operations"]),
        "parameters": len(staged["parameters"]),
        "responses": len(staged["responses"]),
        "schemas": len(staged["schemas"]),
        "elapsed": f"{elapsed:.2f}s",
    }
    log.info(
        "Sync complete: %d specs, %d paths, %d operations in %s",
        counts["specs"], counts["paths"], counts["operations"], counts["elapsed"],
    )
    return counts

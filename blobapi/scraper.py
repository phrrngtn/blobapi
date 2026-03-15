"""
HTTP-based scraper for APIs.guru (optional, alternative to git-based scraping).

The primary scraping path is git-based (see git_scraper.py), which reads
specs directly from the openapi-directory submodule with no HTTP needed.

This HTTP scraper exists as a fallback for environments where you don't
want to clone the full git repo, or for one-off fetches of specific specs.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from blobapi.loader import load_spec
from blobapi.models import ApiRegistry, ApiSpec

log = logging.getLogger(__name__)

APIS_GURU_LIST = "https://api.apis.guru/v2/list.json"
APIS_GURU_METRICS = "https://api.apis.guru/v2/metrics.json"

STATE_FILE_DEFAULT = Path("scraper_state.json")


def _load_state(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def _save_state(path: Path, state: dict):
    path.write_text(json.dumps(state, indent=2, default=str))


# ---------------------------------------------------------------------------
# APIs.guru
# ---------------------------------------------------------------------------


def _ensure_apis_guru_registry(session: Session) -> int:
    """Get or create the APIs.guru registry row."""
    stmt = select(ApiRegistry).filter_by(name="apis.guru", sys_to=None)
    reg = session.execute(stmt).scalar_one_or_none()
    if reg is None:
        reg = ApiRegistry(
            name="apis.guru",
            base_url="https://api.apis.guru/v2/",
            registry_type="apis_guru",
        )
        session.add(reg)
        session.flush()
    return reg.registry_id


def scrape_apis_guru(
    session: Session,
    *,
    state_file: Path = STATE_FILE_DEFAULT,
    max_specs: int | None = None,
    force_full: bool = False,
) -> dict:
    """
    Incremental scrape of APIs.guru catalog.

    Returns a summary dict with counts of new/updated/unchanged specs.
    """
    state = _load_state(state_file)
    headers = {}
    if not force_full:
        etag = state.get("apis_guru_etag")
        if etag:
            headers["If-None-Match"] = etag

    log.info("Fetching APIs.guru catalog...")
    with httpx.Client(timeout=60) as client:
        resp = client.get(APIS_GURU_LIST, headers=headers)

    if resp.status_code == 304:
        log.info("APIs.guru catalog unchanged (ETag match)")
        return {"status": "unchanged", "new": 0, "updated": 0}

    resp.raise_for_status()
    catalog = resp.json()

    # Save ETag for next time
    new_etag = resp.headers.get("etag")
    if new_etag:
        state["apis_guru_etag"] = new_etag

    registry_id = _ensure_apis_guru_registry(session)

    # Track what we've already seen by (provider, api_name) -> source_updated_at
    last_seen = state.get("apis_guru_last_seen", {})
    counts = {"new": 0, "updated": 0, "unchanged": 0, "errors": 0}

    for api_key, api_entry in catalog.items():
        if max_specs is not None and (counts["new"] + counts["updated"]) >= max_specs:
            log.info("Reached max_specs=%d, stopping", max_specs)
            break

        # api_key is "provider" or "provider:service"
        if ":" in api_key:
            provider, api_name = api_key.split(":", 1)
        else:
            provider = api_key
            api_name = api_key

        # Use the preferred version
        preferred = api_entry.get("preferred")
        versions = api_entry.get("versions", {})
        if preferred and preferred in versions:
            version_data = versions[preferred]
        elif versions:
            version_data = next(iter(versions.values()))
        else:
            continue

        updated_str = version_data.get("updated")
        if updated_str:
            source_updated = datetime.fromisoformat(
                updated_str.replace("Z", "+00:00")
            )
        else:
            source_updated = None

        # Check if we've already loaded this version
        prev_updated = last_seen.get(api_key)
        if prev_updated and updated_str and prev_updated == updated_str and not force_full:
            counts["unchanged"] += 1
            continue

        # Fetch the actual spec
        spec_url = (
            version_data.get("swaggerUrl")
            or version_data.get("openapiUrl")
        )
        if not spec_url:
            log.warning("No spec URL for %s, skipping", api_key)
            continue

        try:
            log.info("Fetching spec: %s", api_key)
            with httpx.Client(timeout=30) as client:
                spec_resp = client.get(spec_url)
            spec_resp.raise_for_status()
            spec_doc = spec_resp.json()
        except Exception:
            log.exception("Failed to fetch spec for %s", api_key)
            counts["errors"] += 1
            continue

        # Load into normalized tables
        try:
            load_spec(
                session,
                spec_doc,
                provider=provider,
                api_name=api_name,
                registry_id=registry_id,
                source_url=spec_url,
                source_updated_at=source_updated,
            )
            if prev_updated:
                counts["updated"] += 1
            else:
                counts["new"] += 1

            last_seen[api_key] = updated_str
        except Exception:
            log.exception("Failed to load spec for %s", api_key)
            counts["errors"] += 1
            continue

    state["apis_guru_last_seen"] = last_seen
    state["apis_guru_last_scrape"] = datetime.now(timezone.utc).isoformat()
    _save_state(state_file, state)
    session.commit()

    log.info(
        "APIs.guru scrape complete: %d new, %d updated, %d unchanged, %d errors",
        counts["new"], counts["updated"], counts["unchanged"], counts["errors"],
    )
    return counts

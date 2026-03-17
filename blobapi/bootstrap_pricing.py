"""
Bootstrap LLM provider reference data and pricing from Bifrost.

Two operations:
  load_providers()  — upsert adapters/providers.yaml into llm_provider TTST
  bootstrap_prices() — seed llm_model_price_history from Bifrost /v1/models,
                        using the Docker image creation date as sys_from
"""

import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import httpx
import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from blobapi.models import LlmModelPriceHistory, LlmProvider

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

log = logging.getLogger(__name__)

# Anthropic-specific cache/batch multipliers (relative to base input/output).
# Other providers have different or no such tiers — leave NULL.
ANTHROPIC_MULTIPLIERS = {
    "cache_write_5m": 1.25,   # of input
    "cache_write_1h": 2.0,    # of input
    "cache_read": 0.1,        # of input
    "batch_input": 0.5,       # of input
    "batch_output": 0.5,      # of output
}


def _get_bifrost_image_created(container_name: str = "bifrost") -> datetime:
    """Extract the Docker image creation timestamp for sys_from."""
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.Created}}", container_name],
        capture_output=True, text=True, check=True,
    )
    raw = result.stdout.strip()
    # Docker returns RFC 3339 with nanoseconds; truncate to microseconds
    if "." in raw:
        base, frac = raw.split(".", 1)
        # Strip timezone suffix from fractional part
        for tz_char in ("Z", "+", "-"):
            if tz_char in frac:
                idx = frac.index(tz_char)
                frac_digits = frac[:idx][:6]
                tz_suffix = frac[idx:]
                raw = f"{base}.{frac_digits}{tz_suffix}"
                break
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def load_providers(session: Session, yaml_path: Path, now: datetime) -> int:
    """Upsert providers.yaml into llm_provider TTST. Returns count of changes."""
    with open(yaml_path) as f:
        entries = yaml.load(f, Loader=SafeLoader)

    if not isinstance(entries, list):
        return 0

    changed = 0
    for entry in entries:
        slug = entry["provider"]
        display_name = entry.get("display_name")
        urls = dict(entry.get("urls") or {})
        # Merge jina config into urls for single-column storage
        if "jina" in entry:
            urls["jina"] = entry["jina"]

        existing = session.execute(
            select(LlmProvider).filter_by(provider=slug, sys_to=None)
        ).scalar_one_or_none()

        # Skip if unchanged
        if existing and (
            existing.display_name == display_name
            and existing.urls == urls
        ):
            continue

        if existing:
            existing.sys_to = now

        session.add(LlmProvider(
            provider=slug,
            display_name=display_name,
            urls=urls,
            sys_from=now,
        ))
        changed += 1

    session.flush()
    return changed


def _fetch_bifrost_models(bifrost_url: str) -> list[dict]:
    """Fetch model pricing from Bifrost /v1/models endpoint."""
    resp = httpx.get(bifrost_url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


def _prices_match(existing: LlmModelPriceHistory, new: dict) -> bool:
    """Compare all price columns between existing row and new data."""
    return (
        existing.input_per_mtok == new["input_per_mtok"]
        and existing.output_per_mtok == new["output_per_mtok"]
        and existing.cache_write_5m_per_mtok == new.get("cache_write_5m_per_mtok")
        and existing.cache_write_1h_per_mtok == new.get("cache_write_1h_per_mtok")
        and existing.cache_read_per_mtok == new.get("cache_read_per_mtok")
        and existing.batch_input_per_mtok == new.get("batch_input_per_mtok")
        and existing.batch_output_per_mtok == new.get("batch_output_per_mtok")
    )


def bootstrap_prices(
    session: Session,
    bifrost_url: str = "http://localhost:8080/v1/models",
    image_created: datetime | None = None,
    container_name: str = "bifrost",
) -> dict:
    """
    Seed llm_model_price_history from Bifrost.

    Returns dict with counts: {inserted, skipped, closed}.
    """
    if image_created is None:
        image_created = _get_bifrost_image_created(container_name)
    log.info("Using sys_from = %s (Docker image creation date)", image_created)

    models = _fetch_bifrost_models(bifrost_url)
    inserted = 0
    skipped = 0
    closed = 0

    for m in models:
        model_id = m.get("id")
        pricing = m.get("pricing")
        if not model_id or not pricing:
            continue

        prompt_price = pricing.get("prompt")
        completion_price = pricing.get("completion")
        if prompt_price is None or completion_price is None:
            continue

        input_per_mtok = float(prompt_price) * 1_000_000
        output_per_mtok = float(completion_price) * 1_000_000

        # Extract provider slug from model_id (split on first "/")
        provider = model_id.split("/", 1)[0] if "/" in model_id else "unknown"

        # Apply cache/batch multipliers only for Anthropic
        row_data = {
            "input_per_mtok": input_per_mtok,
            "output_per_mtok": output_per_mtok,
        }
        if provider == "anthropic":
            row_data["cache_write_5m_per_mtok"] = round(
                input_per_mtok * ANTHROPIC_MULTIPLIERS["cache_write_5m"], 4)
            row_data["cache_write_1h_per_mtok"] = round(
                input_per_mtok * ANTHROPIC_MULTIPLIERS["cache_write_1h"], 4)
            row_data["cache_read_per_mtok"] = round(
                input_per_mtok * ANTHROPIC_MULTIPLIERS["cache_read"], 4)
            row_data["batch_input_per_mtok"] = round(
                input_per_mtok * ANTHROPIC_MULTIPLIERS["batch_input"], 4)
            row_data["batch_output_per_mtok"] = round(
                output_per_mtok * ANTHROPIC_MULTIPLIERS["batch_output"], 4)

        # Check for existing current row
        existing = session.execute(
            select(LlmModelPriceHistory).filter_by(model=model_id, sys_to=None)
        ).scalar_one_or_none()

        if existing and _prices_match(existing, row_data):
            skipped += 1
            continue

        if existing:
            existing.sys_to = image_created
            closed += 1

        session.add(LlmModelPriceHistory(
            model=model_id,
            sys_from=image_created,
            **row_data,
        ))
        inserted += 1

    session.flush()
    return {"inserted": inserted, "skipped": skipped, "closed": closed}

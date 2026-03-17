"""
Scrape LLM provider pricing pages and update the price history TTST.

Pipeline per provider:
  1. Fetch pricing page via Jina Reader (r.jina.ai/{url}) → clean markdown
  2. Pass markdown + provider name to the LLM (via Bifrost) for structured extraction
  3. Compare against current rows in llm_model_price_history
  4. Insert/close rows where prices changed

Jina Reader handles JS-rendered SPAs, strips boilerplate, and returns
compact markdown — typically <1k tokens for a pricing page. This makes
the LLM extraction cheap (haiku-class model is sufficient).
"""

import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from blobapi.model_id import normalize_model_id
from blobapi.models import LlmModelPriceHistory, LlmProvider

log = logging.getLogger(__name__)

JINA_READER_PREFIX = "https://r.jina.ai/"

# Bifrost chat completions endpoint (OpenAI-compatible)
DEFAULT_BIFROST_URL = "http://localhost:8080/v1/chat/completions"
DEFAULT_MODEL = "anthropic/claude-haiku-4-5-20251001"


def _fetch_via_jina(
    pricing_url: str,
    target_selector: str | None = None,
    remove_selector: str | None = None,
    timeout: int = 60,
) -> str:
    """Fetch a URL via Jina Reader, returning clean markdown."""
    jina_url = JINA_READER_PREFIX + pricing_url
    headers = {}
    if target_selector:
        headers["X-Target-Selector"] = target_selector
    if remove_selector:
        headers["X-Remove-Selector"] = remove_selector
    resp = httpx.get(jina_url, headers=headers, timeout=timeout,
                     follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def _extract_pricing_via_llm(
    provider: str,
    page_markdown: str,
    bifrost_url: str = DEFAULT_BIFROST_URL,
    model: str = DEFAULT_MODEL,
) -> list[dict]:
    """
    Send the pricing page markdown to the LLM for structured extraction.

    Returns a list of dicts with keys: model_id, input_per_mtok,
    output_per_mtok, and optional cache/batch fields.
    """
    system_prompt = (
        "You are a pricing data extraction assistant. Extract ALL model "
        "pricing from the provided page content into structured JSON."
    )
    user_prompt = f"""Below is the content of the "{provider}" model pricing page.

<pricing_page>
{page_markdown}
</pricing_page>

For each model listed, return a JSON object with:
- model_id: API identifier prefixed with "{provider}/" (e.g. "{provider}/model-name")
- input_per_mtok: input cost in USD per million tokens
- output_per_mtok: output cost in USD per million tokens
- cache_write_5m_per_mtok: 5-min cache write cost per MTok (null if not listed)
- cache_write_1h_per_mtok: 1-hr cache write cost per MTok (null if not listed)
- cache_read_per_mtok: cache read/hit cost per MTok (null if not listed)
- batch_input_per_mtok: batch input cost per MTok (null if not listed)
- batch_output_per_mtok: batch output cost per MTok (null if not listed)

Rules:
- Extract ONLY what is explicitly stated. Do NOT calculate or infer.
- If batch pricing is in a separate table, merge it by model name.
- Use null (not 0) for unavailable tiers.
- Omit deprecated models.
- Return a JSON array of objects (no wrapper).
"""

    body = {
        "model": model,
        "max_tokens": 4096,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }

    resp = httpx.post(
        bifrost_url,
        json=body,
        timeout=120,
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    data = resp.json()

    # Extract the text content from the response
    content = data["choices"][0]["message"]["content"]
    usage = data.get("usage", {})
    log.info(
        "LLM extraction for %s: %d prompt tokens, %d completion tokens",
        provider,
        usage.get("prompt_tokens", 0),
        usage.get("completion_tokens", 0),
    )

    # Parse the JSON response — LLMs often wrap in code fences or preamble
    import json
    import re

    # Strip markdown code fences if present
    stripped = content.strip()
    fence_match = re.search(r'```(?:json)?\s*\n?(.*?)```', stripped, re.DOTALL)
    if fence_match:
        stripped = fence_match.group(1).strip()

    # Find first [ or { — skip any preamble text
    for i, ch in enumerate(stripped):
        if ch in ('[', '{'):
            stripped = stripped[i:]
            break

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        log.error("Failed to parse LLM response as JSON: %s...", content[:200])
        return []

    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for key in ("models", "data", "pricing"):
            if key in parsed and isinstance(parsed[key], list):
                return parsed[key]
    return []


def _prices_match(existing: LlmModelPriceHistory, new: dict) -> bool:
    """Compare price columns between existing row and scraped data."""
    return (
        existing.input_per_mtok == new.get("input_per_mtok")
        and existing.output_per_mtok == new.get("output_per_mtok")
        and existing.cache_write_5m_per_mtok == new.get("cache_write_5m_per_mtok")
        and existing.cache_write_1h_per_mtok == new.get("cache_write_1h_per_mtok")
        and existing.cache_read_per_mtok == new.get("cache_read_per_mtok")
        and existing.batch_input_per_mtok == new.get("batch_input_per_mtok")
        and existing.batch_output_per_mtok == new.get("batch_output_per_mtok")
    )


def scrape_provider(
    session: Session,
    provider_slug: str,
    pricing_url: str,
    now: datetime,
    bifrost_url: str = DEFAULT_BIFROST_URL,
    model: str = DEFAULT_MODEL,
    target_selector: str | None = None,
    remove_selector: str | None = None,
) -> dict:
    """
    Scrape one provider's pricing page and update the TTST.

    Returns dict with counts: {inserted, skipped, closed, errors}.
    """
    log.info("Fetching %s pricing via Jina Reader", provider_slug)
    try:
        markdown = _fetch_via_jina(
            pricing_url,
            target_selector=target_selector,
            remove_selector=remove_selector,
        )
    except httpx.HTTPError as e:
        log.error("Failed to fetch %s: %s", pricing_url, e)
        return {"inserted": 0, "skipped": 0, "closed": 0, "errors": 1}

    log.info("Extracting pricing for %s via LLM (%d chars of markdown)",
             provider_slug, len(markdown))
    try:
        models = _extract_pricing_via_llm(
            provider_slug, markdown,
            bifrost_url=bifrost_url, model=model,
        )
    except (httpx.HTTPError, Exception) as e:
        log.error("LLM extraction failed for %s: %s", provider_slug, e)
        return {"inserted": 0, "skipped": 0, "closed": 0, "errors": 1}

    # Build canonical ID set from existing TTST rows for this provider
    existing_ids = set(
        row[0] for row in session.execute(
            select(LlmModelPriceHistory.model).filter(
                LlmModelPriceHistory.model.startswith(provider_slug + "/"),
                LlmModelPriceHistory.sys_to.is_(None),
            )
        ).all()
    )

    inserted = 0
    skipped = 0
    closed = 0

    for m in models:
        model_id = m.get("model_id")
        if not model_id or m.get("input_per_mtok") is None:
            continue

        model_id = normalize_model_id(model_id, canonical_ids=existing_ids)

        row_data = {
            "input_per_mtok": m["input_per_mtok"],
            "output_per_mtok": m["output_per_mtok"],
            "cache_write_5m_per_mtok": m.get("cache_write_5m_per_mtok"),
            "cache_write_1h_per_mtok": m.get("cache_write_1h_per_mtok"),
            "cache_read_per_mtok": m.get("cache_read_per_mtok"),
            "batch_input_per_mtok": m.get("batch_input_per_mtok"),
            "batch_output_per_mtok": m.get("batch_output_per_mtok"),
        }

        existing = session.execute(
            select(LlmModelPriceHistory).filter_by(model=model_id, sys_to=None)
        ).scalar_one_or_none()

        if existing and _prices_match(existing, row_data):
            skipped += 1
            continue

        if existing:
            existing.sys_to = now
            closed += 1

        session.add(LlmModelPriceHistory(
            model=model_id,
            sys_from=now,
            **row_data,
        ))
        inserted += 1

    session.flush()
    return {"inserted": inserted, "skipped": skipped, "closed": closed, "errors": 0}


def scrape_all(
    session: Session,
    providers: list[str] | None = None,
    bifrost_url: str = DEFAULT_BIFROST_URL,
    model: str = DEFAULT_MODEL,
) -> dict:
    """
    Scrape pricing for all (or specified) providers.

    Reads provider URLs from the llm_provider table. If providers is None,
    scrapes all providers that have a pricing URL.

    Returns aggregate counts.
    """
    now = datetime.now(timezone.utc)
    totals = {"inserted": 0, "skipped": 0, "closed": 0, "errors": 0}

    query = select(LlmProvider).filter(LlmProvider.sys_to.is_(None))
    if providers:
        query = query.filter(LlmProvider.provider.in_(providers))

    rows = session.execute(query).scalars().all()

    for row in rows:
        urls = row.urls or {}
        pricing_url = urls.get("pricing")
        if not pricing_url:
            log.warning("No pricing URL for %s, skipping", row.provider)
            continue

        jina = row.urls.get("jina", {}) if row.urls else {}
        result = scrape_provider(
            session, row.provider, pricing_url, now,
            bifrost_url=bifrost_url, model=model,
            target_selector=jina.get("target_selector"),
            remove_selector=jina.get("remove_selector"),
        )
        for k in totals:
            totals[k] += result[k]

    return totals

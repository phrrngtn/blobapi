"""
Normalize LLM model identifiers to canonical API form.

The canonical form is what the provider's API accepts and what appears
in response metadata (e.g. _meta.model from llm_adapt). For Anthropic,
this is the Bifrost model ID: dashes, optional date suffix.

Problem: LLM extraction from pricing pages produces marketing-style
names ("Claude Opus 4.6" → "anthropic/claude-opus-4.6") while the API
uses "claude-opus-4-6" or "claude-opus-4-6-20250514".

Strategy:
  1. Normalize dots to dashes in the version portion
  2. Match against known canonical IDs from Bifrost
  3. Fall through to the normalized form if no exact match
"""

import re


def normalize_model_id(model_id: str, canonical_ids: set[str] | None = None) -> str:
    """
    Normalize a model ID to canonical form.

    Steps:
      1. Lowercase
      2. Replace dots with dashes in version numbers (4.6 → 4-6)
      3. If canonical_ids provided, find the best match

    >>> normalize_model_id("anthropic/claude-opus-4.6")
    'anthropic/claude-opus-4-6'
    >>> normalize_model_id("anthropic/claude-haiku-3.5")
    'anthropic/claude-haiku-3-5'
    >>> normalize_model_id("anthropic/claude-opus-4-6")
    'anthropic/claude-opus-4-6'
    """
    normalized = model_id.lower().strip()

    # Replace dots between digits with dashes (4.6 → 4-6, 3.5 → 3-5)
    normalized = re.sub(r'(\d)\.(\d)', r'\1-\2', normalized)

    if canonical_ids is None:
        return normalized

    # Exact match after normalization
    if normalized in canonical_ids:
        return normalized

    # Try matching against canonical IDs: the normalized form may be
    # a prefix of a dated canonical ID (e.g. "anthropic/claude-opus-4-6"
    # matches "anthropic/claude-opus-4-6-20250514" if the undated form
    # isn't itself canonical)
    prefix_matches = sorted(c for c in canonical_ids if c.startswith(normalized))
    if len(prefix_matches) == 1:
        return prefix_matches[0]

    # If multiple prefix matches, prefer the shortest (undated) one
    if prefix_matches:
        return min(prefix_matches, key=len)

    return normalized

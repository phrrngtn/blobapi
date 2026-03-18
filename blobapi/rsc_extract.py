"""
Extract pricing data from Next.js RSC (React Server Components) payloads.

Many provider pricing pages (Mistral, xAI, Cohere) use Next.js with RSC,
where pricing data is serialized as JSON inside self.__next_f.push() script
blocks in the initial HTML. The data is present without JS execution — no
headless browser or Jina needed — but it's not in HTML table elements.

This module provides per-provider extractors that regex the RSC payload
for pricing objects. Each returns a list of dicts matching the
llm_model_cost adapter output schema:
    {model_id, input_per_mtok, output_per_mtok, ...}
"""

import re
import logging

log = logging.getLogger(__name__)


def _extract_rsc_text(html: str) -> str:
    """
    Extract and unescape all self.__next_f.push() payloads from raw HTML.

    RSC payloads are double-escaped JSON inside script tags. We extract
    them, join, and unescape so regex can find field names like "api_endpoint".
    """
    # The payloads are inside: self.__next_f.push([N,"...escaped..."])
    # Some are single-escaped (\"), some double-escaped (\\")
    raw_parts = re.findall(
        r'self\.__next_f\.push\(\[\d+,\s*"(.*?)"\]\)', html, re.DOTALL
    )
    joined = "".join(raw_parts)

    # Unescape: \\" → " and \" → "
    # Do double-escapes first
    unescaped = joined.replace('\\\\"', '"')
    unescaped = unescaped.replace('\\"', '"')
    unescaped = unescaped.replace('\\n', '\n')
    unescaped = unescaped.replace('\\/', '/')

    return unescaped


def extract_mistral(html: str) -> list[dict]:
    """
    Extract Mistral pricing from RSC payload.

    Two field name variants exist:
      "api":"endpoint" (3 featured models)
      "api_endpoint":"endpoint" (all models, double-escaped)

    Price structure: "price":[
        {"value":"Input (/M tokens)","price_dollar":"$$0.5"},
        {"value":"Output (/M tokens)","price_dollar":"$$1.5"}
    ]
    """
    text = _extract_rsc_text(html)
    models = []
    seen = set()

    # Match both "api" and "api_endpoint" field names
    for match in re.finditer(
        r'"api(?:_endpoint)?"\s*:\s*"([^"]+)"'
        r'.{0,500}?"price"\s*:\s*\['
        r'(.*?)\]',
        text,
        re.DOTALL,
    ):
        endpoint = match.group(1)
        price_block = match.group(2)

        if endpoint in seen:
            continue

        inp = re.search(r'Input.*?price_dollar.*?\$+([0-9.]+)', price_block)
        out = re.search(r'Output.*?price_dollar.*?\$+([0-9.]+)', price_block)

        if inp and out:
            seen.add(endpoint)
            models.append({
                "model_id": f"mistral/{endpoint}",
                "input_per_mtok": float(inp.group(1)),
                "output_per_mtok": float(out.group(1)),
            })

    return models


def extract_xai(html: str) -> list[dict]:
    """
    Extract xAI pricing from RSC payload.

    Structure: "name":"grok-xxx"..."promptTextTokenPrice":"$nNNNNN"
               ..."completionTextTokenPrice":"$nNNNNN"
    The $nNNNNN format: divide by 10000 to get $/MTok.
    """
    text = _extract_rsc_text(html)
    models = []
    seen = set()

    for match in re.finditer(
        r'"name"\s*:\s*"(grok[^"]+)".*?'
        r'"promptTextTokenPrice"\s*:\s*"\$n(\d+)".*?'
        r'"completionTextTokenPrice"\s*:\s*"\$n(\d+)"',
        text,
    ):
        name = match.group(1)
        if name in seen:
            continue
        seen.add(name)

        input_price = int(match.group(2)) / 10000
        output_price = int(match.group(3)) / 10000
        models.append({
            "model_id": f"xai/{name}",
            "input_per_mtok": input_price,
            "output_per_mtok": output_price,
        })

    return models


def extract_cohere(html: str) -> list[dict]:
    """
    Extract Cohere pricing from RSC payload.

    Structure: "modelName":"XXX"..."inputPrice":N.N..."outputPrice":N.N
    Prices are plain numbers (already per MTok).
    """
    text = _extract_rsc_text(html)
    models = []

    for match in re.finditer(
        r'"modelName"\s*:\s*"([^"]+)".*?'
        r'"inputPrice"\s*:\s*([0-9.]+).*?'
        r'"outputPrice"\s*:\s*([0-9.]+)',
        text,
    ):
        name = match.group(1)
        input_price = float(match.group(2))
        output_price = float(match.group(3))
        models.append({
            "model_id": f"cohere/{name.lower().replace(' ', '-')}",
            "input_per_mtok": input_price,
            "output_per_mtok": output_price,
        })

    return models


# Provider slug → (URL, extractor function)
RSC_PROVIDERS = {
    "mistral": ("https://mistral.ai/pricing", extract_mistral),
    "xai": ("https://docs.x.ai/developers/models", extract_xai),
    "cohere": ("https://cohere.com/pricing", extract_cohere),
}

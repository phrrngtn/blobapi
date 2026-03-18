"""
Extract pricing data from JS-rendered pages using a headless browser.

For providers where the pricing is behind tab clicks, in Shadow DOM,
or rendered client-side, we use Playwright (headless Chromium) to:
  1. Navigate to the pricing page
  2. Click any necessary tabs to reveal API pricing
  3. Extract the rendered DOM text
  4. Return the text for downstream processing (LLM extraction or regex)

This replaces RSC regex extraction with a more robust approach that
works with the rendered DOM — the same content a human would see.

Requires: playwright + chromium (uv run playwright install chromium)
"""

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class BrowserScrapeConfig:
    """Per-provider configuration for headless browser extraction."""
    url: str
    # CSS selector for a tab/button to click before extraction (optional)
    click_selector: str | None = None
    # Wait time (ms) after clicking before extraction
    click_wait_ms: int = 3000
    # CSS selector to wait for before extracting
    wait_for_selector: str | None = None


# Per-provider browser configs
BROWSER_CONFIGS = {
    "mistral": BrowserScrapeConfig(
        url="https://mistral.ai/pricing",
        click_selector="button:has-text('API pricing')",
    ),
    "xai": BrowserScrapeConfig(
        url="https://docs.x.ai/developers/models",
    ),
    "cohere": BrowserScrapeConfig(
        url="https://cohere.com/pricing",
    ),
}


def fetch_rendered_text(
    provider: str,
    config: BrowserScrapeConfig | None = None,
) -> str:
    """
    Fetch a provider's pricing page using headless Chromium and return
    the rendered DOM text.

    This handles tab clicks, JS rendering, and wait conditions. The
    returned text is suitable for passing to an LLM for structured
    extraction, or for direct regex parsing.
    """
    from playwright.sync_api import sync_playwright

    if config is None:
        config = BROWSER_CONFIGS.get(provider)
    if config is None:
        raise ValueError(f"No browser config for provider {provider}")

    log.info("Launching headless browser for %s: %s", provider, config.url)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(config.url, wait_until="domcontentloaded", timeout=30000)
        # Extra wait for JS rendering
        page.wait_for_timeout(5000)

        if config.click_selector:
            tab = page.query_selector(config.click_selector)
            if tab:
                log.info("Clicking: %s", config.click_selector)
                tab.click()
                page.wait_for_timeout(config.click_wait_ms)
            else:
                log.warning("Click selector not found: %s", config.click_selector)

        if config.wait_for_selector:
            try:
                page.wait_for_selector(config.wait_for_selector, timeout=10000)
            except Exception:
                log.warning("Wait selector not found: %s", config.wait_for_selector)

        body_text = page.inner_text("body")
        log.info("Extracted %d chars of rendered text for %s",
                 len(body_text), provider)

        browser.close()

    return body_text

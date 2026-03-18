"""
Extract pricing data from JS-rendered pages using a headless browser
with CDP isolated world injection.

Uses Playwright (headless Chromium) + Chrome DevTools Protocol to:
  1. Navigate to the pricing page
  2. Click any necessary tabs to reveal API pricing
  3. Create a CDP isolated world (invisible to page scripts)
  4. Inject extraction JS that queries the rendered DOM
  5. Return structured data or clean text for LLM extraction

The isolated world is the same mechanism used by Chrome extensions'
content scripts and Tampermonkey userscripts. The page's React/Next.js
code cannot detect, block, or interfere with our extraction code.

Requires: playwright + chromium (uv run playwright install chromium)
"""

import json
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class BrowserScrapeConfig:
    """Per-provider configuration for headless browser extraction."""
    url: str
    # CSS selector for a tab/button to click before extraction (optional)
    click_selector: str | None = None
    # Wait time (ms) after clicking before extraction
    click_wait_ms: int = 3000
    # CSS selector to scope text extraction (optional, default: body)
    content_selector: str | None = None


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
    Fetch a provider's pricing page using headless Chromium with a
    CDP isolated world, and return the rendered DOM text.

    The extraction runs in an isolated JS execution context that is
    invisible to the page's own scripts. This prevents anti-scraping
    detection and ensures our code can't be interfered with by the
    page's React/Next.js hydration.

    The returned text is suitable for passing to an LLM for structured
    extraction.
    """
    from playwright.sync_api import sync_playwright

    if config is None:
        config = BROWSER_CONFIGS.get(provider)
    if config is None:
        raise ValueError(f"No browser config for provider {provider}")

    log.info("Launching headless browser for %s: %s", provider, config.url)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.goto(config.url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)

        if config.click_selector:
            tab = page.query_selector(config.click_selector)
            if tab:
                log.info("Clicking: %s", config.click_selector)
                tab.click()
                page.wait_for_timeout(config.click_wait_ms)
            else:
                log.warning("Click selector not found: %s", config.click_selector)

        # Create a CDP isolated world for extraction
        cdp = context.new_cdp_session(page)
        tree = cdp.send("Page.getFrameTree")
        frame_id = tree["frameTree"]["frame"]["id"]

        world = cdp.send("Page.createIsolatedWorld", {
            "frameId": frame_id,
            "worldName": f"pricing-extractor-{provider}",
        })
        ctx_id = world["executionContextId"]
        log.info("Created isolated world (context %d) for %s", ctx_id, provider)

        # Extract text from the isolated world
        selector = config.content_selector or "body"
        extract_js = f"""
        (() => {{
            const el = document.querySelector({json.dumps(selector)});
            return el ? el.innerText : '';
        }})()
        """

        result = cdp.send("Runtime.evaluate", {
            "expression": extract_js,
            "contextId": ctx_id,
            "returnByValue": True,
        })

        body_text = result["result"]["value"]
        log.info("Extracted %d chars of rendered text for %s (isolated world)",
                 len(body_text), provider)

        browser.close()

    return body_text

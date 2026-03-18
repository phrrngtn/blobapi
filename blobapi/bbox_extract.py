"""
Extract text bounding boxes from rendered web pages via CDP isolated worlds.

Injects a TreeWalker + Range.getClientRects() script into a CDP isolated
execution context to extract spatial layout data from any rendered page.
The page's own scripts cannot detect or interfere with the extraction.

Each text node yields:
  - Bounding rectangle (x, y, w, h) in CSS pixels
  - Text content
  - Font metrics (family, size, weight, color)
  - DOM context (parent tag, class)

This is the raw spatial data that blobrange/blobboxes would use to find
table-like regions via schema-driven matching.
"""

import json
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)

# The extraction JS — runs in a CDP isolated world.
# TreeWalker visits every visible text node; Range.getClientRects()
# gives the bounding box without triggering layout reflow.
BBOX_EXTRACTION_JS = """
(() => {
    const results = [];
    const walker = document.createTreeWalker(
        document.body,
        NodeFilter.SHOW_TEXT,
        {
            acceptNode: (node) => {
                const text = node.textContent.trim();
                if (!text || text.length > 200) return NodeFilter.FILTER_REJECT;
                const parent = node.parentElement;
                if (!parent) return NodeFilter.FILTER_REJECT;
                const style = window.getComputedStyle(parent);
                if (style.display === 'none' || style.visibility === 'hidden'
                    || style.opacity === '0') return NodeFilter.FILTER_REJECT;
                return NodeFilter.FILTER_ACCEPT;
            }
        }
    );

    while (walker.nextNode()) {
        const textNode = walker.currentNode;
        const text = textNode.textContent.trim();
        if (!text) continue;

        const range = document.createRange();
        range.selectNodeContents(textNode);
        const rects = range.getClientRects();
        if (rects.length === 0) continue;

        const rect = rects[0];
        if (rect.width === 0 || rect.height === 0) continue;

        const parent = textNode.parentElement;
        const style = window.getComputedStyle(parent);

        results.push({
            text: text,
            x: Math.round(rect.x * 10) / 10,
            y: Math.round(rect.y * 10) / 10,
            w: Math.round(rect.width * 10) / 10,
            h: Math.round(rect.height * 10) / 10,
            font_family: style.fontFamily.split(',')[0].trim().replace(/['\"]/g, ''),
            font_size: parseFloat(style.fontSize),
            font_weight: style.fontWeight,
            color: style.color,
            tag: parent.tagName.toLowerCase(),
            cls: (parent.className || '').substring(0, 80),
        });
    }

    return JSON.stringify(results);
})()
"""


@dataclass
class TextBBox:
    """A text node's bounding box with font and DOM context."""
    text: str
    x: float
    y: float
    w: float
    h: float
    font_family: str
    font_size: float
    font_weight: str
    color: str
    tag: str
    cls: str


def extract_bboxes(
    url: str,
    click_selector: str | None = None,
    click_wait_ms: int = 3000,
    viewport_width: int = 1440,
    viewport_height: int = 900,
) -> list[TextBBox]:
    """
    Extract all visible text bounding boxes from a rendered page.

    Uses headless Chromium + CDP isolated world. The extraction JS
    is invisible to the page's own scripts.

    Returns a list of TextBBox, sorted by (y, x) — reading order.
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": viewport_width, "height": viewport_height}
        )
        page = context.new_page()

        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)

        if click_selector:
            tab = page.query_selector(click_selector)
            if tab:
                tab.click()
                page.wait_for_timeout(click_wait_ms)

        # Create isolated world
        cdp = context.new_cdp_session(page)
        tree = cdp.send("Page.getFrameTree")
        frame_id = tree["frameTree"]["frame"]["id"]
        world = cdp.send("Page.createIsolatedWorld", {
            "frameId": frame_id,
            "worldName": "bbox-extractor",
        })
        ctx_id = world["executionContextId"]

        result = cdp.send("Runtime.evaluate", {
            "expression": BBOX_EXTRACTION_JS,
            "contextId": ctx_id,
            "returnByValue": True,
        })

        raw = json.loads(result["result"]["value"])
        browser.close()

    bboxes = [TextBBox(**b) for b in raw]
    bboxes.sort(key=lambda b: (b.y, b.x))

    log.info("Extracted %d text bboxes from %s", len(bboxes), url)
    return bboxes

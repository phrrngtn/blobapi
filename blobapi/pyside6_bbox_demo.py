"""
PySide6 bbox extraction demo.

Demonstrates the same JS extraction as bbox_extract.py (Playwright) but
using PySide6/Qt WebEngine as the browser host. The extraction JS is
identical — only the injection mechanism differs.

IMPORTANT: Qt WebEngine's offscreen platform does not render page content
properly (the GPU compositor doesn't initialize). This demo requires a
real display (or Xvfb on Linux). For headless/CI use, use bbox_extract.py
(Playwright) instead.

Usage (with display):
    QT_QPA_PLATFORM=cocoa uv run python -m blobapi.pyside6_bbox_demo \\
        https://mistral.ai/pricing \\
        --click "button:has-text('API pricing')"

The same JS injection pattern works in a Qt CTP (Content Task Pane)
embedded in Excel or any other Qt WebEngine host application.
"""
import json
import os
import sys

# Suppress macOS dock icon bouncing
os.environ.setdefault("QT_MAC_DISABLE_FOREGROUND_APPLICATION_TRANSFORM", "1")

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QUrl, QTimer
from PySide6.QtWebEngineCore import QWebEnginePage, QWebEngineScript
from PySide6.QtWebEngineWidgets import QWebEngineView

# The extraction JS — identical to bbox_extract.BBOX_EXTRACTION_JS
# Shared between Playwright and PySide6 controllers.
SNAPSHOT_JS = """
(() => {
    const results = [];
    const walker = document.createTreeWalker(
        document.body, NodeFilter.SHOW_TEXT,
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
            t_ms: Math.round(performance.now() * 100) / 100,
        });
    }

    return JSON.stringify(results);
})()
"""


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "https://mistral.ai/pricing"

    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)

    view = QWebEngineView()
    view.resize(1440, 900)
    # Don't show the window — headless-ish
    # (remove this line to see the page rendered)
    # view.show()

    page = view.page()

    def on_load_finished(ok):
        if not ok:
            print("Page load failed")
            app.quit()
            return

        print(f"Page loaded: {url}")

        def take_snapshot():
            def on_result(result_json):
                bboxes = json.loads(result_json)
                print(f"Extracted {len(bboxes)} text bboxes")

                pricing = [b for b in bboxes
                           if "$" in b["text"] or b["font_size"] >= 20]
                print(f"Pricing-related: {len(pricing)}")
                for b in sorted(pricing, key=lambda b: (b["y"], b["x"])):
                    print(f"  ({b['x']:7.1f},{b['y']:7.1f}) "
                          f"{b['w']:6.1f}x{b['h']:5.1f}  "
                          f"{b['font_size']:5.1f}px {b['font_weight']:>3s}  "
                          f"{b['tag']:6s}  {b['text'][:60]}")

                app.quit()

            # Run in ApplicationWorld (isolated from page)
            page.runJavaScript(
                SNAPSHOT_JS,
                QWebEngineScript.ScriptWorldId.ApplicationWorld,
                on_result,
            )

        # Wait for JS to settle
        QTimer.singleShot(3000, take_snapshot)

    page.loadFinished.connect(on_load_finished)
    page.load(QUrl(url))
    app.exec()


if __name__ == "__main__":
    main()

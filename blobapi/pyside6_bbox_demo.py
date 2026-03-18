"""
PySide6 bbox extraction demo.

Demonstrates the same JS extraction as the Playwright controller but
using PySide6/Qt WebEngine as the browser host. Both use the shared
blobboxes browser bundle — only the injection mechanism differs.

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

from blobboxes.browser import _load_bundle


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "https://mistral.ai/pricing"

    # Load the shared bundle once
    bundle_js = _load_bundle(full=False)

    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)

    view = QWebEngineView()
    view.resize(1440, 900)
    # Don't show the window — headless-ish
    # (remove this line to see the page rendered)
    # view.show()

    page = view.page()

    # Inject bundle as a persistent script in ApplicationWorld
    script = QWebEngineScript()
    script.setSourceCode(bundle_js)
    script.setWorldId(QWebEngineScript.ScriptWorldId.ApplicationWorld)
    script.setInjectionPoint(QWebEngineScript.InjectionPoint.DocumentReady)
    script.setName("blobboxes-bundle")
    page.scripts().insert(script)

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
                           if "$" in b["text"] or b.get("font_size", 0) >= 20]
                print(f"Pricing-related: {len(pricing)}")
                for b in sorted(pricing, key=lambda b: (b["y"], b["x"])):
                    print(f"  ({b['x']:7.1f},{b['y']:7.1f}) "
                          f"{b['w']:6.1f}x{b['h']:5.1f}  "
                          f"{b['font_size']:5.1f}px {b['font_weight']:>3s}  "
                          f"{b['tag']:6s}  {b['text'][:60]}")

                app.quit()

            # Init + snapshot via the shared bundle
            page.runJavaScript(
                "blobboxes.init(); JSON.stringify(blobboxes.snapshot())",
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

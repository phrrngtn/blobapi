"""
Extract text bounding boxes from rendered web pages via CDP isolated worlds.

Thin wrapper around blobboxes.browser — the shared JS bundle and Playwright
controller now live in the blobboxes package.

This module re-exports the extract_bboxes function and TextBBox dataclass
for backward compatibility.
"""

from blobboxes.browser import TextBBox, extract_bboxes

__all__ = ["TextBBox", "extract_bboxes"]

#!/usr/bin/env python3
"""Readable-content extraction for the web evidence cache (see docs/WebCache.md).

Turns a fetched document into the ``title`` / ``text`` / ``last_updated`` an
evidence quote is pulled from. Today that's HTML via trafilatura; this is the
seam where other document types (e.g. PDFs) slot in as additional extractors.
Date extraction is deliberately conservative — a real date the page states, or
None, never a padded guess.
"""

from __future__ import annotations

from typing import NamedTuple


class ExtractedMeta(NamedTuple):
    """Readable content pulled from a page. Any field may be None."""

    title: str | None
    last_updated: str | None
    text: str | None


def extract(html: str, url: str) -> ExtractedMeta:
    """Run trafilatura for text/title; extract the date conservatively.

    ``last_updated`` is a real date stated on the page, or None — never a guess.
    trafilatura's default date extraction pads a weak year-only signal (a stray
    "© 2024", a bare-year meta) up to a fabricated `YYYY-01-01`, which would
    corrupt the "is this still live / need a 2023+ source" judgment. We instead
    ask htmldate with ``extensive_search=False``, which returns None rather than
    pad — for evidence, no date beats a wrong one. ``original_date`` is left False
    (htmldate's default) so we get the page's most recent date, matching the
    ``last_updated`` column name and the recency check.
    """
    import htmldate
    import trafilatura

    title: str | None = None
    text: str | None = None
    doc = trafilatura.bare_extraction(html, url=url, with_metadata=True)
    if doc is not None:
        title = getattr(doc, "title", None)
        text = getattr(doc, "text", None)
    # Fall back to a plain text extraction if metadata extraction yielded none.
    if not text:
        text = trafilatura.extract(html, url=url)
    try:
        last_updated = htmldate.find_date(html, extensive_search=False)
    except Exception:
        last_updated = None
    return ExtractedMeta(title=title, last_updated=last_updated, text=text)

#!/usr/bin/env python3
"""Readable-content extraction for the web evidence cache (see docs/WebCache.md).

Turns a fetched document into the ``title`` / ``text`` / ``last_updated`` an
evidence quote is pulled from. HTML goes through trafilatura (``extract``); PDFs
through pypdf (``extract_pdf``); the caller dispatches by content-type, and this
is the seam where further document types slot in as additional extractors. Date
extraction is deliberately conservative — a real date the document states, or
None, never a padded guess.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from pypdf import DocumentInformation


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


def extract_pdf(raw: bytes) -> ExtractedMeta:
    """Extract title / text / date from a PDF's raw bytes (pypdf).

    The PDF arm of extraction. web_http stores the bytes verbatim; this pulls the
    readable text (for FTS + verbatim quotes), the document title, and a
    conservative date from the PDF's own metadata. ``last_updated`` prefers
    ``/ModDate`` (last-modified) over ``/CreationDate``, matching the HTML path's
    preference for the document's most recent date, and is None when the PDF
    states no parseable date, never a guess.

    A malformed, encrypted, or image-only (scanned) PDF yields empty text rather
    than raising, so the blob is still stored and the caller's thin-content
    warning fires — one bad document never crashes a batch.
    """
    import io

    from pypdf import PdfReader

    try:
        reader = PdfReader(io.BytesIO(raw))
        text = "\n".join(page.extract_text() or "" for page in reader.pages).strip()
        info = reader.metadata
        # Read the title inside the try too: it decodes a PDF text string, which can
        # raise on malformed UTF-16 just like page extraction.
        title = info.title if info is not None else None
    except Exception:
        # pypdf raises an assortment (PdfReadError, stream errors, KeyError, ...)
        # on broken/encrypted PDFs; treat any as "no extractable content" so the
        # blob is still kept and the thin-content path flags it.
        return ExtractedMeta(title=None, last_updated=None, text=None)
    return ExtractedMeta(
        title=title or None,
        last_updated=_pdf_date(info),
        text=text or None,
    )


def _pdf_date(info: DocumentInformation | None) -> str | None:
    """The PDF's own date as ``YYYY-MM-DD``, or None.

    Field priority: ``/ModDate`` (last-modified) then ``/CreationDate`` — not a
    comparison of the two, but ModDate is the page's most recent date, matching
    the HTML path. pypdf parses each into a ``datetime`` (or None); a malformed
    date string can raise on access, which we swallow — for evidence, no date
    beats a wrong one.
    """
    if info is None:
        return None
    for attr in ("modification_date", "creation_date"):
        try:
            dt = getattr(info, attr)
        except Exception:
            dt = None  # a malformed date string can raise on parse — treat as absent
        if isinstance(dt, datetime):
            return dt.date().isoformat()
    return None

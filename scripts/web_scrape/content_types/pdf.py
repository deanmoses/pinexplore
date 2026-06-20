#!/usr/bin/env python3
"""PDF content handler for the web evidence cache (see docs/WebCache.md).

Everything PDF-specific lives here: the ``%PDF-`` signature that reclassifies a
PDF whatever its header claimed, storing the bytes verbatim (no charset decode),
and pulling text / title / date with pypdf. Rulesheets, flyers, and press
releases come in this way. The transport and the fetcher reach this only through
the ``ContentHandler`` interface — they never mention PDF.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from .base import ContentHandler, ExtractedMeta

if TYPE_CHECKING:
    from pypdf import DocumentInformation


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


def _extract_pdf(raw: bytes) -> ExtractedMeta:
    """Extract title / text / date from a PDF's raw bytes (pypdf).

    The bytes are stored verbatim by the transport; this pulls the readable text
    (for FTS + verbatim quotes), the document title, and a conservative date from
    the PDF's own metadata. ``last_updated`` prefers ``/ModDate`` (last-modified)
    over ``/CreationDate``, matching the HTML path's preference for the document's
    most recent date, and is None when the PDF states no parseable date.

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


class PdfHandler(ContentHandler):
    """PDF documents: bytes stored verbatim, parsed by pypdf, never rendered.

    Recognized by the ``%PDF-`` signature even when the header lies (octet-stream,
    a wrong text/* label, or nothing), so a citable document served sloppily isn't
    skipped. Not renderable: a scanned PDF reads as thin too, but a browser can't
    extract its text either (that needs OCR, out of scope).
    """

    mime_types = frozenset({"application/pdf"})
    canonical_mime = "application/pdf"
    signature = b"%PDF-"
    extension = "pdf"
    renderable = False

    def extract(self, raw: bytes, text: str | None, url: str) -> ExtractedMeta:
        return _extract_pdf(raw)

    def thin_warning(
        self, url: str, *, rendered: bool, render_attempted: bool
    ) -> str | None:
        # The PDF analog of a still-thin render: an image-only/scanned PDF (no OCR)
        # or one pypdf couldn't parse. Loud, so a 0-quote PDF isn't silent. Render
        # flags don't apply — a PDF is never rendered.
        return f"WARNING: PDF extracted to little/no text (scanned?): {url}"

#!/usr/bin/env python3
"""PDF content handler for the web evidence cache (see docs/WebCache.md).

Everything PDF-specific lives here: the ``%PDF-`` signature that reclassifies a
PDF whatever its header claimed, storing the bytes verbatim (no charset decode),
and pulling text / title / date. Rulesheets, flyers, and press releases come in
this way. The transport and the fetcher reach this only through the
``ContentHandler`` interface — they never mention PDF.

Text and metadata come from **different backends, deliberately**. The words are
poppler's job (``web_pdftext``), because reading a page as it was printed is
what keeps a flyer's columns attached to their own headings. The title and date
are pypdf's, because they are Info-dict fields rather than anything on the page.
Splitting them also means neither can take the other down: a PDF whose Info dict
is malformed still yields its full text, and a document poppler chokes on still
yields the title it declares.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, override

# Allow `import web_pdftext` (a flat sibling module) whether run as a script or
# imported — the same dance the image handler does for web_ocr.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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


def _pdf_meta(raw: bytes) -> tuple[str | None, str | None]:
    """The PDF's declared title and date, as ``(title, last_updated)``.

    Both come from the Info dict via pypdf, and both are best-effort: a broken,
    encrypted or Info-less document simply declares nothing, which is a normal
    answer rather than a failure. Never raises — pypdf throws an assortment
    (``PdfReadError``, stream errors, ``KeyError``) on damaged files, and reading
    ``title`` decodes a PDF text string that can fail on malformed UTF-16 all by
    itself, so the read sits inside the guard with the parse.
    """
    import io

    from pypdf import PdfReader

    try:
        info = PdfReader(io.BytesIO(raw)).metadata
        title = info.title if info is not None else None
    except Exception:
        return None, None
    return (title or None), _pdf_date(info)


def _extract_pdf(raw: bytes, url: str) -> ExtractedMeta:
    """Extract title / text / date from a PDF's raw bytes.

    The bytes are stored verbatim by the transport; this pulls the readable text
    (for FTS + verbatim quotes) via poppler, plus the document's own title and a
    conservative date. ``last_updated`` prefers ``/ModDate`` (last-modified) over
    ``/CreationDate``, matching the HTML path's preference for the document's
    most recent date, and is None when the PDF states no parseable date.

    Nothing here raises — one bad document never crashes a batch — but the two
    ways of getting no text are kept apart, because only one of them is a fact
    about the document. An image-only (scanned) PDF genuinely has no text layer:
    ``text=None`` is the finding, and the thin-content warning surfaces it. A
    missing poppler or a document poppler can't read produced no *result*, so
    they report ``unavailable`` and the stored text — which the identical bytes
    once supported — is left alone.
    """
    import web_pdftext

    title, last_updated = _pdf_meta(raw)
    try:
        text = web_pdftext.pdf_text(raw)
    except web_pdftext.PdfTextUnavailableError as exc:
        print(f"WARNING: no PDF text extraction for {url}: {exc}", file=sys.stderr)
        return ExtractedMeta(title, last_updated, None, unavailable=True)
    except web_pdftext.PdfTextFailedError as exc:
        print(f"WARNING: could not read PDF {url}: {exc}", file=sys.stderr)
        return ExtractedMeta(title, last_updated, None, unavailable=True)
    return ExtractedMeta(title=title, last_updated=last_updated, text=text)


class PdfHandler(ContentHandler):
    """PDF documents: bytes stored verbatim, read by poppler, never rendered.

    Recognized by the ``%PDF-`` signature even when the header lies (octet-stream,
    a wrong text/* label, or nothing), so a citable document served sloppily isn't
    skipped. Not renderable: a scanned PDF reads as thin too, but a browser can't
    extract its text either (that needs OCR, out of scope).
    """

    mime_types = frozenset({"application/pdf"})
    canonical_mime = "application/pdf"
    signature: bytes | None = b"%PDF-"
    extension = "pdf"
    text_source = "pdf"
    renderable = False
    # poppler over the stored bytes is deterministic, so the corpus can be moved
    # to a new extraction wholesale. A host without poppler re-extracts to
    # nothing and the backfill's never-blank guard leaves those rows alone.
    backfillable = True

    @override
    def extract(self, raw: bytes, text: str | None, url: str) -> ExtractedMeta:
        return _extract_pdf(raw, url)

    @override
    def thin_warning(
        self, url: str, *, rendered: bool, render_attempted: bool
    ) -> str | None:
        # The PDF analog of a still-thin render: an image-only/scanned PDF, whose
        # words need OCR this path doesn't do. Loud, so a 0-quote PDF isn't
        # silent. A PDF poppler *failed* on doesn't reach here — that reports
        # unavailable, which keeps the stored text rather than replacing it with
        # a thin one. Render flags don't apply — a PDF is never rendered.
        return f"WARNING: PDF extracted to little/no text (scanned?): {url}"

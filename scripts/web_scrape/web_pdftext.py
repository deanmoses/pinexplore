#!/usr/bin/env python3
"""PDF text-layer backend for the web evidence cache (see docs/WebCache.md).

Turns a PDF's bytes into the text the cache indexes and quotes, using poppler's
``pdftotext`` in its default reading-order mode: text comes out column by
column, so a two-column page never splices unrelated columns into one output
line — the failure mode that puts a verbatim-looking quote under the wrong
heading. What reading order gives up is the page as printed: a table read this
way is a column of unrelated cells, and a vector checkmark was never in the
text layer at all. Those questions are answered by rendering the page image
from the raw blob instead (see WebCache.md), which is why the one piece of
layout this module preserves is the page boundary: poppler terminates every
page with a form feed, and ``_normalize`` keeps each as a line of its own so
``web_cache.quote_hits()`` can say which PDF page a hit sits on.
"""

from __future__ import annotations

import subprocess

# stdin/stdout, so the bytes read are exactly the caller's — no temp file.
_ARGV = ("pdftotext", "-enc", "UTF-8", "-", "-")

# Liveness, not tuning: the heaviest manual in the corpus takes a second, so this
# only fires on a document that would otherwise hang the batch.
_TIMEOUT_SECONDS = 60


class PdfTextUnavailableError(RuntimeError):
    """poppler isn't installed here — a fact about the host, not the document.

    Callers must treat it as no opinion about this document, never as "it has no
    text": the identical bytes yield text on a host that has poppler, and the
    cache is shared through R2.
    """


class PdfTextFailedError(RuntimeError):
    """poppler ran and could not read these bytes — truncated, malformed, encrypted.

    Promises only that no text was produced. An image-only PDF is read perfectly
    and correctly yields nothing, which is a finding, not this.
    """


def _normalize(out: str) -> str | None:
    """poppler's stdout as storable text, or None when it holds nothing.

    Page markers survive: poppler terminates every page — the last included —
    with a form feed, and each one is kept as a line of its own, so a page's
    text starts on the line after its predecessor's marker and page count
    equals marker count. A blank page still contributes its marker; dropping
    it would shift every later page's ordinal position in the file, which is
    the one thing the markers promise (the cached Time Machine manual has
    three blank pages mid-document). Hence the page-at-a-time shape below:
    ``str.splitlines()`` and per-line ``rstrip()`` both eat form feeds
    outright, so neither may ever see one.

    Within a page, leading whitespace is kept (trailing is page-width padding)
    and blank lines at both edges are trimmed. Output that is nothing but
    whitespace and form feeds still returns None, not a skeleton of empty
    pages — an image-only PDF is a finding, and the caller's thin-content
    warning depends on it.
    """
    pages = out.split("\f")
    kept: list[str] = []
    for i, page in enumerate(pages):
        lines = [line.rstrip() for line in page.split("\n")]
        while lines and not lines[0]:
            lines.pop(0)
        while lines and not lines[-1]:
            lines.pop()
        kept.extend(lines)
        if i < len(pages) - 1:
            kept.append("\f")
    text = "\n".join(kept)
    return text if text.strip() else None


def pdf_text(raw: bytes) -> str | None:
    """The readable text of a PDF, or None when it has no text layer.

    Raises ``PdfTextUnavailableError`` if poppler isn't installed on this host,
    or ``PdfTextFailedError`` if it is but can't read these bytes.
    """
    try:
        # S603 waived: argv is the literal above, and the untrusted bytes go to
        # stdin, so a document can never contribute an argument.
        proc = subprocess.run(  # noqa: S603
            _ARGV,
            input=raw,
            capture_output=True,
            check=False,
            timeout=_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise PdfTextUnavailableError(
            "PDF text extraction needs poppler's `pdftotext` on PATH "
            "(`brew install poppler`, or `apt-get install poppler-utils`). "
            "The blob is still stored; run web_backfill.py once poppler is "
            "installed to fill in the text."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        # Failed, not Unavailable: poppler is here, it just never finished.
        raise PdfTextFailedError(
            f"pdftotext did not finish within {_TIMEOUT_SECONDS}s"
        ) from exc

    text = _normalize(proc.stdout.decode("utf-8", errors="replace"))
    if text is not None:
        # poppler exits non-zero for damage it recovered from, so output that
        # exists is kept: a partly-broken manual still yields its readable pages.
        return text
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", errors="replace").strip().splitlines()
        raise PdfTextFailedError(
            f"pdftotext exited {proc.returncode}: {detail[0] if detail else 'no detail'}"
        )
    # Clean exit, no text: an image-only document, which is an answer.
    return None

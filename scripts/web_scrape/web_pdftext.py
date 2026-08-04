#!/usr/bin/env python3
"""PDF text-layer backend for the web evidence cache (see docs/WebCache.md).

Turns a PDF's bytes into the text the cache indexes and quotes, using poppler's
``pdftotext -layout``.

``-layout`` is load-bearing, and worth a system binary when pypdf is already a
dependency. A PDF has no structure, only glyphs at coordinates, so its "text" is
always a reconstruction; reading them in content-stream order — what a pure
Python reader does — gets every word right and the meaning wrong. On Jersey
Jack's Sonic flyer that order put the Collector's Edition bullets under the
heading ``SPECIAL EDITION`` and vice versa: verbatim strings a quote gate
passes, with the attribution silently swapped. ``-layout`` rebuilds the page as
printed, so a heading keeps its own list.
"""

from __future__ import annotations

import subprocess

# stdin/stdout, so the bytes read are exactly the caller's — no temp file.
_ARGV = ("pdftotext", "-layout", "-enc", "UTF-8", "-", "-")

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

    Leading whitespace is the column alignment and must survive — which is why
    the ends are trimmed a line at a time. ``str.strip()`` on the joined text
    would take the first content line's indentation with the blank lines above
    it, shifting it out of its column.
    """
    lines = [line.rstrip() for line in out.replace("\f", "\n").splitlines()]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines) or None


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

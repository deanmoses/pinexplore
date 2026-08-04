#!/usr/bin/env python3
"""PDF text-layer backend for the web evidence cache (see docs/WebCache.md).

Turns a PDF's bytes into the text the cache indexes and quotes, using
**poppler's ``pdftotext -layout``**.

A PDF has no structure — only glyphs at coordinates — so "the text of this
document" is always a reconstruction, and the only real question is which
reconstruction. ``-layout`` rebuilds the page as it was printed: columns stay
columns, and a heading stays above the list it introduces. The obvious
alternative, reading glyphs in content-stream order, produces text whose *words*
are all correct and whose *meaning* is not. Measured on Jersey Jack's Sonic
comparison flyer, content-stream order emitted the heading ``SPECIAL EDITION``
followed by the Collector's Edition bullets and ``COLLECTOR'S EDITION`` followed
by the Special Edition ones — the two editions' feature lists swapped. Every
string was verbatim, so a quote gate would pass it; only the attribution was
wrong, which is the half that a catalog correction is actually made of.

The cost is a system binary. Everything else here needs at most a wheel, so
poppler is the one thing an operator must install (``brew install poppler``,
``apt-get install poppler-utils``). It buys correctness on the documents makers
actually publish, and the failure is loud and recoverable: a host without it
stores the blob and extracts nothing, and ``web_backfill.py`` fills the text in
once poppler is there.

Two limits worth knowing before quoting. ``-layout`` pads with spaces to hold
columns apart, so a line may carry text from two columns at once — fine to read,
but a quote lifted blindly across a gutter can splice unrelated columns into one
sentence. And a scanned/image-only PDF has no text layer at all; it extracts to
nothing here, and its words need OCR (``web_ocr``).
"""

from __future__ import annotations

import subprocess

# Read the PDF on stdin and write text on stdout, so nothing touches the
# filesystem: the bytes we extract from are exactly the bytes the caller holds,
# with no temp file to leak or collide.
_ARGV = ("pdftotext", "-layout", "-enc", "UTF-8", "-", "-")


class PdfTextUnavailableError(RuntimeError):
    """Text extraction was needed but poppler can't be used *here*.

    A fact about the host, not about any one document: ``pdftotext`` is not on
    PATH. It won't fix itself mid-run, so callers treat it as "no opinion about
    this document" and must never let it blank text the identical bytes already
    produced on a host that had poppler.
    """


class PdfTextFailedError(RuntimeError):
    """poppler ran and could not read these particular bytes.

    A truncated, malformed or permission-encrypted document. Promises only that
    **no text was produced**, which is not "this document has no text" — an
    image-only PDF is read perfectly and correctly yields nothing, and that is a
    finding rather than a failure. Separate from ``PdfTextUnavailableError``
    because the operator is asked for different things (look at this file versus
    install poppler), not because callers behave differently.
    """


def _normalize(out: str) -> str | None:
    """poppler's stdout as storable text, or None when it holds nothing.

    Page breaks arrive as form feeds; they become blank lines so the text stays
    plain and ``sentences()`` splits on them like any other break. Trailing
    whitespace goes because ``-layout`` right-pads to the widest line on the
    page, which is alignment nobody reads. Leading whitespace **stays** — it is
    the column alignment, the entire reason for using this mode.
    """
    lines = [line.rstrip() for line in out.replace("\f", "\n").splitlines()]
    return "\n".join(lines).strip() or None


def pdf_text(raw: bytes) -> str | None:
    """The readable text of a PDF, or None when it has no text layer.

    Raises ``PdfTextUnavailableError`` if poppler isn't installed on this host,
    or ``PdfTextFailedError`` if it is but can't read these bytes.
    """
    try:
        # S603 is waived below: argv is the module-level literal above with no
        # interpolation, and the untrusted bytes are piped to stdin, so a
        # document can never contribute an argument.
        proc = subprocess.run(  # noqa: S603
            _ARGV, input=raw, capture_output=True, check=False
        )
    except FileNotFoundError as exc:
        raise PdfTextUnavailableError(
            "PDF text extraction needs poppler's `pdftotext` on PATH "
            "(`brew install poppler`, or `apt-get install poppler-utils`). "
            "The blob is still stored; run web_backfill.py once poppler is "
            "installed to fill in the text."
        ) from exc

    text = _normalize(proc.stdout.decode("utf-8", errors="replace"))
    if text is not None:
        # poppler reports a non-zero status for damage it recovered from, so
        # output that exists is output we keep: a partly-broken manual still
        # yields the pages it could read, and dropping them would lose evidence
        # the bytes plainly support.
        return text
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", errors="replace").strip().splitlines()
        raise PdfTextFailedError(
            f"pdftotext exited {proc.returncode}: {detail[0] if detail else 'no detail'}"
        )
    # Clean exit, no text: a real answer about a real document (image-only/
    # scanned). The caller's thin-content warning is what surfaces it.
    return None

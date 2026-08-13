#!/usr/bin/env python3
"""Image content handlers for the web evidence cache (see docs/WebCache.md).

Scanned flyers, photographed manual pages, screenshots of pages that won't
scrape — images are evidence whose text is *printed*, so they get their text
from OCR (``web_ocr``, macOS Vision) instead of a parser. The bytes are stored
verbatim as the blob, exactly like a PDF, so a quote always re-verifies against
the original picture.

One handler class per image format, sharing the OCR extraction: the format is
what decides the blob extension (a PNG blob must not be stored as ``.jpg``) and
the magic-byte signature, and nothing else differs. Adding TIFF later is another
two-line subclass.

OCR'd words are findable, but they are the machine's reading: they land in
``ocr_text`` (the machine-read tier) with ``text`` and ``text_source`` NULL,
because measured line-exactness against ground truth is ~88% — a
wrong-but-plausible reading (``1/16"`` for ``11/16"``) reads perfectly fine.
An image acquires a text layer only through a human transcription filed with
``web_import.py`` (``text_source='manual'``).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import override

# Allow `import web_ocr` (a flat sibling module) whether run as a script or
# imported — the same dance the other web_scrape modules do.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from .base import ContentHandler, ExtractedMeta


def _extract_image(raw: bytes, url: str) -> ExtractedMeta:
    """OCR an image's bytes into evidence text; title and date stay None.

    Neither is guessed: OCR'd words are not a document title, and an image's
    EXIF timestamp is when the photo/scan was taken, not the date the document
    states — for evidence, no date beats a wrong one (the same rule the HTML and
    PDF handlers follow). A manual import can supply both by hand.

    Neither failure raises — one image can't kill a fetch batch — and both report
    ``unavailable``, because neither produced a *result*. They stay separate
    types only because they ask the operator for different things: install the
    backend (or transcribe by hand) versus look at this particular file. What
    they must not do is masquerade as "this document has no text", which is a
    finding, and would let a failed run blank text the identical bytes support.
    """
    import web_ocr

    try:
        text = web_ocr.ocr_text(raw)
    except web_ocr.OcrUnavailableError as exc:
        print(f"WARNING: no OCR for {url}: {exc}", file=sys.stderr)
        return ExtractedMeta(title=None, last_updated=None, text=None, unavailable=True)
    except web_ocr.OcrFailedError as exc:
        print(f"WARNING: OCR failed for {url}: {exc}", file=sys.stderr)
        return ExtractedMeta(title=None, last_updated=None, text=None, unavailable=True)
    return ExtractedMeta(title=None, last_updated=None, text=None, ocr_text=text)


class _ImageHandler(ContentHandler):
    """Shared behavior for every image format: OCR in, bytes stored verbatim.

    Not renderable — a headless browser can't read a JPEG any better than we
    can; OCR is the only path to text, so a thin result never escalates.

    ``text_source = None``: OCR is this type's entire reading and it lands in
    ``ocr_text``, so nothing derives a text layer and stamping a label would
    assert otherwise. Images keep OCRing inline at fetch time — one image is
    one Vision call, with no batch to defer.
    """

    text_source: str | None = None
    renderable = False

    @override
    def extract(self, raw: bytes, text: str | None, url: str) -> ExtractedMeta:
        return _extract_image(raw, url)

    @override
    def thin_warning(
        self, url: str, *, rendered: bool, render_attempted: bool
    ) -> str | None:
        # Loud, so a 0-quote image isn't silent: an image whose text OCR can't
        # read is not quotable evidence, whatever it depicts. Render flags don't
        # apply — an image is never rendered.
        return f"WARNING: OCR found little/no text in image: {url}"


class JpegHandler(_ImageHandler):
    """JPEG images — scans and photographs, the common flyer format.

    ``image/jpg`` is claimed alongside the real ``image/jpeg`` because servers
    do send it; both route here and both store a ``.jpg`` blob, so the alias
    can't split one picture across two extensions.
    """

    mime_types = frozenset({"image/jpeg", "image/jpg"})
    canonical_mime = "image/jpeg"
    # SOI + the start of any APPn/DQT marker: every JPEG begins FF D8 FF.
    signature: bytes | None = b"\xff\xd8\xff"
    extension = "jpg"


class PngHandler(_ImageHandler):
    """PNG images — screenshots, and lossless scans."""

    mime_types = frozenset({"image/png"})
    canonical_mime = "image/png"
    signature: bytes | None = b"\x89PNG\r\n\x1a\n"
    extension = "png"


class WebpHandler(_ImageHandler):
    """WebP images — often a modern outlet's only copy, and usually its largest.

    A news CDN serves WebP at full resolution while the JPEGs alongside it are
    downscaled, so the WebP is the copy a flyer is legible in. Vision needs no
    extra backend for it (macOS ImageIO decodes ``org.webmproject.webp``), and
    an animated one reads its first frame.
    """

    mime_types = frozenset({"image/webp"})
    canonical_mime = "image/webp"
    # A RIFF container: "RIFF", a 4-byte length, then the form tag at offset 8.
    # The prefix alone is WAV and AVI too, and the length bytes in between can
    # be anything — so the tag is what decides, and ``signature`` is only the
    # marker that this type is sniffable at all.
    signature: bytes | None = b"RIFF"
    signature_span: int | None = 12
    extension = "webp"

    @override
    def matches(self, raw: bytes) -> bool:
        return raw[:4] == b"RIFF" and raw[8:12] == b"WEBP"

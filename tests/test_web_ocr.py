"""Tests for web_ocr — the macOS Vision OCR backend.

The pure line-filtering logic runs everywhere; the actual Vision bridge is
exercised by one Darwin-only test against a tiny checked-in PNG, so the
platform glue is covered where it can run and skipped where it can't.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import web_ocr

SAMPLE_PNG = Path(__file__).parent / "data" / "ocr_sample.png"

darwin_only = pytest.mark.skipif(
    sys.platform != "darwin", reason="macOS Vision is only available on Darwin"
)


# --------------------------------------------------------------------------- #
# Confidence filtering (pure) — art noise never reaches the evidence text
# --------------------------------------------------------------------------- #


def test_filter_drops_lines_below_the_confidence_floor():
    # The real failure this exists for: stylized playfield/backglass art OCRs to
    # junk tokens at low confidence ("SAUTTL", "BUMON" on the Mecatronics flyer),
    # which would otherwise pollute both the FTS index and any quote.
    lines = [(1.0, "MECATRONICS INDUSTRIA"), (0.5, "SAUTTL"), (0.3, "BUMON")]
    assert web_ocr.filter_lines(lines, 0.6) == "MECATRONICS INDUSTRIA"


def test_filter_keeps_line_order_and_joins_with_newlines():
    lines = [(1.0, "first"), (0.9, "second"), (1.0, "third")]
    assert web_ocr.filter_lines(lines, 0.6) == "first\nsecond\nthird"


def test_filter_returns_none_when_everything_is_below_the_floor():
    # No text at all is None, not "" — the caller's thin-content warning keys on it.
    assert web_ocr.filter_lines([(0.3, "junk"), (0.5, "noise")], 0.6) is None


def test_filter_returns_none_for_no_lines():
    assert web_ocr.filter_lines([], 0.6) is None


def test_filter_floor_is_inclusive():
    assert web_ocr.filter_lines([(0.6, "kept")], 0.6) == "kept"


# --------------------------------------------------------------------------- #
# ocr_text — the backend proper (Vision stubbed, then real on Darwin)
# --------------------------------------------------------------------------- #


def test_ocr_text_applies_the_floor_to_recognized_lines(monkeypatch):
    monkeypatch.setattr(
        web_ocr, "_recognize", lambda raw: [(1.0, "kept"), (0.4, "dropped")]
    )
    assert web_ocr.ocr_text(b"fake-image-bytes") == "kept"


def test_ocr_text_raises_when_vision_is_unavailable(monkeypatch):
    # A missing pyobjc / non-Darwin host is a setup failure, not a per-image one:
    # it surfaces as a typed error so a CLI can report it and the image handler
    # can downgrade it to a warning rather than crashing a batch.
    def _boom(raw: bytes) -> list[tuple[float, str]]:
        raise web_ocr.OcrUnavailableError("no Vision here")

    monkeypatch.setattr(web_ocr, "_recognize", _boom)
    with pytest.raises(web_ocr.OcrUnavailableError):
        web_ocr.ocr_text(b"fake-image-bytes")


@darwin_only
def test_ocr_reads_text_from_a_real_image():
    # End-to-end through the actual Vision bridge (bytes -> NSData -> request).
    text = web_ocr.ocr_text(SAMPLE_PNG.read_bytes())
    assert text is not None
    assert "PINBALL EVIDENCE" in text.upper()


@darwin_only
def test_ocr_returns_none_for_an_image_with_no_text():
    # A blank image recognizes nothing — None, so the caller warns instead of
    # storing an empty-string page.
    blank = (SAMPLE_PNG.parent / "ocr_blank.png").read_bytes()
    assert web_ocr.ocr_text(blank) is None


# --------------------------------------------------------------------------- #
# A file we can't read is not a host that can't read
# --------------------------------------------------------------------------- #


@darwin_only
def test_undecodable_image_raises_failed_not_unavailable():
    # Vision refuses bytes that aren't a decodable image. That says nothing about
    # this host's ability to OCR, and conflating the two would let one corrupt
    # file masquerade as "no OCR backend here" — which callers treat as a reason
    # to keep whatever text they already had.
    with pytest.raises(web_ocr.OcrFailedError):
        web_ocr.ocr_text(b"\xff\xd8\xff not really a jpeg")


def test_failed_and_unavailable_are_distinct_types():
    assert not issubclass(web_ocr.OcrFailedError, web_ocr.OcrUnavailableError)
    assert not issubclass(web_ocr.OcrUnavailableError, web_ocr.OcrFailedError)

"""Tests for web_pdfocr — the PDF OCR pass filling ``pages.ocr_text``.

The pass logic (selection, the sheet-count assertion, the per-document write)
runs everywhere with a stubbed ``ocr_pdf``; the real Quartz+Vision pipeline is
exercised by one Darwin-only test against the fixture PDF, matching how
``test_web_ocr`` covers the image path.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SCRIPTS_WEB = Path(__file__).resolve().parent.parent / "scripts" / "web_scrape"
sys.path.insert(0, str(SCRIPTS_WEB))

import web_cache as wc  # noqa: E402
import web_pdfocr  # noqa: E402

darwin_only = pytest.mark.skipif(
    sys.platform != "darwin", reason="Quartz/Vision are only available on Darwin"
)


def _seed_pdf(cache, *, url: str, raw: bytes, text: str | None) -> None:
    sha = wc.content_sha(raw)
    wc.blob_path(sha, ext="pdf").parent.mkdir(parents=True, exist_ok=True)
    wc.blob_path(sha, ext="pdf").write_bytes(raw)
    wc.upsert_page(
        cache,
        url=wc.normalize_url(url),
        raw_url=url,
        content_sha=sha,
        fetched_at=wc.now_iso(),
        content_type="application/pdf",
        text=text,
        text_source="pdf",
    )


# --------------------------------------------------------------------------- #
# Sheet counting (pure)
# --------------------------------------------------------------------------- #


def test_text_sheet_count_reads_marker_lines():
    assert web_pdfocr._text_sheet_count("a\n\f\nb\n\f") == 2


def test_text_sheet_count_is_none_without_markers():
    # An unpaginated text layer is a document with no page information — one
    # the assertion must not compare against, not a one-sheet document.
    assert web_pdfocr._text_sheet_count("just words") is None
    assert web_pdfocr._text_sheet_count(None) is None
    assert web_pdfocr._text_sheet_count("") is None


def test_text_sheet_count_ignores_interior_form_feeds():
    # Only a line that is exactly "\f" is a marker (the corpus-wide rule from
    # _parse_doc); a stray mid-line feed must not mint a phantom sheet.
    assert web_pdfocr._text_sheet_count("a\fb\n\f") == 1


# --------------------------------------------------------------------------- #
# ocr_one — the per-document write
# --------------------------------------------------------------------------- #


def test_ocr_one_writes_the_tier_and_search_finds_it(cache, monkeypatch):
    _seed_pdf(cache, url="https://x.com/m.pdf", raw=b"%PDF-fake", text="a\n\f\nb\n\f")
    monkeypatch.setattr(
        web_pdfocr, "ocr_pdf", lambda raw, url: ("ink one\n\f\nink two\n\f", 2)
    )
    rec = wc.get("https://x.com/m.pdf", con=cache)
    assert rec is not None
    assert web_pdfocr.ocr_one(cache, rec) is True
    row = wc.get("https://x.com/m.pdf", con=cache)
    assert row is not None
    assert row["ocr_text"] == "ink one\n\f\nink two\n\f"
    (hit,) = wc.search("ink", con=cache)
    assert hit["ocr_matches"] == 2


def test_ocr_one_refuses_a_sheet_count_mismatch(cache, monkeypatch, capsys):
    # The only thing standing between a rasterization/extraction mismatch and
    # `page 41` meaning two different sheets in the two columns.
    _seed_pdf(cache, url="https://x.com/m.pdf", raw=b"%PDF-fake", text="a\n\f\nb\n\f")
    monkeypatch.setattr(web_pdfocr, "ocr_pdf", lambda raw, url: ("ink\n\f", 1))
    rec = wc.get("https://x.com/m.pdf", con=cache)
    assert rec is not None
    assert web_pdfocr.ocr_one(cache, rec) is False
    row = wc.get("https://x.com/m.pdf", con=cache)
    assert row is not None
    assert row["ocr_text"] is None
    assert "sheet count mismatch" in capsys.readouterr().err


def test_ocr_one_accepts_any_count_when_the_text_layer_is_unpaginated(
    cache, monkeypatch
):
    # A dark document (or an unpaginated import) has nothing to compare
    # against; the raster's own count stands.
    _seed_pdf(cache, url="https://x.com/dark.pdf", raw=b"%PDF-fake", text=None)
    monkeypatch.setattr(web_pdfocr, "ocr_pdf", lambda raw, url: ("ink\n\f", 1))
    rec = wc.get("https://x.com/dark.pdf", con=cache)
    assert rec is not None
    assert web_pdfocr.ocr_one(cache, rec) is True


def test_ocr_one_skips_a_missing_blob(cache, monkeypatch, capsys):
    wc.upsert_page(
        cache,
        url=wc.normalize_url("https://x.com/gone.pdf"),
        raw_url="https://x.com/gone.pdf",
        content_sha="deadbeef",
        fetched_at=wc.now_iso(),
        content_type="application/pdf",
    )
    monkeypatch.setattr(
        web_pdfocr, "ocr_pdf", lambda raw, url: pytest.fail("must not be called")
    )
    rec = wc.get("https://x.com/gone.pdf", con=cache)
    assert rec is not None
    assert web_pdfocr.ocr_one(cache, rec) is False
    assert "blob missing" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# The real pipeline, where it can run
# --------------------------------------------------------------------------- #


@darwin_only
def test_ocr_pdf_reads_the_fixture_pdf(make_pdf):
    text, sheets = web_pdfocr.ocr_pdf(
        make_pdf(text="Hello PDF evidence"), "https://x.com/f.pdf"
    )
    assert sheets == 1
    # A marker after every sheet, the final one included, each on its own line
    # — the exact shape poppler's normalization gives pages.text, so the two
    # columns split identically.
    assert text.endswith("\n\f")
    assert text.count("\f") == 1
    assert "Hello PDF evidence" in text

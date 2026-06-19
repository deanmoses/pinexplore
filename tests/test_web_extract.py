"""Tests for web_extract: conservative date extraction (no network)."""

from __future__ import annotations

import web_extract


def test_extract_date_null_when_only_weak_year_signal():
    html = (
        '<html><head><meta name="date" content="2024"></head>'
        "<body><article><p>Defunct maker.</p>"
        "<footer>© 2024 Acme</footer></article></body></html>"
    )
    assert web_extract.extract(html, "http://x").last_updated is None


def test_extract_date_is_most_recent_real_date():
    html = (
        "<html><head>"
        '<meta property="article:published_time" content="2023-06-15">'
        '<meta property="article:modified_time" content="2024-08-01">'
        "</head><body><article><p>y</p></article></body></html>"
    )
    assert web_extract.extract(html, "http://x").last_updated == "2024-08-01"


# --------------------------------------------------------------------------- #
# extract_pdf — text/title/date from raw PDF bytes (pypdf)
# --------------------------------------------------------------------------- #


def test_extract_pdf_pulls_text_title_and_date(make_pdf):
    raw = make_pdf(
        title="Test Rulesheet",
        moddate="D:20240115093000-08'00'",
        creationdate="D:20200101000000Z",
    )
    meta = web_extract.extract_pdf(raw)
    assert "Hello PDF evidence" in (meta.text or "")
    assert meta.title == "Test Rulesheet"
    # ModDate preferred over the older CreationDate (most-recent-date semantics).
    assert meta.last_updated == "2024-01-15"


def test_extract_pdf_falls_back_to_creationdate(make_pdf):
    raw = make_pdf(creationdate="D:20200101000000Z")  # no ModDate
    assert web_extract.extract_pdf(raw).last_updated == "2020-01-01"


def test_extract_pdf_no_metadata_yields_none_title_and_date(make_pdf):
    meta = web_extract.extract_pdf(make_pdf())  # no Info-dict fields
    assert meta.title is None
    assert meta.last_updated is None
    assert "Hello PDF evidence" in (meta.text or "")


def test_extract_pdf_malformed_returns_empty_not_raises():
    # A broken/garbage PDF must not crash a batch: empty meta, no exception, so the
    # blob is still stored and the caller's thin-content warning fires.
    meta = web_extract.extract_pdf(b"%PDF-1.4 not really a pdf")
    assert meta == web_extract.ExtractedMeta(None, None, None)


def test_extract_pdf_scanned_image_only_has_no_text(make_pdf):
    # An image-only PDF (here: empty content) extracts to nothing — text is None.
    assert web_extract.extract_pdf(make_pdf(text="")).text is None

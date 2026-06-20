"""Tests for the content-type handler registry and its handlers (no network).

The registry routing (handler_for / sniff / the extractable + sniffable sets),
plus each handler's per-type behavior: the HTML handler's charset decoding and
conservative date extraction, and the PDF handler's pypdf text/title/date pull.
"""

from __future__ import annotations

import content_types as ct
from content_types.html import HtmlHandler, _sniff_meta_charset
from content_types.pdf import PdfHandler

# --------------------------------------------------------------------------- #
# Registry — routing by content type and by magic-byte signature
# --------------------------------------------------------------------------- #


def test_handler_for_routes_known_types():
    assert isinstance(ct.handler_for("text/html"), HtmlHandler)
    assert isinstance(ct.handler_for("application/xhtml+xml"), HtmlHandler)
    assert isinstance(ct.handler_for("application/pdf"), PdfHandler)


def test_handler_for_unknown_type_is_none():
    assert ct.handler_for("image/png") is None


def test_extractable_set_is_the_union_of_handler_mime_types():
    assert {
        "text/html",
        "application/xhtml+xml",
        "application/pdf",
    } == ct.EXTRACTABLE_CONTENT_TYPES


def test_sniff_matches_pdf_signature_whatever_the_header():
    # The %PDF- signature identifies a PDF served as octet-stream / mislabeled.
    assert isinstance(ct.sniff(b"%PDF-1.4\nstuff"), PdfHandler)


def test_sniff_returns_none_for_non_signature_bytes():
    # HTML carries no signature; a genuine binary download matches nothing.
    assert ct.sniff(b"<html>hi</html>") is None
    assert ct.sniff(b"PK\x03\x04zip") is None


def test_pdf_handler_is_not_renderable_html_is():
    assert ct.handler_for("text/html").renderable is True
    assert ct.handler_for("application/pdf").renderable is False


def test_handler_extensions():
    assert ct.handler_for("text/html").extension == "html"
    assert ct.handler_for("application/pdf").extension == "pdf"


def test_sniffed_canonical_mime_round_trips_to_a_handler():
    # http_get stamps content_type = sniffed.canonical_mime; web_fetch then re-looks
    # the handler up by it. Lock that round-trip: the stamped label must be one a
    # handler claims (the registry validates this, but guard it from the outside too).
    for handler in ct.HANDLERS:
        if handler.signature is not None:
            assert handler.canonical_mime in ct.EXTRACTABLE_CONTENT_TYPES
            assert ct.handler_for(handler.canonical_mime) is handler


# --------------------------------------------------------------------------- #
# thin_warning — the per-type no-text message (user-facing strings + branching)
# --------------------------------------------------------------------------- #


def test_html_thin_warning_suggests_render_when_none_attempted():
    msg = HtmlHandler().thin_warning("http://x", rendered=False, render_attempted=False)
    assert msg is not None
    assert "likely JS-only" in msg
    assert "--render" in msg


def test_html_thin_warning_distinct_after_a_render():
    msg = HtmlHandler().thin_warning("http://x", rendered=True, render_attempted=True)
    assert msg == "WARNING: still thin after render: http://x"


def test_html_thin_warning_quiet_when_render_attempted_and_failed():
    # A render was tried and returned None (render already logged why) — stay quiet.
    assert (
        HtmlHandler().thin_warning("http://x", rendered=False, render_attempted=True)
        is None
    )


def test_pdf_thin_warning_is_scanned_message_regardless_of_render_flags():
    # A PDF never renders, so the flags don't change its message.
    pdf = PdfHandler()
    for rendered, attempted in [(False, False), (False, True)]:
        msg = pdf.thin_warning(
            "http://x", rendered=rendered, render_attempted=attempted
        )
        assert msg == "WARNING: PDF extracted to little/no text (scanned?): http://x"


# --------------------------------------------------------------------------- #
# HTML handler — charset fallback (a bogus label must not crash the batch)
# --------------------------------------------------------------------------- #


def _decode(raw: bytes, charset: str | None) -> str | None:
    return HtmlHandler().decode(raw, charset)


def test_decode_honors_valid_charset():
    assert _decode("café".encode("latin-1"), "latin-1") == "café"


def test_decode_falls_back_to_utf8_on_unknown_charset():
    # A page advertising a junk charset label would make bytes.decode raise
    # LookupError — which escapes the fetch_one except tuple and kills the batch.
    # Fall back to utf-8 instead of losing the page.
    assert _decode(b"hi", "utf-8x-bogus") == "hi"


def test_decode_never_raises_on_bad_bytes():
    # utf-8 fallback still uses errors="replace", so undecodable bytes don't raise.
    assert isinstance(_decode(b"\xff\xfe bad", "totally-not-a-charset"), str)


# --------------------------------------------------------------------------- #
# HTML handler — charset sniffing for headerless Shift-JIS pages
# --------------------------------------------------------------------------- #
#
# Real-world failure: old Japanese pages (ampress.co.jp, showayuen) are served as
# Shift-JIS/cp932 with no Content-Type charset header, so a blind utf-8 decode
# yields mojibake. Resolve the charset from the page's own <meta> / detection.

JP = "会社概要"  # "Company Overview" — the showayuen kaisha_gaiyou page's title


def test_decode_sniffs_meta_http_equiv_charset_when_header_absent():
    # Legacy <meta http-equiv="Content-Type" ... charset=Shift_JIS>, no HTTP header.
    html = (
        '<html><head><meta http-equiv="Content-Type" '
        f'content="text/html; charset=Shift_JIS"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode("cp932")
    assert JP in _decode(html, None)


def test_decode_sniffs_html5_meta_charset_when_header_absent():
    html = (
        f'<html><head><meta charset="shift_jis"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode("cp932")
    assert JP in _decode(html, None)


def test_decode_detects_charset_when_no_header_and_no_meta():
    # No header, no meta declaration: fall back to charset-normalizer detection.
    body = (
        f"<html><body><h1>{JP}</h1>" + "日本語の本文。" * 40 + "</body></html>"
    ).encode("cp932")
    assert JP in _decode(body, None)


def test_decode_header_charset_wins_over_meta():
    # The HTTP header is authoritative; a contradicting meta must not override it.
    html = (
        f'<html><head><meta charset="shift_jis"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode()
    assert JP in _decode(html, "utf-8")


def test_decode_shift_jis_meta_decoded_as_cp932_superset():
    # Pages declaring Shift_JIS routinely use cp932 extension chars (①, etc.) that
    # the strict shift_jis codec can't decode. Treat the whole family as cp932.
    html = (
        f'<html><head><meta charset="Shift_JIS"></head><body>①{JP}</body></html>'
    ).encode("cp932")
    out = _decode(html, None)
    assert "①" in out
    assert JP in out


def test_sniff_meta_charset_html5_form():
    assert _sniff_meta_charset(b'<meta charset="utf-8">') == "utf-8"


def test_sniff_meta_charset_http_equiv_form():
    raw = b'<meta http-equiv="Content-Type" content="text/html; charset=Shift_JIS">'
    assert _sniff_meta_charset(raw) == "Shift_JIS"


def test_sniff_meta_charset_none_when_absent():
    assert _sniff_meta_charset(b"<html><body>no meta</body></html>") is None


def test_sniff_meta_charset_only_scans_head():
    # The HTML spec puts the declaration in the first 1024 bytes; ignore late strays.
    raw = b"x" * 1100 + b'<meta charset="shift_jis">'
    assert _sniff_meta_charset(raw) is None


# --------------------------------------------------------------------------- #
# HTML handler — conservative date extraction (no padded guess)
# --------------------------------------------------------------------------- #


def _extract_html(html: str, url: str = "http://x") -> ct.ExtractedMeta:
    return HtmlHandler().extract(b"", html, url)


def test_extract_date_null_when_only_weak_year_signal():
    html = (
        '<html><head><meta name="date" content="2024"></head>'
        "<body><article><p>Defunct maker.</p>"
        "<footer>© 2024 Acme</footer></article></body></html>"
    )
    assert _extract_html(html).last_updated is None


def test_extract_date_is_most_recent_real_date():
    html = (
        "<html><head>"
        '<meta property="article:published_time" content="2023-06-15">'
        '<meta property="article:modified_time" content="2024-08-01">'
        "</head><body><article><p>y</p></article></body></html>"
    )
    assert _extract_html(html).last_updated == "2024-08-01"


# --------------------------------------------------------------------------- #
# PDF handler — text/title/date from raw PDF bytes (pypdf)
# --------------------------------------------------------------------------- #


def _extract_pdf(raw: bytes) -> ct.ExtractedMeta:
    return PdfHandler().extract(raw, None, "http://x")


def test_extract_pdf_pulls_text_title_and_date(make_pdf):
    raw = make_pdf(
        title="Test Rulesheet",
        moddate="D:20240115093000-08'00'",
        creationdate="D:20200101000000Z",
    )
    meta = _extract_pdf(raw)
    assert "Hello PDF evidence" in (meta.text or "")
    assert meta.title == "Test Rulesheet"
    # ModDate preferred over the older CreationDate (most-recent-date semantics).
    assert meta.last_updated == "2024-01-15"


def test_extract_pdf_falls_back_to_creationdate(make_pdf):
    raw = make_pdf(creationdate="D:20200101000000Z")  # no ModDate
    assert _extract_pdf(raw).last_updated == "2020-01-01"


def test_extract_pdf_no_metadata_yields_none_title_and_date(make_pdf):
    meta = _extract_pdf(make_pdf())  # no Info-dict fields
    assert meta.title is None
    assert meta.last_updated is None
    assert "Hello PDF evidence" in (meta.text or "")


def test_extract_pdf_malformed_returns_empty_not_raises():
    # A broken/garbage PDF must not crash a batch: empty meta, no exception, so the
    # blob is still stored and the caller's thin-content warning fires.
    assert _extract_pdf(b"%PDF-1.4 not really a pdf") == ct.ExtractedMeta(
        None, None, None
    )


def test_extract_pdf_scanned_image_only_has_no_text(make_pdf):
    # An image-only PDF (here: empty content) extracts to nothing — text is None.
    assert _extract_pdf(make_pdf(text="")).text is None

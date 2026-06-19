"""Tests for web_http: charset decoding and wire-safe URL encoding (no network)."""

from __future__ import annotations

import web_http

# --------------------------------------------------------------------------- #
# _decode_body: charset fallback (a bogus label must not crash the batch)
# --------------------------------------------------------------------------- #


def test_decode_body_honors_valid_charset():
    assert web_http._decode_body("café".encode("latin-1"), "latin-1") == "café"


def test_decode_body_falls_back_to_utf8_on_unknown_charset():
    # A page advertising a junk charset label would make bytes.decode raise
    # LookupError — which escapes the fetch_one except tuple and kills the batch.
    # Fall back to utf-8 instead of losing the page.
    assert web_http._decode_body(b"hi", "utf-8x-bogus") == "hi"


def test_decode_body_never_raises_on_bad_bytes():
    # utf-8 fallback still uses errors="replace", so undecodable bytes don't raise.
    out = web_http._decode_body(b"\xff\xfe bad", "totally-not-a-charset")
    assert isinstance(out, str)


# --------------------------------------------------------------------------- #
# _decode_body: charset sniffing for headerless Shift-JIS pages
# --------------------------------------------------------------------------- #
#
# Real-world failure: old Japanese pages (ampress.co.jp, showayuen) are served as
# Shift-JIS/cp932 with no Content-Type charset header, so a blind utf-8 decode
# yields mojibake. Resolve the charset from the page's own <meta> / detection.

JP = "会社概要"  # "Company Overview" — the showayuen kaisha_gaiyou page's title


def test_decode_body_sniffs_meta_http_equiv_charset_when_header_absent():
    # Legacy <meta http-equiv="Content-Type" ... charset=Shift_JIS>, no HTTP header.
    html = (
        '<html><head><meta http-equiv="Content-Type" '
        f'content="text/html; charset=Shift_JIS"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode("cp932")
    assert JP in web_http._decode_body(html, None)


def test_decode_body_sniffs_html5_meta_charset_when_header_absent():
    html = (
        f'<html><head><meta charset="shift_jis"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode("cp932")
    assert JP in web_http._decode_body(html, None)


def test_decode_body_detects_charset_when_no_header_and_no_meta():
    # No header, no meta declaration: fall back to charset-normalizer detection.
    body = (
        f"<html><body><h1>{JP}</h1>" + "日本語の本文。" * 40 + "</body></html>"
    ).encode("cp932")
    assert JP in web_http._decode_body(body, None)


def test_decode_body_header_charset_wins_over_meta():
    # The HTTP header is authoritative; a contradicting meta must not override it.
    html = (
        f'<html><head><meta charset="shift_jis"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode()
    assert JP in web_http._decode_body(html, "utf-8")


def test_decode_body_shift_jis_meta_decoded_as_cp932_superset():
    # Pages declaring Shift_JIS routinely use cp932 extension chars (①, etc.) that
    # the strict shift_jis codec can't decode. Treat the whole family as cp932.
    html = (
        f'<html><head><meta charset="Shift_JIS"></head><body>①{JP}</body></html>'
    ).encode("cp932")
    out = web_http._decode_body(html, None)
    assert "①" in out
    assert JP in out


# --------------------------------------------------------------------------- #
# _sniff_meta_charset — read the charset an HTML page declares about itself
# --------------------------------------------------------------------------- #


def test_sniff_meta_charset_html5_form():
    assert web_http._sniff_meta_charset(b'<meta charset="utf-8">') == "utf-8"


def test_sniff_meta_charset_http_equiv_form():
    raw = b'<meta http-equiv="Content-Type" content="text/html; charset=Shift_JIS">'
    assert web_http._sniff_meta_charset(raw) == "Shift_JIS"


def test_sniff_meta_charset_none_when_absent():
    assert web_http._sniff_meta_charset(b"<html><body>no meta</body></html>") is None


def test_sniff_meta_charset_only_scans_head():
    # The HTML spec puts the declaration in the first 1024 bytes; ignore late strays.
    raw = b"x" * 1100 + b'<meta charset="shift_jis">'
    assert web_http._sniff_meta_charset(raw) is None


# --------------------------------------------------------------------------- #
# request_url — wire-safe encoding of a readable normalized URL
# --------------------------------------------------------------------------- #


def test_request_url_percent_encodes_non_ascii_path():
    # The bug this fixes: a non-ASCII path raised UnicodeEncodeError in urllib.
    got = web_http.request_url("https://www.weblio.jp/content/サンワイズ")
    assert got == (
        "https://www.weblio.jp/content/%E3%82%B5%E3%83%B3%E3%83%AF%E3%82%A4%E3%82%BA"
    )
    assert got.isascii()


def test_request_url_idempotent_on_ascii_and_encoded():
    plain = "https://example.com/foo/bar?a=1&b=2"
    assert web_http.request_url(plain) == plain
    # already-percent-encoded path is not double-encoded (%E3 stays %E3)
    enc = "https://www.weblio.jp/content/%E3%82%B5%E3%83%B3"
    assert web_http.request_url(enc) == enc


def test_request_url_preserves_ipv6_brackets():
    # parts.hostname drops the brackets an IPv6 literal needs; without them the
    # rebuilt netloc (::1:8080) is ambiguous/malformed. Host stays ASCII, so this
    # also guards the non-IDNA path.
    assert web_http.request_url("http://[::1]:8080/x") == "http://[::1]:8080/x"
    assert web_http.request_url("http://[2001:db8::1]/p") == "http://[2001:db8::1]/p"


def test_request_url_idna_encodes_non_ascii_host():
    got = web_http.request_url("https://日本.example/x")
    assert got.startswith("https://xn--")
    assert got.endswith("/x")
    assert got.isascii()

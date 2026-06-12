"""Tests for web_fetch.fetch_one behaviors — all offline (``_http_get`` stubbed).

Covers the guards, content gates, redirect handling, change detection, and
failure logging, plus _extract's conservative date extraction.
"""

from __future__ import annotations

import http.client
from typing import TYPE_CHECKING

import pytest
import web_cache as wc
import web_fetch

if TYPE_CHECKING:
    import sqlite3

# A fetches-log row projected to the columns the assertions care about:
# (url, http_status, content_sha, changed).
FetchRow = tuple[str, int | None, str | None, int | None]


@pytest.fixture(autouse=True)
def _no_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    # Never sleep in tests (the real per-domain limiter would on repeat fetches).
    monkeypatch.setattr(web_fetch, "_rate_limit", lambda domain: None)


def _stub_get(
    monkeypatch: pytest.MonkeyPatch,
    *,
    body: bytes = b"<html><body><p>hi</p></body></html>",
    final_url: str | None = None,
    status: int = 200,
    content_type: str = "text/html",
    skip: web_fetch.SkipReason | None = None,
) -> None:
    """Install a fake _http_get returning a crafted _Resp (final_url defaults to
    the requested url; pass it to simulate a redirect)."""

    def _get(url: str) -> web_fetch._Resp:
        fu = url if final_url is None else final_url
        if skip:
            return web_fetch._Resp(status, content_type, fu, None, None, skip)
        return web_fetch._Resp(status, content_type, fu, body, body.decode(), None)

    monkeypatch.setattr(web_fetch, "_http_get", _get)


def _run(
    con: sqlite3.Connection,
    raw_url: str,
    *,
    query: str | None = "q",
    force: bool = False,
    max_age_days: int = 30,
) -> None:
    web_fetch.fetch_one(
        con,
        raw_url,
        query=query,
        force=force,
        max_age_days=max_age_days,
    )


def _fetches(con: sqlite3.Connection) -> list[FetchRow]:
    return [
        (r[0], r[1], r[2], r[3])
        for r in con.execute(
            "SELECT url, http_status, content_sha, changed FROM fetches ORDER BY id"
        ).fetchall()
    ]


def _page(con: sqlite3.Connection, url: str) -> wc.PageRow:
    """Fetch a page row that the test expects to exist, narrowing away None.

    Use for positive lookups (when a row should be stored); a missing row fails
    here with a clear message instead of a downstream KeyError/TypeError. The
    absence cases assert ``wc.get(...) is None`` directly."""
    row = wc.get(url, con=con)
    assert row is not None, f"expected a stored page for {url}"
    return row


# --------------------------------------------------------------------------- #
# guards
# --------------------------------------------------------------------------- #


def test_unsupported_scheme_rejected_without_fetching(cache, monkeypatch):
    called = []
    monkeypatch.setattr(web_fetch, "_http_get", lambda u: called.append(u))
    _run(cache, "file:///etc/passwd")
    assert called == []
    assert _fetches(cache) == []  # not even logged — it's rejected input, not a fetch


def test_hostless_url_rejected(cache, monkeypatch):
    called = []
    monkeypatch.setattr(web_fetch, "_http_get", lambda u: called.append(u))
    _run(cache, "https://")
    assert called == []


def test_malformed_url_skipped_not_raised(cache, monkeypatch):
    # normalize_url raises ValueError on a bad port / invalid IPv6; a garbage
    # --from-file row must skip (before any fetch), not abort the whole batch.
    called = []
    monkeypatch.setattr(web_fetch, "_http_get", lambda u: called.append(u))
    _run(cache, "http://example.com:abc/foo")  # bad port — must not raise
    _run(cache, "http://[::1bad/foo")  # invalid IPv6 — must not raise
    assert called == []
    assert _fetches(cache) == []  # rejected input, never a fetch attempt


# --------------------------------------------------------------------------- #
# content-type gate + size cap
# --------------------------------------------------------------------------- #


def test_non_html_content_type_skipped_but_logged(cache, monkeypatch):
    _stub_get(monkeypatch, skip="content-type", content_type="application/pdf")
    _run(cache, "https://x.com/doc.pdf")
    assert wc.get("https://x.com/doc.pdf", con=cache) is None  # no page row
    assert _fetches(cache) == [("https://x.com/doc.pdf", 200, None, None)]  # logged


def test_oversize_response_skipped_but_logged(cache, monkeypatch):
    _stub_get(monkeypatch, skip="too-large")
    _run(cache, "https://x.com/big")
    assert wc.get("https://x.com/big", con=cache) is None
    assert _fetches(cache)[0][1] == 200


# --------------------------------------------------------------------------- #
# redirect reconciliation + fresh-skip
# --------------------------------------------------------------------------- #


def test_redirect_keys_on_final_url_keeps_raw(cache, monkeypatch):
    _stub_get(monkeypatch, final_url="https://site.com/canonical")
    _run(cache, "http://site.com/req")
    assert (
        wc.get("http://site.com/req", con=cache) is None
    )  # not stored under requested
    row = wc.get("https://site.com/canonical", con=cache)
    assert row is not None
    assert row["url"] == "https://site.com/canonical"
    assert row["raw_url"] == "http://site.com/req"


def test_redirecting_url_is_fresh_skipped_on_second_run(cache, monkeypatch):
    calls = []

    def _get(url):
        calls.append(url)
        return web_fetch._Resp(
            200,
            "text/html",
            "https://site.com/canonical",
            b"<html>x</html>",
            "<html>x</html>",
            None,
        )

    monkeypatch.setattr(web_fetch, "_http_get", _get)
    _run(cache, "http://site.com/req")
    _run(cache, "http://site.com/req")  # row lives under canonical; raw_url matches
    assert len(calls) == 1  # second run skipped, not re-followed


# --------------------------------------------------------------------------- #
# change detection / versioning
# --------------------------------------------------------------------------- #


def test_change_detection_versions_blobs_and_logs(cache, monkeypatch):
    url = "https://s.com/p"
    _stub_get(monkeypatch, body=b"<html>v1</html>")
    _run(cache, url, force=True)
    sha1 = _page(cache, url)["content_sha"]

    _run(cache, url, force=True)  # unchanged refetch

    _stub_get(monkeypatch, body=b"<html>v2 DIFFERENT</html>")
    _run(cache, url, force=True)  # changed refetch
    sha2 = _page(cache, url)["content_sha"]

    assert sha1 != sha2
    assert wc.html_path(sha1).exists()  # both versions kept
    assert wc.html_path(sha2).exists()
    assert _page(cache, url)["content_sha"] == sha2  # points at latest
    assert [r[3] for r in _fetches(cache)] == [1, 0, 1]  # new, unchanged, changed


def test_unchanged_refetch_does_not_rewrite_blob(cache, monkeypatch):
    url = "https://s.com/p"
    _stub_get(monkeypatch, body=b"<html>same</html>")
    _run(cache, url, force=True)
    sha = _page(cache, url)["content_sha"]
    blob = wc.html_path(sha)
    mtime = blob.stat().st_mtime_ns
    _run(cache, url, force=True)
    assert blob.stat().st_mtime_ns == mtime  # not rewritten


# --------------------------------------------------------------------------- #
# failure handling (must not crash the batch)
# --------------------------------------------------------------------------- #


def test_invalid_url_error_is_logged_not_raised(cache, monkeypatch):
    def boom(url):
        raise http.client.InvalidURL("URL can't contain control characters")

    monkeypatch.setattr(web_fetch, "_http_get", boom)
    _run(cache, "https://x.com/p")  # must not raise
    assert wc.get("https://x.com/p", con=cache) is None
    assert _fetches(cache) == [("https://x.com/p", None, None, None)]  # null status


# --------------------------------------------------------------------------- #
# _decode_body: charset fallback (a bogus label must not crash the batch)
# --------------------------------------------------------------------------- #


def test_decode_body_honors_valid_charset():
    assert web_fetch._decode_body("café".encode("latin-1"), "latin-1") == "café"


def test_decode_body_falls_back_to_utf8_on_unknown_charset():
    # A page advertising a junk charset label would make bytes.decode raise
    # LookupError — which escapes the fetch_one except tuple and kills the batch.
    # Fall back to utf-8 instead of losing the page.
    assert web_fetch._decode_body(b"hi", "utf-8x-bogus") == "hi"


def test_decode_body_never_raises_on_bad_bytes():
    # utf-8 fallback still uses errors="replace", so undecodable bytes don't raise.
    out = web_fetch._decode_body(b"\xff\xfe bad", "totally-not-a-charset")
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
    assert JP in web_fetch._decode_body(html, None)


def test_decode_body_sniffs_html5_meta_charset_when_header_absent():
    html = (
        f'<html><head><meta charset="shift_jis"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode("cp932")
    assert JP in web_fetch._decode_body(html, None)


def test_decode_body_detects_charset_when_no_header_and_no_meta():
    # No header, no meta declaration: fall back to charset-normalizer detection.
    body = (
        f"<html><body><h1>{JP}</h1>" + "日本語の本文。" * 40 + "</body></html>"
    ).encode("cp932")
    assert JP in web_fetch._decode_body(body, None)


def test_decode_body_header_charset_wins_over_meta():
    # The HTTP header is authoritative; a contradicting meta must not override it.
    html = (
        f'<html><head><meta charset="shift_jis"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode()
    assert JP in web_fetch._decode_body(html, "utf-8")


def test_decode_body_shift_jis_meta_decoded_as_cp932_superset():
    # Pages declaring Shift_JIS routinely use cp932 extension chars (①, etc.) that
    # the strict shift_jis codec can't decode. Treat the whole family as cp932.
    html = (
        f'<html><head><meta charset="Shift_JIS"></head><body>①{JP}</body></html>'
    ).encode("cp932")
    out = web_fetch._decode_body(html, None)
    assert "①" in out
    assert JP in out


# --------------------------------------------------------------------------- #
# _sniff_meta_charset — read the charset an HTML page declares about itself
# --------------------------------------------------------------------------- #


def test_sniff_meta_charset_html5_form():
    assert web_fetch._sniff_meta_charset(b'<meta charset="utf-8">') == "utf-8"


def test_sniff_meta_charset_http_equiv_form():
    raw = b'<meta http-equiv="Content-Type" content="text/html; charset=Shift_JIS">'
    assert web_fetch._sniff_meta_charset(raw) == "Shift_JIS"


def test_sniff_meta_charset_none_when_absent():
    assert web_fetch._sniff_meta_charset(b"<html><body>no meta</body></html>") is None


def test_sniff_meta_charset_only_scans_head():
    # The HTML spec puts the declaration in the first 1024 bytes; ignore late strays.
    raw = b"x" * 1100 + b'<meta charset="shift_jis">'
    assert web_fetch._sniff_meta_charset(raw) is None


# --------------------------------------------------------------------------- #
# _extract: conservative date extraction (no network)
# --------------------------------------------------------------------------- #


def test_extract_date_null_when_only_weak_year_signal():
    html = (
        '<html><head><meta name="date" content="2024"></head>'
        "<body><article><p>Defunct maker.</p>"
        "<footer>© 2024 Acme</footer></article></body></html>"
    )
    assert web_fetch._extract(html, "http://x").last_updated is None


def test_extract_date_is_most_recent_real_date():
    html = (
        "<html><head>"
        '<meta property="article:published_time" content="2023-06-15">'
        '<meta property="article:modified_time" content="2024-08-01">'
        "</head><body><article><p>y</p></article></body></html>"
    )
    assert web_fetch._extract(html, "http://x").last_updated == "2024-08-01"


# --------------------------------------------------------------------------- #
# _request_url — wire-safe encoding of a readable normalized URL
# --------------------------------------------------------------------------- #


def test_request_url_percent_encodes_non_ascii_path():
    # The bug this fixes: a non-ASCII path raised UnicodeEncodeError in urllib.
    got = web_fetch._request_url("https://www.weblio.jp/content/サンワイズ")
    assert got == (
        "https://www.weblio.jp/content/"
        "%E3%82%B5%E3%83%B3%E3%83%AF%E3%82%A4%E3%82%BA"
    )
    assert got.isascii()


def test_request_url_idempotent_on_ascii_and_encoded():
    plain = "https://example.com/foo/bar?a=1&b=2"
    assert web_fetch._request_url(plain) == plain
    # already-percent-encoded path is not double-encoded (%E3 stays %E3)
    enc = "https://www.weblio.jp/content/%E3%82%B5%E3%83%B3"
    assert web_fetch._request_url(enc) == enc


def test_request_url_preserves_ipv6_brackets():
    # parts.hostname drops the brackets an IPv6 literal needs; without them the
    # rebuilt netloc (::1:8080) is ambiguous/malformed. Host stays ASCII, so this
    # also guards the non-IDNA path.
    assert web_fetch._request_url("http://[::1]:8080/x") == "http://[::1]:8080/x"
    assert web_fetch._request_url("http://[2001:db8::1]/p") == "http://[2001:db8::1]/p"


def test_request_url_idna_encodes_non_ascii_host():
    got = web_fetch._request_url("https://日本.example/x")
    assert got.startswith("https://xn--")
    assert got.endswith("/x")
    assert got.isascii()

"""Tests for web_fetch.fetch_one orchestration — all offline (no network).

Covers the guards, content gates, redirect handling, change detection, failure
logging, and the headless-render escalation/fallback (``http_get`` and
``render`` stubbed). The pieces fetch_one composes are tested in their own
modules: transport/charset in test_web_http, extraction in test_web_extract,
the render primitives in test_web_render.
"""

from __future__ import annotations

import http.client
from typing import TYPE_CHECKING

import pytest
import web_cache as wc
import web_fetch
import web_http
import web_render

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
    skip: web_http.SkipReason | None = None,
    decode_body: bool = True,
) -> None:
    """Install a fake http_get returning a crafted Resp (final_url defaults to
    the requested url; pass it to simulate a redirect).

    ``decode_body=False`` mimics a binary fetch (a PDF): text is None and the raw
    bytes carry through to the extractor, the way http_get returns a PDF."""

    def _get(url: str) -> web_http.Resp:
        fu = url if final_url is None else final_url
        if skip:
            return web_http.Resp(status, content_type, fu, None, None, skip)
        text = body.decode() if decode_body else None
        return web_http.Resp(status, content_type, fu, body, text, None)

    monkeypatch.setattr(web_fetch, "http_get", _get)


def _run(
    con: sqlite3.Connection,
    raw_url: str,
    *,
    query: str | None = "q",
    force: bool = False,
    max_age_days: int = 30,
    browser: object | None = None,
    force_render: bool = False,
    thin_chars: int = web_render.THIN_TEXT_CHARS,
) -> None:
    web_fetch.fetch_one(
        con,
        raw_url,
        query=query,
        force=force,
        max_age_days=max_age_days,
        browser=browser,  # type: ignore[arg-type]  # tests pass a sentinel; render is stubbed
        force_render=force_render,
        thin_chars=thin_chars,
    )


# Rich HTML that extracts to well over THIN_TEXT_CHARS (so it is not thin).
RICH_HTML = (
    "<html><body><article><p>"
    + "Rich readable article text. " * 20
    + "</p></article></body></html>"
).encode()
# A client-rendered skeleton: extracts to near-nothing, so it reads as thin.
THIN_HTML = b"<html><body><div id='root'></div><p>hi</p></body></html>"


def _stub_render(
    monkeypatch: pytest.MonkeyPatch,
    *,
    body: bytes,
    final_url: str | None = None,
) -> None:
    """Stub web_fetch.render to return a crafted rendered Resp (no real browser).

    Bypasses LazyBrowser/Playwright entirely, so tests pass a sentinel ``browser``.
    """

    def _r(url: str, _browser: object) -> web_http.Resp:
        fu = url if final_url is None else final_url
        return web_http.Resp(200, "text/html", fu, body, body.decode(), None)

    monkeypatch.setattr(web_fetch, "render", _r)


def _fetches(con: sqlite3.Connection) -> list[FetchRow]:
    return [
        (r[0], r[1], r[2], r[3])
        for r in con.execute(
            "SELECT url, http_status, content_sha, changed FROM fetches ORDER BY id"
        ).fetchall()
    ]


def _rendered(con: sqlite3.Connection) -> list[int | None]:
    return [r[0] for r in con.execute("SELECT rendered FROM fetches ORDER BY id")]


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
    monkeypatch.setattr(web_fetch, "http_get", lambda u: called.append(u))
    _run(cache, "file:///etc/passwd")
    assert called == []
    assert _fetches(cache) == []  # not even logged — it's rejected input, not a fetch


def test_hostless_url_rejected(cache, monkeypatch):
    called = []
    monkeypatch.setattr(web_fetch, "http_get", lambda u: called.append(u))
    _run(cache, "https://")
    assert called == []


def test_malformed_url_skipped_not_raised(cache, monkeypatch):
    # normalize_url raises ValueError on a bad port / invalid IPv6; a garbage
    # --from-file row must skip (before any fetch), not abort the whole batch.
    called = []
    monkeypatch.setattr(web_fetch, "http_get", lambda u: called.append(u))
    _run(cache, "http://example.com:abc/foo")  # bad port — must not raise
    _run(cache, "http://[::1bad/foo")  # invalid IPv6 — must not raise
    assert called == []
    assert _fetches(cache) == []  # rejected input, never a fetch attempt


# --------------------------------------------------------------------------- #
# content-type gate + size cap
# --------------------------------------------------------------------------- #


def test_non_extractable_content_type_skipped_but_logged(cache, monkeypatch):
    # An image is neither extractable nor PDF-sniffable: skipped, but still logged.
    _stub_get(monkeypatch, skip="content-type", content_type="image/png")
    _run(cache, "https://x.com/pic.png")
    assert wc.get("https://x.com/pic.png", con=cache) is None  # no page row
    assert _fetches(cache) == [("https://x.com/pic.png", 200, None, None)]  # logged


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
        return web_http.Resp(
            200,
            "text/html",
            "https://site.com/canonical",
            b"<html>x</html>",
            "<html>x</html>",
            None,
        )

    monkeypatch.setattr(web_fetch, "http_get", _get)
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

    monkeypatch.setattr(web_fetch, "http_get", boom)
    _run(cache, "https://x.com/p")  # must not raise
    assert wc.get("https://x.com/p", con=cache) is None
    assert _fetches(cache) == [("https://x.com/p", None, None, None)]  # null status


# --------------------------------------------------------------------------- #
# headless render fallback (web_fetch.render stubbed — no real browser)
# --------------------------------------------------------------------------- #


def test_thin_plain_fetch_escalates_to_render(cache, monkeypatch):
    url = "https://spa.com/x"
    _stub_get(monkeypatch, body=THIN_HTML)  # plain GET extracts thin
    _stub_render(monkeypatch, body=RICH_HTML)  # render returns real content
    _run(cache, url, browser=object())
    row = _page(cache, url)
    assert row["rendered"] == 1
    assert "Rich readable article text" in (row["text"] or "")
    assert _rendered(cache) == [1]  # the fetch row is flagged a render too


def test_render_disabled_keeps_thin_plain_result(cache, monkeypatch):
    url = "https://spa.com/x"
    _stub_get(monkeypatch, body=THIN_HTML)
    called: list[str] = []
    monkeypatch.setattr(web_fetch, "render", lambda u, _b: called.append(u))
    _run(cache, url, browser=None)  # --no-render: browser is None
    assert called == []  # never attempted
    row = _page(cache, url)
    assert not row["rendered"]  # None/0, not a render
    assert _rendered(cache) == [0]


def test_rich_plain_fetch_does_not_render(cache, monkeypatch):
    url = "https://ok.com/x"
    _stub_get(monkeypatch, body=RICH_HTML)  # not thin
    called: list[str] = []

    def _no_call(u: str, _b: object) -> None:
        called.append(u)

    monkeypatch.setattr(web_fetch, "render", _no_call)
    _run(cache, url, browser=object())  # render enabled, but page isn't thin
    assert called == []
    assert not _page(cache, url)["rendered"]


def test_force_render_renders_even_when_not_thin(cache, monkeypatch):
    url = "https://ok.com/x"
    _stub_get(monkeypatch, body=RICH_HTML)  # not thin
    _stub_render(monkeypatch, body=RICH_HTML)
    _run(cache, url, browser=object(), force_render=True)
    assert _page(cache, url)["rendered"] == 1


def test_render_failure_falls_back_to_plain_result(cache, monkeypatch):
    url = "https://spa.com/x"
    _stub_get(monkeypatch, body=THIN_HTML)
    monkeypatch.setattr(web_fetch, "render", lambda u, _b: None)  # render failed
    _run(cache, url, browser=object())
    row = _page(cache, url)
    assert not row["rendered"]  # kept the plain (thin) result
    assert (row["text"] or "").strip() == "hi"
    # The failed render is still audited (None status, rendered=1), then the plain
    # fetch (200, rendered=0) — fetches logs every fetch.
    assert _rendered(cache) == [1, 0]
    assert [r[1] for r in _fetches(cache)] == [None, 200]


def test_render_redirect_rekeys_to_final_url(cache, monkeypatch):
    _stub_get(monkeypatch, body=THIN_HTML)
    _stub_render(monkeypatch, body=RICH_HTML, final_url="https://spa.com/real")
    _run(cache, "https://spa.com/x", browser=object())
    assert wc.get("https://spa.com/x", con=cache) is None  # not under requested
    row = wc.get("https://spa.com/real", con=cache)
    assert row is not None
    assert row["rendered"] == 1
    assert row["raw_url"] == "https://spa.com/x"


# --------------------------------------------------------------------------- #
# thin-content warnings (loud failure)
# --------------------------------------------------------------------------- #


def test_thin_warning_suggests_render_when_not_attempted(cache, monkeypatch, capsys):
    _stub_get(monkeypatch, body=THIN_HTML)
    _run(cache, "https://spa.com/x", browser=None)  # --no-render: never attempted
    assert "--render" in capsys.readouterr().err  # actionable: try rendering


def test_thin_warning_not_misleading_when_render_attempted_and_failed(
    cache, monkeypatch, capsys
):
    # A failed render must NOT suggest "retry with --render" — it was already on.
    _stub_get(monkeypatch, body=THIN_HTML)
    monkeypatch.setattr(web_fetch, "render", lambda u, _b: None)  # attempted, failed
    _run(cache, "https://spa.com/x", browser=object())
    assert "--render" not in capsys.readouterr().err


def test_rendered_then_still_thin_warns_distinctly(cache, monkeypatch, capsys):
    _stub_get(monkeypatch, body=THIN_HTML)
    _stub_render(monkeypatch, body=THIN_HTML)  # render didn't help
    _run(cache, "https://spa.com/x", browser=object())
    assert "still thin after render" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# PDF documents (extracted via web_extract.extract_pdf, stored as .pdf blobs)
# --------------------------------------------------------------------------- #


def test_pdf_stored_as_pdf_blob_with_extracted_text(cache, monkeypatch, make_pdf):
    raw = make_pdf(title="Spec Sheet", moddate="D:20240115093000Z")
    _stub_get(monkeypatch, body=raw, content_type="application/pdf", decode_body=False)
    _run(cache, "https://x.com/doc.pdf")
    row = _page(cache, "https://x.com/doc.pdf")
    assert row["content_type"] == "application/pdf"
    assert row["html_file"].endswith(".pdf")  # blob keeps its .pdf extension
    assert row["title"] == "Spec Sheet"
    assert row["last_updated"] == "2024-01-15"
    assert "Hello PDF evidence" in (row["text"] or "")
    assert not row["rendered"]  # a PDF is never a render
    assert wc.html_path(row["content_sha"], ext="pdf").exists()


def test_pdf_dedups_deterministically_on_refetch(cache, monkeypatch, make_pdf):
    # PDF bytes are stored verbatim, so an unchanged refetch is byte-identical
    # (changed=0) — unlike a render, whose DOM is rarely byte-stable.
    raw = make_pdf(title="Spec")
    _stub_get(monkeypatch, body=raw, content_type="application/pdf", decode_body=False)
    _run(cache, "https://x.com/doc.pdf", force=True)
    _run(cache, "https://x.com/doc.pdf", force=True)
    assert [r[3] for r in _fetches(cache)] == [1, 0]  # new, then unchanged


def test_scanned_pdf_never_renders_and_warns(cache, monkeypatch, make_pdf, capsys):
    # A scanned/image-only PDF extracts to nothing → reads as thin. It must NOT
    # escalate to a browser render (even with --render), and warns distinctly.
    _stub_get(
        monkeypatch,
        body=make_pdf(text=""),
        content_type="application/pdf",
        decode_body=False,
    )
    called: list[str] = []
    monkeypatch.setattr(web_fetch, "render", lambda u, _b: called.append(u))
    _run(cache, "https://x.com/scan.pdf", browser=object(), force_render=True)
    assert called == []  # never escalated to render
    row = _page(cache, "https://x.com/scan.pdf")
    assert row["content_type"] == "application/pdf"
    assert not row["rendered"]
    err = capsys.readouterr().err
    assert "scanned" in err
    assert "--render" not in err  # not the JS-only message

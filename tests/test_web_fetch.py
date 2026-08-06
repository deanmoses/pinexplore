"""Tests for web_fetch.fetch_one orchestration — all offline (no network).

Covers the guards, content gates, redirect handling, change detection, failure
logging, and the headless-render escalation/fallback (``http_get`` and
``render`` stubbed). The pieces fetch_one composes are tested in their own
modules: the transport gate in test_web_http, the content-type handlers
(charset + extraction) in test_content_types, the render primitives in
test_web_render.
"""

from __future__ import annotations

import http.client
from typing import TYPE_CHECKING

import pytest
import web_cache as wc
import web_fetch
import web_http
import web_ocr
import web_render
import web_video
from content_types import extension_for

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
    # A zip is neither extractable nor sniffable: skipped, but still logged.
    _stub_get(monkeypatch, skip="content-type", content_type="application/zip")
    _run(cache, "https://x.com/dump.zip")
    assert wc.get("https://x.com/dump.zip", con=cache) is None  # no page row
    assert _fetches(cache) == [("https://x.com/dump.zip", 200, None, None)]  # logged


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
    assert wc.blob_path(sha1).exists()  # both versions kept
    assert wc.blob_path(sha2).exists()
    assert _page(cache, url)["content_sha"] == sha2  # points at latest
    assert [r[3] for r in _fetches(cache)] == [1, 0, 1]  # new, unchanged, changed


def test_unchanged_refetch_does_not_rewrite_blob(cache, monkeypatch):
    url = "https://s.com/p"
    _stub_get(monkeypatch, body=b"<html>same</html>")
    _run(cache, url, force=True)
    sha = _page(cache, url)["content_sha"]
    blob = wc.blob_path(sha)
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
    # The plain result's body text survives, after the (empty) frontmatter.
    assert (row["text"] or "").endswith("---\n\nhi")
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
# PDF documents (extracted via the PDF handler, stored as .pdf blobs)
# --------------------------------------------------------------------------- #


def test_pdf_stored_as_pdf_blob_with_extracted_text(cache, monkeypatch, make_pdf):
    raw = make_pdf(title="Spec Sheet", moddate="D:20240115093000Z")
    _stub_get(monkeypatch, body=raw, content_type="application/pdf", decode_body=False)
    _run(cache, "https://x.com/doc.pdf")
    row = _page(cache, "https://x.com/doc.pdf")
    assert row["content_type"] == "application/pdf"
    assert row["title"] == "Spec Sheet"
    assert row["last_updated"] == "2024-01-15"
    assert "Hello PDF evidence" in (row["text"] or "")
    assert not row["rendered"]  # a PDF is never a render
    # The blob's .pdf extension is derived from content_type, not stored on the row.
    assert extension_for(row["content_type"]) == "pdf"
    assert wc.blob_path(row["content_sha"], ext="pdf").exists()


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


# --------------------------------------------------------------------------- #
# Images (text via OCR, stored as .jpg/.png blobs)
# --------------------------------------------------------------------------- #

JPEG_BYTES = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00 not a real jpeg, enough for a sniff"


def _stub_ocr(monkeypatch: pytest.MonkeyPatch, text: str | None) -> None:
    monkeypatch.setattr(web_ocr, "ocr_text", lambda raw: text)


def test_image_stored_as_jpg_blob_with_ocr_text(cache, monkeypatch):
    _stub_ocr(monkeypatch, "O MELHOR FLIPPER JAMAIS FABRICADO. " * 10)
    _stub_get(
        monkeypatch, body=JPEG_BYTES, content_type="image/jpeg", decode_body=False
    )
    _run(cache, "https://x.com/flyer.jpg")
    row = _page(cache, "https://x.com/flyer.jpg")
    assert row["content_type"] == "image/jpeg"
    # Machine-read words land in the findable tier; no citable layer is
    # derived, and text_source stays NULL rather than asserting one.
    assert "O MELHOR FLIPPER" in (row["ocr_text"] or "")
    assert row["text"] is None
    assert row["text_source"] is None
    assert not row["rendered"]
    assert extension_for(row["content_type"]) == "jpg"
    assert wc.blob_path(row["content_sha"], ext="jpg").exists()


def test_image_never_renders_and_warns_when_no_text_recognized(
    cache, monkeypatch, capsys
):
    # A photo with no legible text reads as thin. It must NOT escalate to a
    # browser render (a browser can't read a JPEG either), and warns about OCR.
    _stub_ocr(monkeypatch, None)
    _stub_get(
        monkeypatch, body=JPEG_BYTES, content_type="image/jpeg", decode_body=False
    )
    called: list[str] = []
    monkeypatch.setattr(web_fetch, "render", lambda u, _b: called.append(u))
    _run(cache, "https://x.com/photo.jpg", browser=object(), force_render=True)
    assert called == []
    err = capsys.readouterr().err
    assert "OCR" in err
    assert "--render" not in err


def test_image_dedups_deterministically_on_refetch(cache, monkeypatch):
    # Image bytes are stored verbatim, so an unchanged refetch is byte-identical.
    _stub_ocr(monkeypatch, "text")
    _stub_get(
        monkeypatch, body=JPEG_BYTES, content_type="image/jpeg", decode_body=False
    )
    _run(cache, "https://x.com/flyer.jpg", force=True)
    _run(cache, "https://x.com/flyer.jpg", force=True)
    assert [r[3] for r in _fetches(cache)] == [1, 0]  # new, then unchanged


# --------------------------------------------------------------------------- #
# text_source — every fetched row records how its text was derived
# --------------------------------------------------------------------------- #


def test_html_page_records_html_text_source(cache, monkeypatch):
    _stub_get(monkeypatch, body=RICH_HTML)
    _run(cache, "https://x.com/a")
    assert _page(cache, "https://x.com/a")["text_source"] == "html"


def test_pdf_records_pdf_text_source(cache, monkeypatch, make_pdf):
    _stub_get(
        monkeypatch,
        body=make_pdf(title="Spec"),
        content_type="application/pdf",
        decode_body=False,
    )
    _run(cache, "https://x.com/doc.pdf")
    assert _page(cache, "https://x.com/doc.pdf")["text_source"] == "pdf"


def test_rendered_page_still_records_html_text_source(cache, monkeypatch):
    # `rendered` and `text_source` answer different questions: where the bytes
    # came from vs what turned them into text. A render is still HTML extraction.
    _stub_get(monkeypatch, body=THIN_HTML)
    _stub_render(monkeypatch, body=RICH_HTML)
    _run(cache, "https://spa.com/x", browser=object())
    row = _page(cache, "https://spa.com/x")
    assert row["rendered"] == 1
    assert row["text_source"] == "html"


# --------------------------------------------------------------------------- #
# A missing extraction backend must not blank an already-cached page
# --------------------------------------------------------------------------- #


def _stub_ocr_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(raw: bytes) -> str | None:
        raise web_ocr.OcrUnavailableError("no Vision on this host")

    monkeypatch.setattr(web_ocr, "ocr_text", _boom)


def test_refetch_without_ocr_backend_keeps_the_stored_text(cache, monkeypatch):
    # The cache is shared through R2, so a page OCR'd on macOS can be refetched
    # from a host with no Vision. "No backend here" is not "this image has no
    # text": blanking the row (and its FTS entry) would destroy evidence that
    # the unchanged blob still supports.
    _stub_ocr(monkeypatch, "O MELHOR FLIPPER JAMAIS FABRICADO")
    _stub_get(
        monkeypatch, body=JPEG_BYTES, content_type="image/jpeg", decode_body=False
    )
    _run(cache, "https://x.com/flyer.jpg")

    _stub_ocr_unavailable(monkeypatch)
    _run(cache, "https://x.com/flyer.jpg", force=True)

    row = _page(cache, "https://x.com/flyer.jpg")
    assert row["ocr_text"] == "O MELHOR FLIPPER JAMAIS FABRICADO"
    assert [r[3] for r in _fetches(cache)] == [1, 0]  # new, then unchanged
    # Still searchable — the OCR-tier FTS entry survived too.
    assert [h["url"] for h in wc.search("flipper", con=cache)] == [
        "https://x.com/flyer.jpg"
    ]


def test_changed_bytes_without_ocr_backend_do_not_keep_stale_text(cache, monkeypatch):
    # The other side of it: when the image itself changed, the old text no
    # longer describes the stored blob, so it must not be carried over.
    _stub_ocr(monkeypatch, "text of the first version")
    _stub_get(
        monkeypatch, body=JPEG_BYTES, content_type="image/jpeg", decode_body=False
    )
    _run(cache, "https://x.com/flyer.jpg")

    _stub_ocr_unavailable(monkeypatch)
    _stub_get(
        monkeypatch,
        body=JPEG_BYTES + b" version two",
        content_type="image/jpeg",
        decode_body=False,
    )
    _run(cache, "https://x.com/flyer.jpg", force=True)

    row = _page(cache, "https://x.com/flyer.jpg")
    assert row["text"] is None
    # The staleness rule: changed bytes clear the machine-read tier too, even
    # when this host couldn't produce a replacement reading.
    assert row["ocr_text"] is None
    assert [r[3] for r in _fetches(cache)] == [1, 1]  # new, then changed


# --------------------------------------------------------------------------- #
# A successful fetch must not downgrade a human-reviewed transcription
# --------------------------------------------------------------------------- #


def _seed_manual(con, url: str, *, body: bytes, text: str) -> None:
    """A page whose text a person wrote (what web_import leaves behind)."""
    wc.upsert_page(
        con,
        url=url,
        raw_url=url,
        content_sha=wc.content_sha(body),
        fetched_at=wc.now_iso(),
        http_status=None,
        content_type="image/jpeg",
        text=text,
        text_source="manual",
        imported=True,
    )


TRANSCRIPTION = "MECATRONICS INDÚSTRIA E COMÉRCIO LTDA. " * 8


def test_refetch_keeps_manual_text_when_bytes_are_identical(cache, monkeypatch):
    # A source that 403s (hence the import) can become fetchable later, or land
    # in a --from-file batch. Re-extracting the same bytes would replace a
    # person's reviewed transcription with OCR output and log it as changed=0.
    url = "https://x.com/flyer.jpg"
    _seed_manual(cache, url, body=JPEG_BYTES, text=TRANSCRIPTION)
    _stub_ocr(monkeypatch, "MEGATRONICS SAUTTL BUMON")
    _stub_get(
        monkeypatch, body=JPEG_BYTES, content_type="image/jpeg", decode_body=False
    )
    _run(cache, url, force=True)

    row = _page(cache, url)
    assert row["text"] == TRANSCRIPTION
    assert row["text_source"] == "manual"
    # The bytes genuinely came from the server this time, so the byte-provenance
    # flag flips even though the text provenance doesn't — they answer different
    # questions and are independent by design.
    assert row["imported"] == 0
    assert row["http_status"] == 200


def test_changed_bytes_supersede_manual_text_but_warn(cache, monkeypatch, capsys):
    # The transcription described the *old* bytes, so carrying it onto a new
    # version would be worse than losing it — but it must not vanish quietly.
    url = "https://x.com/flyer.jpg"
    _seed_manual(cache, url, body=JPEG_BYTES, text=TRANSCRIPTION)
    _stub_ocr(monkeypatch, "text of the new version " * 12)
    _stub_get(
        monkeypatch,
        body=JPEG_BYTES + b" version two",
        content_type="image/jpeg",
        decode_body=False,
    )
    _run(cache, url, force=True)

    row = _page(cache, url)
    # The new version's reading is machine-read, so it lands in ocr_text and
    # the row no longer claims a citable layer at all.
    assert row["text"] is None
    assert row["text_source"] is None
    assert "text of the new version" in (row["ocr_text"] or "")
    err = capsys.readouterr().err
    assert "transcription" in err.lower()
    # Tells the reviewer what to do about it.
    assert "re-review" in err
    assert url in err


def test_ordinary_fetch_records_imported_zero_not_null(cache, monkeypatch):
    # NULL now means "predates the column"; a fetch knows its bytes weren't
    # handed over, so it says so rather than leaving the question open.
    _stub_get(monkeypatch, body=RICH_HTML)
    _run(cache, "https://x.com/a")
    assert _page(cache, "https://x.com/a")["imported"] == 0


def test_refetch_keeps_manually_supplied_title_and_date(cache, monkeypatch, make_pdf):
    # A PDF imported with --title/--date corrections: a later refetch of the
    # same bytes must not put the document's own metadata back. Preserving the
    # transcription but not the metadata the same person set is half a rule.
    url = "https://x.com/doc.pdf"
    raw = make_pdf(title="Untitled-1", moddate="D:20200101000000Z")
    wc.upsert_page(
        cache,
        url=url,
        raw_url=url,
        content_sha=wc.content_sha(raw),
        fetched_at=wc.now_iso(),
        title="Mecatronics price list",
        last_updated="1986-05-01",
        content_type="application/pdf",
        text=TRANSCRIPTION,
        text_source="manual",
        imported=True,
    )
    _stub_get(monkeypatch, body=raw, content_type="application/pdf", decode_body=False)
    _run(cache, url, force=True)

    row = _page(cache, url)
    assert row["title"] == "Mecatronics price list"
    assert row["last_updated"] == "1986-05-01"
    assert row["text"] == TRANSCRIPTION


def test_failed_ocr_request_keeps_stored_text_on_unchanged_bytes(cache, monkeypatch):
    # Vision fails for host reasons too — timeout, out-of-memory, internal error —
    # not only for bytes it can't decode. A transient failure must not blank text
    # the byte-identical blob still supports. Safe in every case, because
    # preservation only fires when the content hash is unchanged.
    url = "https://x.com/flyer.jpg"
    _stub_ocr(monkeypatch, "O MELHOR FLIPPER JAMAIS FABRICADO")
    _stub_get(
        monkeypatch, body=JPEG_BYTES, content_type="image/jpeg", decode_body=False
    )
    _run(cache, url)

    def _fail(raw: bytes) -> str | None:
        raise web_ocr.OcrFailedError("Vision could not read this image: timeout")

    monkeypatch.setattr(web_ocr, "ocr_text", _fail)
    _run(cache, url, force=True)

    row = _page(cache, url)
    assert row["ocr_text"] == "O MELHOR FLIPPER JAMAIS FABRICADO"


# --------------------------------------------------------------------------- #
# The video path plays by the same rules as every other fetch
# --------------------------------------------------------------------------- #

VTT = b"WEBVTT\n\n00:00:01.000 --> 00:00:04.000\nthe machine shipped in 1986\n"
WATCH_URL = "https://www.youtube.com/watch?v=O-2BXTXLXIY"


def _stub_video(monkeypatch: pytest.MonkeyPatch, *, vtt: bytes = VTT) -> None:
    monkeypatch.setattr(
        web_video,
        "fetch_video",
        lambda url: web_video.VideoResp(
            title="Machine history",
            upload_date="2024-03-01",
            vtt=vtt,
            caption_note="en",
        ),
    )


def test_video_refetch_keeps_a_reviewed_transcription(cache, monkeypatch):
    # A video with no captions is exactly the case someone transcribes by hand
    # and imports under the watch URL. Captions appearing later — or any refetch
    # of identical caption bytes — must not overwrite that with machine text.
    wc.upsert_page(
        cache,
        url=WATCH_URL,
        raw_url=WATCH_URL,
        content_sha=wc.content_sha(VTT),
        fetched_at=wc.now_iso(),
        title="Reviewed title",
        last_updated="1986-05-01",
        content_type="text/vtt",
        text=TRANSCRIPTION,
        text_source="manual",
        imported=True,
    )
    _stub_video(monkeypatch)
    _run(cache, WATCH_URL, force=True)

    row = _page(cache, WATCH_URL)
    assert row["text"] == TRANSCRIPTION
    assert row["text_source"] == "manual"
    assert row["title"] == "Reviewed title"
    assert row["last_updated"] == "1986-05-01"


def test_video_fetch_without_a_prior_row_uses_the_video_metadata(cache, monkeypatch):
    # The ordinary path is unchanged: title and date come from the video, not
    # the caption file, which carries neither.
    _stub_video(monkeypatch)
    _run(cache, WATCH_URL)
    row = _page(cache, WATCH_URL)
    assert row["title"] == "Machine history"
    assert row["last_updated"] == "2024-03-01"
    assert row["text_source"] == "vtt"
    assert "shipped in 1986" in (row["text"] or "")


# --------------------------------------------------------------------------- #
# Thin detection measures the body, never the assembled metadata+body
# --------------------------------------------------------------------------- #

# A JS shell with rich og: metadata: the assembled text is fat (>200 chars of
# head tags) while the body is an empty mount point. Exactly the page whose
# thinness the metadata block must not mask.
FAT_META_THIN_BODY = (
    "<html><head><title>Fat Meta</title>"
    '<meta property="og:description" content="'
    + "A very rich page description crawlers get instead of content. " * 8
    + '"></head><body><div id="root"></div></body></html>'
).encode()


def test_thin_detection_ignores_fat_metadata_block(cache, monkeypatch, capsys):
    url = "https://spa.example/x"
    _stub_get(monkeypatch, body=FAT_META_THIN_BODY)
    _run(cache, url)  # no browser: a thin page warns instead of rendering
    row = _page(cache, url)
    assert len(row["text"] or "") > web_render.THIN_TEXT_CHARS  # fat on paper
    assert "thin content, likely JS-only" in capsys.readouterr().err


def test_thin_fat_meta_page_escalates_to_render(cache, monkeypatch):
    url = "https://spa.example/y"
    _stub_get(monkeypatch, body=FAT_META_THIN_BODY)
    _stub_render(monkeypatch, body=RICH_HTML)
    _run(cache, url, browser=object())
    assert _page(cache, url)["rendered"] == 1


# --------------------------------------------------------------------------- #
# The too-large message — self-contained, because nobody reads the docs first
# --------------------------------------------------------------------------- #


def _too_large(content_type: str, limit: int, declared: int | None) -> str:
    return web_fetch._too_large_reason(
        web_http.Resp(
            200,
            content_type,
            "https://x.com/d",
            None,
            None,
            "too-large",
            limit,
            declared,
        )
    )


# The caps below are formatter inputs, not the real policy — each type's actual
# limit is stated once, in its handler.
def test_too_large_names_the_type_whose_cap_applied():
    # A bare "too large" doesn't identify which limit that is now that they
    # differ per type, and the number alone isn't something to act on.
    msg = _too_large("application/pdf", 3 * 1024 * 1024, 7 * 1024 * 1024)
    assert "3MB cap for application/pdf" in msg
    assert "max_response_bytes" in msg


def test_too_large_reports_how_far_over_it_is():
    # A hair over and ten times over call for different decisions; the read stops
    # at the cap, so the declared length is the only thing that can tell them apart.
    assert "7.0MB response" in _too_large(
        "application/pdf", 3 * 1024 * 1024, 7 * 1024 * 1024
    )


def test_too_large_stays_readable_when_no_length_was_declared():
    msg = _too_large("application/pdf", 3 * 1024 * 1024, None)
    assert msg.startswith("response over the 3MB cap")


def test_too_large_reports_each_types_own_cap():
    assert "4MB cap for text/html" in _too_large("text/html", 4 * 1024 * 1024, None)


def test_image_with_rich_ocr_is_not_thin(cache, monkeypatch, capsys):
    # The thin probe falls back to ocr_text when a handler derives no citable
    # layer: an image Vision read a full flyer off must not warn "OCR found
    # little/no text" just because `text` is (by design) NULL.
    _stub_ocr(monkeypatch, "O MELHOR FLIPPER JAMAIS FABRICADO. " * 10)
    _stub_get(
        monkeypatch, body=JPEG_BYTES, content_type="image/jpeg", decode_body=False
    )
    _run(cache, "https://x.com/flyer.jpg")
    assert "OCR found little/no text" not in capsys.readouterr().err

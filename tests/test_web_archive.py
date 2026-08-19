"""Tests for web_archive — the Wayback client — all offline (no network).

The CDX transport (``_get_body``) and the capture transport (``http_get``) are
stubbed; pacing and backoff sleeps are disabled. What's covered is exactly the
plan's three guards against fake "it doesn't exist" plus the byte-level traps:
a refusal (empty body) is never read as "no captures", the newest capture is
selected without ``collapse``, and a gzipped ``id_`` body is decompressed on
its magic bytes into real HTML.
"""

from __future__ import annotations

import gzip
import json
import urllib.error

import pytest
import web_archive
import web_http

URL = "http://www.ipdb.org/machine.cgi?id=125"

# A CDX JSON body: header + rows, matching fl=timestamp,original,mimetype,statuscode.
HEADER = ["timestamp", "original", "mimetype", "statuscode"]


def _cdx_body(*rows: list[str]) -> bytes:
    return json.dumps([HEADER, *rows]).encode()


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    # No pacing sleeps, no backoff sleeps, fresh circuit breaker per test.
    monkeypatch.setattr(web_archive, "_sleep", lambda seconds: None)
    monkeypatch.setattr(web_archive, "_last_request", {})
    monkeypatch.setattr(web_archive, "_consecutive_refusals", 0)


def _stub_cdx(monkeypatch: pytest.MonkeyPatch, bodies: list[bytes | Exception]):
    """Feed _get_body one canned answer per attempt; returns the call log."""
    calls: list[str] = []

    def _get(url: str) -> bytes:
        calls.append(url)
        answer = bodies[min(len(calls), len(bodies)) - 1]
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(web_archive, "_get_body", _get)
    return calls


# --------------------------------------------------------------------------- #
# CDX: absence vs refusal (guard 1)
# --------------------------------------------------------------------------- #


def test_empty_result_set_is_a_real_negative(monkeypatch):
    # CDX answers a URL it holds nothing for with a parseable `[]` (measured),
    # which is the only shape allowed to mean "no captures".
    _stub_cdx(monkeypatch, [b"[]"])
    assert web_archive.newest_capture(URL) is None


def test_empty_body_is_a_refusal_not_absence(monkeypatch):
    # The rate limiter's refusal is an empty body with no error — 31 of 40
    # rapid requests, silently. Reading it as "no captures" would poison the
    # corpus with false negatives later sessions inherit.
    calls = _stub_cdx(monkeypatch, [b""])
    with pytest.raises(web_archive.CdxRefusedError):
        web_archive.newest_capture(URL)
    assert len(calls) == 3  # retried through the backoff schedule first


def test_transient_failure_recovers_on_retry(monkeypatch):
    # 504s occur on otherwise-valid queries and succeed on retry (measured).
    row = ["20200101000000", URL, "text/html", "200"]
    _stub_cdx(
        monkeypatch,
        [
            urllib.error.HTTPError(URL, 504, "Gateway Time-out", None, None),
            _cdx_body(row),
        ],
    )
    capture = web_archive.newest_capture(URL)
    assert capture is not None
    assert capture.timestamp == "20200101000000"


def test_try_capture_keeps_refusal_and_absence_loudly_apart(monkeypatch, capsys):
    # The two Nones mean opposite things downstream: only a genuine negative
    # may ever earn a "we looked and it is not there" record.
    _stub_cdx(monkeypatch, [b""])
    assert web_archive.try_capture(URL) is None
    assert "not evidence of absence" in capsys.readouterr().err

    _stub_cdx(monkeypatch, [b"[]"])
    monkeypatch.setattr(web_archive, "_consecutive_refusals", 0)
    assert web_archive.try_capture(URL) is None
    err = capsys.readouterr().err
    assert "no archive capture" in err
    assert "not evidence of absence" not in err


def test_circuit_breaker_stops_consulting_a_refusing_cdx(monkeypatch, capsys):
    # A batch of dead URLs against a refusing CDX must not become an hour of
    # per-URL backoff sleeps — after two exhausted lookups, stop asking.
    calls = _stub_cdx(monkeypatch, [b""])
    assert web_archive.try_capture(URL) is None
    assert web_archive.try_capture(URL) is None
    asked = len(calls)
    assert web_archive.try_capture(URL) is None  # skipped, not retried
    assert len(calls) == asked
    assert "skipped" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# CDX: newest-capture selection (the sort-order trap)
# --------------------------------------------------------------------------- #


def test_newest_capture_takes_the_last_200_not_the_first(monkeypatch):
    # CDX returns oldest-first; collapse keeps the FIRST (oldest) row per key,
    # which is how a prior harvest picked 2016 over 2018. Selection here is an
    # explicit max over uncollapsed rows.
    _stub_cdx(
        monkeypatch,
        [
            _cdx_body(
                ["20160101000000", URL, "text/html", "200"],
                ["20170101000000", URL, "text/html", "200"],
                ["20180101000000", URL, "text/html", "200"],
            )
        ],
    )
    capture = web_archive.newest_capture(URL)
    assert capture is not None
    assert capture.timestamp == "20180101000000"


def test_newest_capture_skips_revisits_but_is_not_blinded_by_them(monkeypatch):
    # A warc/revisit row (statuscode '-') newer than the last 200 is evidence
    # the content was UNCHANGED — the bytes live at the 200 it dedupes, so the
    # newest 200 is the right fetch. (guard 2: never filter=statuscode:200 at
    # the query, which silently drops revisit rows from view.)
    calls = _stub_cdx(
        monkeypatch,
        [
            _cdx_body(
                ["20150101000000", URL, "text/html", "200"],
                ["20190101000000", URL, "warc/revisit", "-"],
            )
        ],
    )
    capture = web_archive.newest_capture(URL)
    assert capture is not None
    assert capture.timestamp == "20150101000000"
    assert "filter" not in calls[0]


# --------------------------------------------------------------------------- #
# fetching a capture: id_ form + content-encoding (the gzip trap)
# --------------------------------------------------------------------------- #

HTML = (
    b"<html><body><article><p>Project Date: October 11, 1982. "
    + b"Real machine page text. " * 20
    + b"</p></article></body></html>"
)


def _stub_web_get(monkeypatch: pytest.MonkeyPatch, body: bytes):
    """Stub http_get for the /web/ capture fetch; returns the call log."""
    calls: list[str] = []

    def _get(url: str) -> web_http.Resp:
        calls.append(url)
        # id_ replays raw origin bytes: no decode happened upstream for gzip,
        # so mimic http_get's behavior of decoding whatever bytes it got.
        text = body.decode("utf-8", errors="replace")
        return web_http.Resp(200, "text/html", url, body, text, None)

    monkeypatch.setattr(web_archive, "http_get", _get)
    return calls


CAPTURE = web_archive.Capture("20141006120618", URL, "text/html", "200")


def test_fetch_uses_the_id_form(monkeypatch):
    # The bare /web/<ts>/ form injects the Wayback banner and ArchiveTeam
    # blurb — the cause of all 154 chrome-polluted rows.
    calls = _stub_web_get(monkeypatch, HTML)
    hit = web_archive.fetch_capture(CAPTURE)
    assert hit is not None
    assert calls == [f"https://web.archive.org/web/20141006120618id_/{URL}"]
    assert hit.capture_url == calls[0]
    assert hit.timestamp == "20141006120618"


def test_gzipped_capture_body_is_decompressed_to_html(monkeypatch):
    # id_ replays the origin's Content-Encoding; IPDB serves gzip. A client
    # that stores the wire bytes caches binary garbage — and the header can't
    # be trusted (x-archive-orig-content-encoding came back None on the very
    # capture that was gzipped), so detection is on the magic bytes.
    _stub_web_get(monkeypatch, gzip.compress(HTML))
    hit = web_archive.fetch_capture(CAPTURE)
    assert hit is not None
    assert hit.resp.raw == HTML  # what gets stored and sha'd
    assert not hit.resp.raw.startswith(b"\x1f\x8b")
    assert hit.resp.text is not None
    assert "Project Date" in hit.resp.text


def test_truncated_gzip_is_discarded_not_stored(monkeypatch, capsys):
    _stub_web_get(monkeypatch, gzip.compress(HTML)[:40])
    assert web_archive.fetch_capture(CAPTURE) is None
    assert "did not decompress" in capsys.readouterr().err


def test_redirected_capture_records_where_the_bytes_came_from(monkeypatch):
    # Wayback may redirect to the capture it actually serves; provenance must
    # record that address and timestamp, not the one we asked for.
    served = f"https://web.archive.org/web/20180505000000id_/{URL}"

    def _get(url: str) -> web_http.Resp:
        return web_http.Resp(200, "text/html", served, HTML, HTML.decode(), None)

    monkeypatch.setattr(web_archive, "http_get", _get)
    hit = web_archive.fetch_capture(CAPTURE)
    assert hit is not None
    assert hit.capture_url == served
    assert hit.timestamp == "20180505000000"


def test_capture_date_is_the_timestamps_human_face():
    assert web_archive.capture_date("20141006120618") == "2014-10-06"


def test_newer_than_stops_a_downgrade_without_fetching(monkeypatch, capsys):
    # The caller's evidence bound: a capture older than what the cache already
    # holds is reported, not fetched — the /web/ endpoint is never touched.
    _stub_cdx(monkeypatch, [_cdx_body(["20240314000000", URL, "text/html", "200"])])

    def _never(url):
        raise AssertionError("capture fetched despite an unbeaten newer_than")

    monkeypatch.setattr(web_archive, "http_get", _never)
    assert web_archive.try_capture(URL, newer_than="20260801000000") is None
    err = capsys.readouterr().err
    assert "no newer than the evidence already cached" in err
    assert "not evidence of absence" not in err  # a decline, not a refusal


def test_the_capture_already_held_is_not_refetched(monkeypatch):
    # A dead page's stale row derives newer_than from its own raw_url, so the
    # very capture it holds compares equal and is not re-downloaded — every
    # freshness window, forever, which is real bandwidth on a large PDF.
    _stub_cdx(monkeypatch, [_cdx_body(["20240314120000", URL, "text/html", "200"])])

    def _never(url):
        raise AssertionError("byte-identical capture re-downloaded")

    monkeypatch.setattr(web_archive, "http_get", _never)
    assert web_archive.try_capture(URL, newer_than="20240314120000") is None


def test_a_strictly_newer_capture_clears_the_bound(monkeypatch):
    # The bound is a bound, not a lock: the archive crawling the page again
    # after our evidence date is genuinely newer and fetches normally.
    _stub_cdx(monkeypatch, [_cdx_body(["20260101000000", URL, "text/html", "200"])])
    _stub_web_get(monkeypatch, HTML)
    hit = web_archive.try_capture(URL, newer_than="20240314120000")
    assert hit is not None


def test_gzipped_capture_decodes_with_the_replayed_header_charset(monkeypatch):
    # id_ replays the origin's headers, so a legacy page whose charset lives
    # only in its Content-Type header still decodes right after decompression
    # (http_get's own decode saw compressed bytes and is void). Latin-1 with no
    # <meta charset>: the header is the only authority.
    page = (
        "<html><body><p>" + "Réglage du plateau. " * 20 + "</p></body></html>"
    ).encode("latin-1")
    body = gzip.compress(page)

    def _get(url: str) -> web_http.Resp:
        return web_http.Resp(
            200,
            "text/html",
            url,
            body,
            body.decode("utf-8", errors="replace"),
            None,
            header_charset="iso-8859-1",
        )

    monkeypatch.setattr(web_archive, "http_get", _get)
    hit = web_archive.fetch_capture(CAPTURE)
    assert hit is not None
    assert hit.resp.text is not None
    assert "Réglage" in hit.resp.text

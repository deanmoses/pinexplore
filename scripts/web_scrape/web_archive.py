#!/usr/bin/env python3
"""Wayback Machine client for the web evidence cache (see docs/WebCache.md).

The fetcher's archive fallback: when a live fetch fails, ``try_capture`` asks
archive.org's CDX index for the newest capture of the URL and fetches its
original bytes, so a dead page becomes cached evidence instead of a dead end.
``web_fetch`` owns when to fall back and how to store the result; this module
owns only how to talk to archive.org. No SQLite here.

Also a small CLI for the research job the fallback can't reach — enumerating
what the archive holds for a URL or site prefix that no longer exists:

    uv run python scripts/web_scrape/web_archive.py list 'http://deadsite.com/page.html'
    uv run python scripts/web_scrape/web_archive.py list 'deadsite.com/' --prefix

Everything here follows the CDX behavior measured during planning (see the
plan's appendix): captures are fetched with the ``id_`` modifier so the bytes
are the origin's, not Wayback's rewritten page; ``id_`` replays the origin's
``Content-Encoding``, so a gzipped body is decompressed on its magic bytes
(the header is unreliable); and a CDX refusal — the rate limiter's empty-body
reply — is never confused with "no captures", which only a parseable empty
result set may claim.
"""

from __future__ import annotations

import argparse
import http.client
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from pathlib import Path
from typing import NamedTuple

import certifi

# Allow sibling imports whether run as a script or imported.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from content_types import SNIFF_BYTES, handler_for, sniff
from web_http import MAX_RESPONSE_BYTES, USER_AGENT, Resp, http_get

CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
WEB_PREFIX = "https://web.archive.org/web/"

# The two endpoints live on one host but tolerate very different request rates
# (measured: 40 rapid CDX requests died after 9; ~85 sequential /web/ fetches
# at 3s survived), so each gets its own budget rather than sharing web_fetch's
# per-host limiter.
CDX_RATE_SECONDS = 3.0
WEB_RATE_SECONDS = 3.0

# A refused CDX request is retried with growing pauses; the refusal is service
# protection, not a block, and transient 504s succeed on retry.
_CDX_BACKOFF_SECONDS = (4.0, 12.0)

# A CDX response is small (a whole-host sweep measured 681KB); the cap stops a
# runaway body from being buffered without bounding real queries.
_CDX_MAX_BYTES = 32 * 1024 * 1024

# After this many consecutive exhausted-retry refusals, stop consulting CDX for
# the rest of the run: the service is saying no, and a batch of dead URLs must
# not turn that into an hour of per-URL backoff sleeps.
_CDX_CIRCUIT_REFUSALS = 2
_consecutive_refusals = 0

# Indirection so tests can run the retry loop without real sleeps.
_sleep = time.sleep

type Seconds = float
# One entry per endpoint bucket ("cdx" / "web"), monotonic-clock timestamps.
_last_request: dict[str, Seconds] = {}

_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

_GZIP_MAGIC = b"\x1f\x8b"

# The capture address a fetch actually landed on: /web/<timestamp>[id_]/...
_CAPTURE_TIMESTAMP = re.compile(r"/web/(\d{4,14})(?:id_)?/")


class CdxRefusedError(Exception):
    """CDX refused or failed to answer — NOT evidence of absence.

    The rate limiter's refusal is an empty body with no error, so a caller
    that read it as "no captures" would record a false negative that later
    sessions inherit and trust. Only a response that parses as an (empty)
    result set may say the archive holds nothing.
    """


class Capture(NamedTuple):
    """One CDX index row."""

    timestamp: str  # yyyyMMddhhmmss
    original: str  # the URL as the crawler requested it (http/https both occur)
    mimetype: str
    statuscode: str  # '-' on a warc/revisit row (content unchanged that day)


class ArchiveHit(NamedTuple):
    """A capture fetched and ready to store."""

    resp: Resp  # original bytes, content-encoding undone, text re-extracted
    capture_url: str  # the /web/<ts>id_/ address the bytes actually came from
    timestamp: str  # yyyyMMddhhmmss of the capture served


def capture_date(timestamp: str) -> str:
    """``yyyyMMdd...`` → ``YYYY-MM-DD``, the human face of a capture timestamp."""
    return f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}"


def _pace(bucket: str, seconds: Seconds) -> None:
    """Sleep so consecutive requests in one budget stay >= ``seconds`` apart."""
    last = _last_request.get(bucket)
    if last is not None:
        wait = seconds - (time.monotonic() - last)
        if wait > 0:
            _sleep(wait)
    _last_request[bucket] = time.monotonic()


def _get_body(url: str) -> bytes:
    """Plain GET for the CDX endpoint (JSON, no content-type gate, no policy).

    ``web_http.http_get`` is deliberately not reused here: it gates on the
    content types the *cache* stores, and a CDX answer is an index listing,
    not evidence.
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60, context=_SSL_CONTEXT) as resp:
        body: bytes = resp.read(_CDX_MAX_BYTES + 1)
    if len(body) > _CDX_MAX_BYTES:
        raise CdxRefusedError(f"CDX response over {_CDX_MAX_BYTES} bytes")
    return body


def _cdx_rows(query_url: str) -> list[list[str]]:
    """The parsed JSON rows for one CDX query, retried through refusals.

    Every failure shape retries — a transport error, an HTTP error (transient
    504s succeed on retry), and the refusal's unparseable empty body alike —
    and exhausting the retries raises ``CdxRefusedError`` rather than returning
    anything a caller could mistake for an empty result set.
    """
    reason = "refused"
    for attempt in range(len(_CDX_BACKOFF_SECONDS) + 1):
        if attempt:
            _sleep(_CDX_BACKOFF_SECONDS[attempt - 1])
        _pace("cdx", CDX_RATE_SECONDS)
        try:
            body = _get_body(query_url)
        except (
            urllib.error.URLError,  # HTTPError included — it subclasses URLError
            TimeoutError,
            http.client.HTTPException,
            OSError,
        ) as exc:
            reason = str(exc)
            continue
        try:
            rows = json.loads(body)
        except json.JSONDecodeError:
            # The rate limiter's signature: an empty (or truncated) body with a
            # clean status. Not an answer, so never an empty result set.
            reason = "empty/unparseable body (rate-limit refusal)"
            continue
        if isinstance(rows, list):
            return rows
        reason = "unexpected JSON shape"
    raise CdxRefusedError(reason)


def cdx_captures(
    url: str,
    *,
    match_type: str | None = None,
    collapse: str | None = None,
    limit: int | None = None,
) -> list[Capture]:
    """CDX index rows for a URL, oldest first (the index's own order).

    ``url`` goes into the query string as given (percent-encoding is
    ``urlencode``'s job, so a URL containing its own query string is safe).
    An empty list is a real answer — the archive holds nothing — because a
    refusal raises ``CdxRefusedError`` instead. ``collapse`` is for "which URLs
    exist" (prefix enumeration) only: it keeps the *oldest* capture per key,
    so it must never feed "which capture to fetch".
    """
    params: list[tuple[str, str]] = [
        ("url", url),
        ("output", "json"),
        ("fl", "timestamp,original,mimetype,statuscode"),
    ]
    if match_type:
        params.append(("matchType", match_type))
    if collapse:
        params.append(("collapse", collapse))
    if limit:
        params.append(("limit", str(limit)))
    rows = _cdx_rows(CDX_ENDPOINT + "?" + urllib.parse.urlencode(params))
    if not rows:
        return []
    header, *data = rows
    idx = {name: i for i, name in enumerate(header)}
    return [
        Capture(
            timestamp=row[idx["timestamp"]],
            original=row[idx["original"]],
            mimetype=row[idx["mimetype"]],
            statuscode=row[idx["statuscode"]],
        )
        for row in data
    ]


def newest_capture(url: str) -> Capture | None:
    """The newest 200 capture of a URL, or None when the archive holds none.

    Selected with an explicit ``max`` over uncollapsed rows: CDX sorts oldest
    first and ``collapse=urlkey`` keeps the *first* row per key, so any
    collapse-based "newest" quietly returns the oldest capture. Revisit rows
    (statuscode ``-``) are newer evidence the content was unchanged, but the
    bytes live at the 200 capture they dedupe, so the newest 200 is the right
    fetch either way.
    """
    best: Capture | None = None
    for capture in cdx_captures(url):
        if capture.statuscode == "200" and (
            best is None or capture.timestamp > best.timestamp
        ):
            best = capture
    return best


def _decode_content_encoding(resp: Resp) -> Resp | None:
    """Undo the origin's Content-Encoding that ``id_`` replays verbatim.

    Detected on the gzip magic bytes, not the header — the very capture that
    motivated this came back with no ``x-archive-orig-content-encoding`` while
    its body was gzip — and bounded by the handler's own byte cap so a bomb
    can't inflate past what a direct fetch would have accepted. The text is
    re-decoded from the real bytes with the charset the origin's replayed
    header declared (``http_get``'s own decode saw compressed bytes, so it is
    void): a gzipped legacy page whose charset lives only in that header would
    otherwise fall to statistical detection. None means the body claimed gzip
    and didn't decompress: storing it would cache binary garbage as evidence.
    """
    if resp.raw is None or not resp.raw.startswith(_GZIP_MAGIC):
        return resp
    handler = handler_for(resp.content_type)
    cap = (handler.max_response_bytes if handler else None) or MAX_RESPONSE_BYTES
    decomp = zlib.decompressobj(wbits=31)  # 31 = gzip container
    try:
        raw = decomp.decompress(resp.raw, cap + 1)
    except zlib.error:
        return None
    if not decomp.eof or len(raw) > cap:
        if len(raw) > cap:
            return resp._replace(
                raw=None, text=None, skip="too-large", limit=cap, declared_size=None
            )
        return None
    # The decompressed bytes may reveal a signature the gzip prefix hid
    # (mirrors http_get's own sniff-before-trusting-the-header step).
    sniffed = sniff(raw[:SNIFF_BYTES])
    if sniffed is not None:
        handler = sniffed
        resp = resp._replace(content_type=sniffed.canonical_mime)
    text = handler.decode(raw, resp.header_charset) if handler else None
    return resp._replace(raw=raw, text=text)


def fetch_capture(capture: Capture) -> ArchiveHit | None:
    """Fetch a capture's original bytes, ready for the cache to store.

    Always through the ``id_`` form: the rewritten page injects the Wayback
    banner and ArchiveTeam blurb into the stored text (the cause of every
    chrome-polluted row this replaced). Reuses ``http_get`` so the capture
    passes the same content-type and size gates a live fetch would, then
    undoes the replayed content-encoding. None means the capture couldn't be
    fetched cleanly — which is a failure to report, never a claim the archive
    holds nothing.
    """
    target = f"{WEB_PREFIX}{capture.timestamp}id_/{capture.original}"
    for attempt in range(2):
        if attempt:
            _sleep(WEB_RATE_SECONDS)
        _pace("web", WEB_RATE_SECONDS)
        try:
            resp = http_get(target)
        except urllib.error.HTTPError as exc:
            print(f"archive fetch HTTP {exc.code}: {target}", file=sys.stderr)
            if exc.code == 404:  # the index said it exists; retrying won't help
                return None
            continue
        except (
            urllib.error.URLError,
            TimeoutError,
            ValueError,
            http.client.HTTPException,
        ) as exc:
            print(f"archive fetch failed: {target} ({exc})", file=sys.stderr)
            continue
        decoded = _decode_content_encoding(resp)
        if decoded is None:
            print(
                f"archive fetch discarded: {target} (gzip body did not "
                f"decompress; storing it would cache binary garbage)",
                file=sys.stderr,
            )
            return None
        # Wayback may redirect to the capture it actually serves; the address
        # the bytes came from is the provenance worth recording.
        match = _CAPTURE_TIMESTAMP.search(decoded.final_url)
        timestamp = match.group(1) if match else capture.timestamp
        return ArchiveHit(decoded, decoded.final_url, timestamp)
    return None


def try_capture(url: str, *, newer_than: str | None = None) -> ArchiveHit | None:
    """The fetcher's fallback: the newest capture of ``url``, fetched, or None.

    The two Nones a caller cannot act on differently are kept loudly apart on
    stderr, because they mean opposite things downstream: "no archive capture"
    is a real negative (the kind that may earn a ``document_hunts`` row), while
    a refusal or fetch failure is no evidence of anything and must never be
    recorded as "we looked and it is not there".

    ``newer_than`` (``yyyyMMddhhmmss``; a shorter prefix compares fine) is the
    caller's evidence bound: a capture not strictly newer is reported but not
    fetched. That is two guards in one comparison — the *downgrade* guard (a
    page cached live in August whose site then dies must not have its text
    replaced by an older capture on the next routine refetch) and the
    *idempotence* guard (a dead page's stale row must not re-download its own
    byte-identical capture every freshness window — invisible for HTML, real
    bandwidth for a large PDF).
    """
    global _consecutive_refusals
    if _consecutive_refusals >= _CDX_CIRCUIT_REFUSALS:
        print(
            f"archive lookup skipped (CDX refused {_consecutive_refusals} "
            f"lookups in a row this run) — not evidence of absence: {url}",
            file=sys.stderr,
        )
        return None
    try:
        capture = newest_capture(url)
    except CdxRefusedError as exc:
        _consecutive_refusals += 1
        print(
            f"archive lookup refused ({exc}) — not evidence of absence, "
            f"retry later: {url}",
            file=sys.stderr,
        )
        return None
    _consecutive_refusals = 0
    if capture is None:
        print(f"no archive capture: {url}", file=sys.stderr)
        return None
    if newer_than is not None and capture.timestamp <= newer_than:
        print(
            f"archive not used: its newest capture "
            f"({capture_date(capture.timestamp)}) is no newer than the "
            f"evidence already cached ({capture_date(newer_than)}); keeping "
            f"what is stored: {url}",
            file=sys.stderr,
        )
        return None
    return fetch_capture(capture)


# --------------------------------------------------------------------------- #
# CLI — enumerate what the archive holds
# --------------------------------------------------------------------------- #


def _cmd_list(url: str, *, prefix: bool, limit: int) -> int:
    """Print the archive's captures for a URL (or every URL under a prefix).

    The research verb: a site that no longer exists cannot be crawled or
    searched, so this is the only enumeration of it there is. Prefix mode
    collapses to one row per URL (the oldest — fine for "which URLs exist",
    never for "which capture to fetch"; ``list`` the exact URL for that).
    """
    try:
        captures = cdx_captures(
            url,
            match_type="prefix" if prefix else None,
            collapse="urlkey" if prefix else None,
            limit=limit,
        )
    except CdxRefusedError as exc:
        print(
            f"CDX refused ({exc}) — not evidence of absence, retry later",
            file=sys.stderr,
        )
        return 2
    for cap in captures:
        status = cap.statuscode
        note = "  (revisit: content unchanged that day)" if status == "-" else ""
        print(
            f"{capture_date(cap.timestamp)}  {cap.timestamp}  {status:>3}  "
            f"{cap.mimetype}  {cap.original}{note}"
        )
    sys.stdout.flush()
    if not captures:
        print(f"no captures: {url}", file=sys.stderr)
        return 1
    shown = "URLs (oldest capture each)" if prefix else "captures"
    at_cap = " — raise --limit for more" if len(captures) == limit else ""
    print(f"{len(captures)} {shown}{at_cap}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Enumerate archive.org's captures of a URL or dead site. "
        "(Fetching a capture into the cache is web_fetch.py's job — it falls "
        "back to the archive on its own when a live fetch fails.)"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p_list = sub.add_parser("list", help="captures of a URL, oldest first")
    p_list.add_argument("url", help="exact URL, or a site prefix with --prefix")
    p_list.add_argument(
        "--prefix",
        action="store_true",
        help="list every archived URL under this prefix (one row per URL)",
    )
    p_list.add_argument(
        "--limit", type=int, default=200, help="max rows (default %(default)s)"
    )
    args = parser.parse_args(argv)
    return _cmd_list(args.url, prefix=args.prefix, limit=args.limit)


if __name__ == "__main__":
    sys.exit(main())

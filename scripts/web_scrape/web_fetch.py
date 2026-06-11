#!/usr/bin/env python3
"""Polite web fetcher for the web evidence cache (see docs/WebCache.md).

Fetches a page once, extracts readable text (trafilatura) and the page's own
date (htmldate, conservatively), stores the raw HTML blob and an upserted
``pages`` row in the SQLite cache, then logs the fetch event (and the search
intent that drove it).

Politeness: a descriptive User-Agent, a per-domain rate limit, and an
idempotent skip when the URL was fetched within the max-age window.

    # one page, recording the search intent that led here
    uv run python scripts/web_scrape/web_fetch.py <url> --query "haggis pinball closed 2024"
    # batch: a TSV of `url<TAB>query` (blank query allowed; '#' lines skipped)
    uv run python scripts/web_scrape/web_fetch.py --from-file urls.tsv
    # refetch even if fresh; tune the freshness window
    uv run python scripts/web_scrape/web_fetch.py <url> --query "..." --force --max-age 7
"""

from __future__ import annotations

import argparse
import http.client
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, NamedTuple

if TYPE_CHECKING:
    import sqlite3

# Allow `import web_cache` whether run as a script or imported.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import web_cache

USER_AGENT = (
    "pinexplore-webevidence/1.0 "
    "(+https://github.com/deanmoses/pindata; pinball catalog evidence cache)"
)
DEFAULT_MAX_AGE_DAYS = 30
RATE_LIMIT_SECONDS = 2.0
MAX_RESPONSE_BYTES = 10 * 1024 * 1024  # cap the body we'll buffer + extract
HTML_CONTENT_TYPES = {"text/html", "application/xhtml+xml"}


SkipReason = Literal["content-type", "too-large"]


class _Resp(NamedTuple):
    """Result of an HTTP GET. ``raw``/``text`` are None when we declined the body
    (``skip`` set to 'content-type' or 'too-large'); ``final_url`` is post-redirect."""

    status: int
    content_type: str
    final_url: str
    raw: bytes | None
    text: str | None
    skip: SkipReason | None


type Domain = str
# Seconds from time.monotonic() — a relative clock for measuring elapsed time,
# never comparable to wall-clock/epoch timestamps.
type MonotonicSeconds = float

# Per-domain timestamp of the last request, for the rate limiter.
_last_request: dict[Domain, MonotonicSeconds] = {}


def _parse_iso(ts: str) -> datetime:
    """Parse one of our ISO8601 'Z' timestamps to an aware datetime."""
    return datetime.fromisoformat(ts)


def _rate_limit(domain: Domain) -> None:
    """Sleep so consecutive requests to the same domain are >= RATE_LIMIT apart."""
    last = _last_request.get(domain)
    if last is not None:
        wait = RATE_LIMIT_SECONDS - (time.monotonic() - last)
        if wait > 0:
            time.sleep(wait)
    _last_request[domain] = time.monotonic()


# --------------------------------------------------------------------------- #
# HTTP fetch + extraction
# --------------------------------------------------------------------------- #


def _decode_body(raw: bytes, charset: str) -> str:
    """Decode response bytes, falling back to utf-8 on an unknown charset label.

    A page can advertise a junk ``charset=`` (e.g. ``utf-8x-bogus``); passing it
    straight to ``bytes.decode`` raises ``LookupError``, which would escape
    ``fetch_one``'s except tuple and crash a whole ``--from-file`` batch. A bad
    label shouldn't lose the page — decode as utf-8 instead. ``errors="replace"``
    throughout so undecodable bytes never raise either.
    """
    try:
        return raw.decode(charset, errors="replace")
    except LookupError:
        return raw.decode("utf-8", errors="replace")


def _http_get(url: str) -> _Resp:
    """GET a URL with our UA, gating on content-type and response size.

    Skips downloading the body for non-HTML content types, and caps the read at
    MAX_RESPONSE_BYTES so a giant response can't be buffered into memory then
    charset-decoded into garbage. ``final_url`` is the post-redirect URL.
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        status = resp.status
        content_type = resp.headers.get_content_type()
        final_url = resp.geturl()
        charset = resp.headers.get_content_charset() or "utf-8"
        if content_type not in HTML_CONTENT_TYPES:
            return _Resp(status, content_type, final_url, None, None, "content-type")
        raw = resp.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        return _Resp(status, content_type, final_url, None, None, "too-large")
    text = _decode_body(raw, charset)
    return _Resp(status, content_type, final_url, raw, text, None)


class ExtractedMeta(NamedTuple):
    """Readable content pulled from a page. Any field may be None."""

    title: str | None
    last_updated: str | None
    text: str | None


def _extract(html: str, url: str) -> ExtractedMeta:
    """Run trafilatura for text/title; extract the date conservatively.

    ``last_updated`` is a real date stated on the page, or None — never a guess.
    trafilatura's default date extraction pads a weak year-only signal (a stray
    "© 2024", a bare-year meta) up to a fabricated `YYYY-01-01`, which would
    corrupt the "is this still live / need a 2023+ source" judgment. We instead
    ask htmldate with ``extensive_search=False``, which returns None rather than
    pad — for evidence, no date beats a wrong one. ``original_date`` is left False
    (htmldate's default) so we get the page's most recent date, matching the
    ``last_updated`` column name and the recency check.
    """
    import htmldate
    import trafilatura

    title: str | None = None
    text: str | None = None
    doc = trafilatura.bare_extraction(html, url=url, with_metadata=True)
    if doc is not None:
        title = getattr(doc, "title", None)
        text = getattr(doc, "text", None)
    # Fall back to a plain text extraction if metadata extraction yielded none.
    if not text:
        text = trafilatura.extract(html, url=url)
    try:
        last_updated = htmldate.find_date(html, extensive_search=False)
    except Exception:
        last_updated = None
    return ExtractedMeta(title=title, last_updated=last_updated, text=text)


# --------------------------------------------------------------------------- #
# Fetch one page
# --------------------------------------------------------------------------- #


def fetch_one(
    con: sqlite3.Connection,
    raw_url: str,
    *,
    query: str | None,
    force: bool,
    max_age_days: int,
) -> None:
    try:
        url = web_cache.normalize_url(raw_url)
        parts = urllib.parse.urlsplit(url)
        scheme_ok = parts.scheme in ("http", "https")
        host = parts.hostname
    except ValueError as exc:
        # normalize_url / urlsplit raise ValueError on a malformed URL (a bad port
        # like ':abc', an invalid IPv6 literal). A garbage --from-file row must
        # skip, not abort the whole batch — this runs before the fetch try below.
        print(f"skip (malformed URL): {raw_url} ({exc})", file=sys.stderr)
        return
    if not scheme_ok or not host:
        print(f"skip (unsupported or malformed URL): {raw_url}", file=sys.stderr)
        return
    domain = host

    existing = web_cache.get(url, con=con)
    # For the freshness skip, also match a prior fetch that redirected: its row is
    # keyed by the post-redirect URL, but raw_url holds the as-requested form — so
    # a redirecting source gets fetch-once-reuse instead of being re-followed every
    # run. (`existing`, keyed strictly by the requested URL, still drives change
    # detection below; the post-fetch redirect reconciliation re-keys it.)
    fresh_row = existing or web_cache.get_by_raw_url(raw_url, con=con)
    if fresh_row and not force:
        age_days = (datetime.now(UTC) - _parse_iso(fresh_row["last_fetched_at"])).days
        if age_days <= max_age_days:
            canonical = fresh_row["url"]
            print(f"skip (fresh, {age_days}d): {canonical}")
            return

    _rate_limit(domain)
    fetched_at = web_cache.now_iso()
    try:
        resp = _http_get(url)
    except urllib.error.HTTPError as exc:
        print(f"HTTP {exc.code}: {url}", file=sys.stderr)
        web_cache.append_fetch(
            con,
            url=url,
            fetched_at=fetched_at,
            search_query=query,
            http_status=exc.code,
        )
        return
    except (
        urllib.error.URLError,
        TimeoutError,
        ValueError,
        http.client.HTTPException,
    ) as exc:
        # Log the failed attempt too — fetches is an audit of *every* fetch.
        # Beyond ordinary network errors, this catches the malformed-URL errors
        # urlopen raises that slip past the netloc guard — http.client.InvalidURL
        # (an HTTPException) for a host with spaces/control chars, ValueError for
        # an unknown url type, IDNA UnicodeError — so one bad --from-file line
        # can't crash the whole batch.
        print(f"FAILED: {url} ({exc})", file=sys.stderr)
        web_cache.append_fetch(
            con, url=url, fetched_at=fetched_at, search_query=query, http_status=None
        )
        return

    if resp.skip:
        why = (
            f"non-HTML {resp.content_type}"
            if resp.skip == "content-type"
            else f"response > {MAX_RESPONSE_BYTES // (1024 * 1024)}MB"
        )
        print(f"skip ({why}): {url}", file=sys.stderr)
        web_cache.append_fetch(
            con,
            url=url,
            fetched_at=fetched_at,
            search_query=query,
            http_status=resp.status,
        )
        return

    # Reconcile redirects: key the row on where the content actually lives (the
    # post-redirect URL), so a 301'd URL dedups against a future direct fetch of
    # the canonical address. raw_url keeps the originally-requested URL.
    final_url = web_cache.normalize_url(resp.final_url)
    if final_url != url:
        url = final_url
        existing = web_cache.get(url, con=con)

    # Both are guaranteed non-None when skip is None (see _http_get).
    assert resp.raw is not None
    assert resp.text is not None
    # Content-address the blob so each distinct version is preserved. An unchanged
    # refetch resolves to the same file (no rewrite); a changed one writes a new
    # blob alongside the old. `changed` is relative to the version last stored.
    content_sha = web_cache.content_sha(resp.raw)
    changed = existing is None or existing.get("content_sha") != content_sha
    blob = web_cache.html_path(content_sha)
    if not blob.exists():
        blob.write_bytes(resp.raw)

    meta = _extract(resp.text, url)
    web_cache.upsert_page(
        con,
        url=url,
        raw_url=raw_url,
        content_sha=content_sha,
        html_file=web_cache.html_rel(content_sha),
        fetched_at=fetched_at,
        last_updated=meta.last_updated,
        title=meta.title,
        http_status=resp.status,
        content_type=resp.content_type,
        text=meta.text,
    )
    web_cache.append_fetch(
        con,
        url=url,
        fetched_at=fetched_at,
        search_query=query,
        http_status=resp.status,
        content_sha=content_sha,
        changed=changed,
    )
    state = "new" if existing is None else ("changed" if changed else "unchanged")
    title = meta.title or "(no title)"
    print(f"fetched [{resp.status}] ({state}): {url}\n    {title}")


# --------------------------------------------------------------------------- #
# Batch input + CLI
# --------------------------------------------------------------------------- #


class FetchRequest(NamedTuple):
    """A URL to fetch and the search intent that led to it (query optional)."""

    url: str
    query: str | None


def _read_tsv(path: str) -> list[FetchRequest]:
    """Parse a `url<TAB>query` TSV. Blank/`#` lines skipped; query optional."""
    requests: list[FetchRequest] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        url, _, query = line.partition("\t")
        url = url.strip()
        query_val = query.strip() or None
        if url:
            requests.append(FetchRequest(url, query_val))
    return requests


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Polite fetcher for the web evidence cache."
    )
    parser.add_argument("url", nargs="?", help="A single URL to fetch.")
    parser.add_argument(
        "--query", help="The search intent that led to this URL (logged)."
    )
    parser.add_argument(
        "--from-file", help="A TSV of `url<TAB>query` to fetch in batch."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Refetch even if the page is within the max-age window.",
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=DEFAULT_MAX_AGE_DAYS,
        help=f"Freshness window in days (default: {DEFAULT_MAX_AGE_DAYS}).",
    )
    args = parser.parse_args()

    con = web_cache.connect()
    web_cache.init_schema(con)

    try:
        if args.from_file:
            requests = _read_tsv(args.from_file)
        elif args.url:
            requests = [FetchRequest(args.url, args.query)]
        else:
            parser.error("provide a URL or --from-file")

        for raw_url, query in requests:
            fetch_one(
                con,
                raw_url,
                query=query,
                force=args.force,
                max_age_days=args.max_age,
            )
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

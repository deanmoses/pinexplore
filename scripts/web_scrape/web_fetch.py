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
import contextlib
import http.client
import re
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


# A page can declare its charset in an HTML <meta> tag two ways: the HTML5
# ``<meta charset="...">`` and the legacy
# ``<meta http-equiv="Content-Type" content="text/html; charset=...">``. Both put
# the value right after a ``charset=``, so one pattern catches them.
_META_CHARSET_RE = re.compile(
    rb"""<meta[^>]+?charset\s*=\s*["']?\s*([a-zA-Z0-9_.:-]+)""",
    re.IGNORECASE,
)

# Windows-authored Japanese pages routinely declare ``Shift_JIS`` but actually use
# cp932 (its superset, with NEC/IBM extension characters like ①, ㈱). Python's
# strict ``shift_jis`` codec mangles those extension bytes, so decode the whole
# family as cp932 — it round-trips genuine Shift_JIS unchanged.
_CHARSET_ALIASES = {
    "shift_jis": "cp932",
    "shift-jis": "cp932",
    "shiftjis": "cp932",
    "sjis": "cp932",
    "x-sjis": "cp932",
}


def _sniff_meta_charset(raw: bytes) -> str | None:
    """Return the charset an HTML page declares in a ``<meta>`` tag, or None.

    Per the HTML spec the declaration must appear in the first 1024 bytes, so we
    only scan that prefix (and bound a stray match deeper in the body). Matches
    both ``<meta charset="shift_jis">`` and the legacy
    ``<meta http-equiv="Content-Type" content="text/html; charset=Shift_JIS">``.
    """
    match = _META_CHARSET_RE.search(raw[:1024])
    if match is None:
        return None
    return match.group(1).decode("ascii", errors="replace")


def _detect_charset(raw: bytes) -> str | None:
    """Statistically detect the charset of undeclared bytes, or None.

    The last resort when neither the HTTP header nor the HTML declares a charset
    (common for old Japanese pages served as Shift-JIS). charset-normalizer is
    already an indirect dependency via trafilatura.
    """
    from charset_normalizer import from_bytes

    best = from_bytes(raw).best()
    return best.encoding if best is not None else None


def _try_decode(raw: bytes, label: str | None) -> str | None:
    """Decode ``raw`` using charset ``label``, or None if the label is empty or
    unknown to Python's codecs (a junk ``charset=`` shouldn't raise and lose the
    page). The Shift_JIS family is upgraded to its cp932 superset first."""
    if not label:
        return None
    codec = _CHARSET_ALIASES.get(label.strip().lower(), label)
    try:
        return raw.decode(codec, errors="replace")
    except LookupError:
        return None


def _decode_body(raw: bytes, header_charset: str | None) -> str:
    """Decode response bytes to text, resolving the charset in priority order:

    1. the HTTP ``Content-Type`` charset, when the server sent one (authoritative);
    2. a ``<meta>`` charset the HTML declares about itself;
    3. charset-normalizer's statistical detection;
    4. utf-8, as a last resort.

    The motivating bug: old Japanese pages served as Shift-JIS/cp932 with *no*
    charset header — a blind utf-8 decode turned their titles to mojibake. An
    unknown/junk label (e.g. a bogus ``charset=utf-8x-bogus``, which would raise
    ``LookupError`` and escape ``fetch_one``'s except tuple) is skipped rather than
    allowed to lose the page; ``errors="replace"`` throughout so undecodable bytes
    never raise either.
    """
    # Header then <meta>; only if neither yields a usable label do we run the
    # (relatively costly) statistical detection.
    for label in (header_charset, _sniff_meta_charset(raw)):
        decoded = _try_decode(raw, label)
        if decoded is not None:
            return decoded
    detected = _try_decode(raw, _detect_charset(raw))
    return detected if detected is not None else raw.decode("utf-8", errors="replace")


def _request_url(url: str) -> str:
    """Make a normalized URL safe to put on the HTTP request line.

    ``normalize_url`` keeps the readable UTF-8 form (the cache key), but urllib
    will not encode it: a non-ASCII host or path (e.g. a Japanese Weblio article,
    ``/content/サンワイズ``) raises ``UnicodeEncodeError`` when urllib tries to
    ASCII-encode the request line. So IDNA-encode a non-ASCII host and
    percent-encode non-ASCII path/query bytes here, for the wire only. Idempotent:
    an already-ASCII or already-percent-encoded URL is unchanged (``%`` is in the
    safe set, so ``%E3`` is not double-encoded).
    """
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or ""
    if host and not host.isascii():
        # leave as-is on failure; urlopen surfaces a clear error if it's truly bad
        with contextlib.suppress(UnicodeError):
            host = host.encode("idna").decode("ascii")
    # parts.hostname strips the brackets an IPv6 literal needs; restore them, or
    # the rebuilt netloc (e.g. ``::1:8080``) becomes ambiguous/malformed. A domain
    # or IDNA-encoded host never contains ':', so this only fires for IPv6.
    netloc = f"[{host}]" if ":" in host else host
    if parts.port is not None:
        netloc = f"{netloc}:{parts.port}"
    if parts.username:
        netloc = (
            parts.username
            + (f":{parts.password}" if parts.password else "")
            + f"@{netloc}"
        )
    path = urllib.parse.quote(parts.path, safe="/%:@-._~!$&'()*+,;=")
    query = urllib.parse.quote(parts.query, safe="%:@-._~!$&'()*+,;=/?")
    return urllib.parse.urlunsplit((parts.scheme, netloc, path, query, ""))


def _http_get(url: str) -> _Resp:
    """GET a URL with our UA, gating on content-type and response size.

    Skips downloading the body for non-HTML content types, and caps the read at
    MAX_RESPONSE_BYTES so a giant response can't be buffered into memory then
    charset-decoded into garbage. ``final_url`` is the post-redirect URL — the
    readable input ``url`` when we landed where we asked, else the redirect target.
    """
    request_url = _request_url(url)
    req = urllib.request.Request(request_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        status = resp.status
        content_type = resp.headers.get_content_type()
        landed = resp.geturl()
        # geturl() echoes the (encoded) request URL when there was no redirect;
        # return the original readable url then, so a non-ASCII page isn't re-keyed
        # to its percent-encoded form on every fetch.
        final_url = url if landed == request_url else landed
        # None (not a utf-8 default) when the server omits the charset, so
        # _decode_body can fall through to the page's own <meta>/detection — the
        # headerless Shift-JIS case that a blind utf-8 decode mojibakes.
        header_charset = resp.headers.get_content_charset()
        if content_type not in HTML_CONTENT_TYPES:
            return _Resp(status, content_type, final_url, None, None, "content-type")
        raw = resp.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        return _Resp(status, content_type, final_url, None, None, "too-large")
    text = _decode_body(raw, header_charset)
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

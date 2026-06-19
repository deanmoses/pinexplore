#!/usr/bin/env python3
"""Polite web fetcher for the web evidence cache (see docs/WebCache.md).

The CLI entry point and per-URL orchestration. Fetches a page once
(``web_http``), extracts readable text and the page's own date (``web_extract``),
escalating to a headless render for JavaScript-only pages (``web_render``), then
stores the raw blob and an upserted ``pages`` row in the SQLite cache
(``web_cache``) and logs the fetch event (and the search intent that drove it).

Politeness: a descriptive User-Agent, a per-domain rate limit, and an
idempotent skip when the URL was fetched within the max-age window.

    # one page, recording the search intent that led here
    uv run python scripts/web_scrape/web_fetch.py <url> --query "haggis pinball closed 2024"
    # batch: a TSV of `url<TAB>query` (blank query allowed; '#' lines skipped)
    uv run python scripts/web_scrape/web_fetch.py --from-file urls.tsv
    # refetch even if fresh; tune the freshness window
    uv run python scripts/web_scrape/web_fetch.py <url> --query "..." --force --max-age 7

JavaScript-rendered pages: when the plain GET extracts to near-nothing (an SPA
skeleton), the fetcher escalates to a headless-Chromium render and stores that
DOM, marked ``rendered``. The fallback is on by default; ``--no-render`` disables
it, ``--render`` forces it, ``--thin-chars`` tunes the threshold. See
docs/JsFetch.md.
"""

from __future__ import annotations

import argparse
import http.client
import sys
import time
import urllib.error
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    import sqlite3

# Allow sibling imports whether run as a script or imported.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import web_cache
from web_extract import extract
from web_http import MAX_RESPONSE_BYTES, http_get
from web_render import (
    THIN_TEXT_CHARS,
    BrowserUnavailableError,
    LazyBrowser,
    is_thin,
    render,
)

DEFAULT_MAX_AGE_DAYS = 30
RATE_LIMIT_SECONDS = 2.0


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
# Fetch one page
# --------------------------------------------------------------------------- #


def fetch_one(
    con: sqlite3.Connection,
    raw_url: str,
    *,
    query: str | None,
    force: bool,
    max_age_days: int,
    browser: LazyBrowser | None = None,
    force_render: bool = False,
    thin_chars: int = THIN_TEXT_CHARS,
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
        resp = http_get(url)
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

    # text is guaranteed non-None when skip is None (see http_get).
    assert resp.text is not None
    meta = extract(resp.text, url)

    # JS-only pages extract to near-nothing from the plain GET; escalate to a
    # headless render (unless disabled) and, if it succeeds, adopt its DOM as the
    # stored blob. --render forces a render even when the plain fetch isn't thin.
    rendered = False
    render_attempted = False
    if browser is not None and (force_render or is_thin(meta.text, thin_chars)):
        render_attempted = True
        # The render is a second hit to the domain (document + sub-resources), so
        # honor the same per-domain spacing the plain GET did.
        _rate_limit(urllib.parse.urlsplit(url).hostname or domain)
        rresp = render(url, browser)
        if rresp is not None:
            # Reconcile the render's own redirect, mirroring the plain path above.
            rfinal = web_cache.normalize_url(rresp.final_url)
            if rfinal != url:
                url = rfinal
                existing = web_cache.get(url, con=con)
            resp = rresp
            rendered = True
            assert resp.text is not None
            meta = extract(resp.text, url)
        else:
            # The render was attempted and failed (render logged why). fetches is
            # an audit of *every* fetch, so record the failed attempt — None status,
            # flagged a render — even though we fall back to the plain result below.
            web_cache.append_fetch(
                con,
                url=url,
                fetched_at=fetched_at,
                search_query=query,
                http_status=None,
                rendered=True,
            )

    # raw is guaranteed non-None for both a plain fetch (skip is None) and a render.
    assert resp.raw is not None
    # Content-address the blob so each distinct version is preserved. An unchanged
    # refetch resolves to the same file (no rewrite); a changed one writes a new
    # blob alongside the old. `changed` is relative to the version last stored.
    # (Rendered DOM is rarely byte-stable, so renders are usually 'changed'.)
    content_sha = web_cache.content_sha(resp.raw)
    changed = existing is None or existing.get("content_sha") != content_sha
    blob = web_cache.html_path(content_sha)
    if not blob.exists():
        blob.write_bytes(resp.raw)

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
        rendered=rendered,
    )
    web_cache.append_fetch(
        con,
        url=url,
        fetched_at=fetched_at,
        search_query=query,
        http_status=resp.status,
        content_sha=content_sha,
        changed=changed,
        rendered=rendered,
    )
    state = "new" if existing is None else ("changed" if changed else "unchanged")
    if rendered:
        state += ", rendered"
    title = meta.title or "(no title)"
    print(f"fetched [{resp.status}] ({state}): {url}\n    {title}")
    # Loud failure: a still-thin page is the silent-200 bug surfacing. Warn whether
    # or not we rendered, so detection is useful even under --no-render.
    if is_thin(meta.text, thin_chars):
        if rendered:
            print(f"WARNING: still thin after render: {url}", file=sys.stderr)
        elif not render_attempted:
            # A render might rescue it; suggest it only when we didn't already try.
            # --force too, since this page is now fresh and would otherwise skip.
            print(
                f"WARNING: thin content, likely JS-only: {url} "
                "(retry with --force --render after `uv run playwright install chromium`)",
                file=sys.stderr,
            )
        # else: a render was attempted and failed — render already logged why.


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
    render_group = parser.add_mutually_exclusive_group()
    render_group.add_argument(
        "--no-render",
        action="store_true",
        help="Disable the headless-render fallback (pure stdlib fetch).",
    )
    render_group.add_argument(
        "--render",
        action="store_true",
        help=(
            "Force a headless render even when the plain fetch isn't thin. Pair "
            "with --force to re-render a page that's already cached and fresh."
        ),
    )
    parser.add_argument(
        "--thin-chars",
        type=int,
        default=THIN_TEXT_CHARS,
        help=(
            "Extracted-text length below which a page is judged thin / JS-only "
            f"and a render is tried (default: {THIN_TEXT_CHARS})."
        ),
    )
    args = parser.parse_args()

    # One browser per run, threaded into fetch_one (a batch pays startup once, and
    # lazily — see LazyBrowser). None disables the fallback entirely.
    browser = None if args.no_render else LazyBrowser()
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
            try:
                fetch_one(
                    con,
                    raw_url,
                    query=query,
                    force=args.force,
                    max_age_days=args.max_age,
                    browser=browser,
                    force_render=args.render,
                    thin_chars=args.thin_chars,
                )
            except BrowserUnavailableError as exc:
                # Render setup failed (no Chromium / no playwright). It won't fix
                # itself mid-batch, so stop with the actionable message.
                print(f"ERROR: {exc}", file=sys.stderr)
                return 1
    finally:
        con.close()
        if browser is not None:
            browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

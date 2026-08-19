#!/usr/bin/env python3
"""Polite web fetcher for the web evidence cache (see docs/WebCache.md).

The CLI entry point and per-URL orchestration. Fetches a page once
(``web_http``), dispatches to its content-type handler (``content_types``) to
extract readable text and the page's own date, escalating to a headless render
for JavaScript-only pages (``web_render``), then stores the raw blob and an
upserted ``pages`` row in the SQLite cache (``web_cache``) and logs the fetch
event (and the search intent that drove it).

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
it, ``--render`` forces it, ``--thin-chars`` tunes the threshold.

Dead or blocking pages: when the live fetch fails outright (a 403/404, a dead
host), the fetcher escalates to archive.org's newest capture (``web_archive``)
and stores it keyed under the URL that was asked for, with the capture address
in ``raw_url``. On by default; ``--no-archive`` disables it.

Each document type is a handler in ``content_types`` (one file per type): it
claims its content types, recognizes itself from a magic-byte signature, and owns
how its body is decoded, extracted, stored, and warned about. PDFs (rulesheets,
flyers, press releases), for instance, are stored as raw ``.pdf`` blobs — no
charset decode, no render. A new type is a new file there, not a
branch through this module.
"""

from __future__ import annotations

import argparse
import http.client
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import NamedTuple

# Allow sibling imports whether run as a script or imported.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import web_archive
import web_cache
import web_video
from content_types import ContentHandler, ExtractedMeta, handler_for
from web_http import Resp, http_get
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


def _thin_probe(meta: ExtractedMeta) -> str | None:
    """The text thinness is judged on.

    The HTML handler fills ``body_text`` with the document body alone, and that
    is what must be measured: a JS-only page ships rich ``og:`` tags precisely
    because crawlers don't run JS, so the assembled ``text`` (metadata block +
    body) reads fat where the page is empty. ``is None`` — not truthiness — an
    empty-string body is a real, thin answer that must not fall through to the
    fat metadata block.

    A handler whose words are machine-read leaves ``text`` None and fills
    ``ocr_text`` (the image handlers), so the probe falls back to it: an image
    Vision read a full flyer off is not thin, and measuring only ``text`` would
    fire "OCR found little/no text" on every image ever fetched. Every other
    handler fills neither extra field and is measured on ``text``, unchanged.
    """
    if meta.body_text is not None:
        return meta.body_text
    if meta.text is None and meta.ocr_text is not None:
        return meta.ocr_text
    return meta.text


# --------------------------------------------------------------------------- #
# Which text a refetch stores
# --------------------------------------------------------------------------- #


def _resolve_text(
    meta: ExtractedMeta,
    existing: web_cache.PageRow | None,
    *,
    changed: bool,
    handler: ContentHandler,
) -> tuple[ExtractedMeta, str | None]:
    """Decide whether this fetch's extraction replaces the stored text.

    A fetch normally stores what it just extracted. Two cases must not, both of
    them a *worse* text silently replacing a better one on bytes that didn't
    change — and logged as an innocuous ``changed=0``:

    **This run produced no result.** Image OCR needs macOS Vision, and the cache
    is shared through R2, so a page OCR'd on a Mac can be refetched from a host
    that can't OCR — or from one where Vision times out. Either way
    ``meta.unavailable`` says we learned nothing this time, which is not "this
    document has no text"; blanking the row would destroy evidence — and its FTS
    entry — that the byte-identical blob still supports.

    **The stored text is a human transcription.** ``text_source='manual'`` is the
    one rung where a person is answerable for the words, and it exists precisely
    because machine extraction was unavailable or too poor to quote. Re-extracting
    identical bytes would trade a reviewed transcription for OCR output. Changing
    a transcription stays a deliberate act through ``web_import.py``, which is the
    single audited path for it — never a side effect of a routine refetch.

    Both hinge on the bytes being unchanged, because then the earlier text still
    describes what we store. When the bytes *did* change, the new extraction wins:
    a transcription of the old version would misdescribe the new one. A superseded
    transcription is called out loudly, since nobody would otherwise notice a
    person's work going stale.
    """
    if existing is None:
        return meta, handler.text_source

    keep_manual = bool(existing["text_source"] == "manual" and existing["text"])
    if not changed and (keep_manual or meta.unavailable):
        # The stored text is kept either way; the two cases differ only on the
        # metadata around it. A human owns a manual row wholesale — `--title` and
        # `--date` are part of what they set, so a PDF imported with a corrected
        # title must not have its own "Untitled-1" put back (and where the
        # importer supplied nothing, the stored values came from this same
        # extraction anyway, since identical bytes extract identically). The
        # unavailable case extracted nothing at all, so it takes the stored values
        # only where it has none of its own.
        return (
            meta._replace(
                title=existing["title"]
                if keep_manual
                else meta.title or existing["title"],
                last_updated=(
                    existing["last_updated"]
                    if keep_manual
                    else meta.last_updated or existing["last_updated"]
                ),
                text=existing["text"],
                # Carry the stored OCR too, or an unavailable image refetch
                # would read as thin (both meta fields empty) while the row it
                # keeps holds a full reading. A fresh reading, when this run
                # made one, still wins — the bytes are identical, so it can
                # only be the same or newer Vision's.
                ocr_text=meta.ocr_text or existing.get("ocr_text"),
                unavailable=False,
                # The fresh extraction's body was just discarded with the rest
                # of it; None makes the thin check measure the stored text —
                # what the row actually holds — instead of text it doesn't.
                body_text=None,
            ),
            existing["text_source"],
        )
    if changed and keep_manual:
        print(
            f"WARNING: source changed, so its reviewed transcription no longer "
            f"describes it: {existing['url']} (re-review the new version "
            f"and re-import if the transcription still applies)",
            file=sys.stderr,
        )
    return meta, handler.text_source


# --------------------------------------------------------------------------- #
# Fetch one page
# --------------------------------------------------------------------------- #


def _transport_failure_reason(exc: Exception) -> str:
    """Why a fetch never got a response, and what to do about it.

    A TLS failure reads like an outage but the page opens in a browser, so the
    way forward is a hand-saved import, not a retry. The wrapped error already
    names the failed check; this only has to name the way out.
    """
    reason = getattr(exc, "reason", None)
    if isinstance(reason, ssl.SSLCertVerificationError):
        return (
            f"{exc} — its certificate can't be verified. A browser may still "
            f"open it: save the file and add it with web_import.py"
        )
    return str(exc)


def _too_large_reason(resp: Resp) -> str:
    """Why a response was refused for size, and what to do about it.

    Says whose cap it was, since limits are per content type and a bare number
    doesn't identify which one applied, and names the attribute that changes it
    — an operator meeting this needs to decide whether the document belongs in
    the cache, not to go looking for where the number lives.
    """
    cap_mb = (resp.limit or 0) / (1024 * 1024)
    size = (
        f"{resp.declared_size / (1024 * 1024):.1f}MB response"
        if resp.declared_size is not None
        else "response"
    )
    return (
        f"{size} over the {cap_mb:g}MB cap for {resp.content_type} — raise "
        f"max_response_bytes on its handler in content_types/ to cache it"
    )


def _web_url(raw_url: str) -> tuple[web_cache.NormalizedUrl, str]:
    """Normalize a URL and its host, refusing anything that isn't a web address.

    Raises ``ValueError`` with the reason, so a garbage ``--from-file`` row is a
    per-URL skip rather than an aborted batch: ``normalize_url`` rejects it (a
    bad port like ':abc', an invalid IPv6 literal), it isn't http(s) with a host
    (``ftp://``, ``mailto:``), or its authority holds characters ``http.client``
    refuses at connect time.

    The character test reads the authority *decoded*, and whole, because that is
    what the transport sees: ``urllib.request`` unquotes before connecting, so
    ``https://not%20a%20url/`` is the same dead address wearing an escape, and
    userinfo travels with the host rather than inside ``hostname``.

    Shared, so that what the fetcher won't fetch, ``--doc-class`` won't mint a
    document for either.
    """
    url = web_cache.normalize_url(raw_url)
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise ValueError(f"not a web scheme: {parts.scheme or '(none)'}")
    host = parts.hostname
    if not host:
        raise ValueError("no host")
    authority = urllib.parse.unquote(parts.netloc)
    if any(c.isspace() or ord(c) < 0x21 or ord(c) == 0x7F for c in authority):
        raise ValueError(f"unusable host: {parts.netloc!r}")
    return url, host


def _store_result(
    con: sqlite3.Connection,
    *,
    url: web_cache.NormalizedUrl,
    raw_url: str,
    resp: Resp,
    handler: ContentHandler,
    meta: ExtractedMeta,
    existing: web_cache.PageRow | None,
    query: str | None,
    fetched_at: str,
    rendered: bool,
    render_attempted: bool,
    thin_chars: int,
    note: str | None = None,
) -> None:
    """Store one successful fetch: blob, page row, audit row, stdout report.

    The shared tail of the live path and the archive fallback. Content-addresses
    the blob so each distinct version of a page is preserved: an unchanged
    refetch resolves to the same file (no rewrite), a changed one writes a new
    blob alongside the old — ``changed`` is relative to the version last stored.
    (Rendered DOM is rarely byte-stable, so renders are usually 'changed'.)
    ``note`` joins the printed state so an archive row announces itself.
    """
    assert resp.raw is not None
    content_sha = web_cache.content_sha(resp.raw)
    changed = existing is None or existing.get("content_sha") != content_sha
    # The blob keeps its type's extension (a PDF as .pdf) so it re-opens in the
    # right viewer on verify rather than being mislabeled .html.
    blob = web_cache.blob_path(content_sha, ext=handler.extension)
    if not blob.exists():
        blob.write_bytes(resp.raw)

    meta, text_source = _resolve_text(meta, existing, changed=changed, handler=handler)

    web_cache.upsert_page(
        con,
        url=url,
        raw_url=raw_url,
        content_sha=content_sha,
        fetched_at=fetched_at,
        last_updated=meta.last_updated,
        title=meta.title,
        http_status=resp.status,
        content_type=resp.content_type,
        text=meta.text,
        # None for most types: a fetch never OCRs a PDF (that is web_pdfocr's
        # separate pass), and upsert_page keeps or clears the stored tier by
        # whether the bytes changed. The image handlers supply a value.
        ocr_text=meta.ocr_text,
        rendered=rendered,
        text_source=text_source,
        imported=False,
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
    if note:
        state += f", {note}"
    title = meta.title or "(no title)"
    print(f"fetched [{resp.status}] ({state}): {url}\n    {title}")
    # Loud failure: a still-thin page is the silent-200 bug surfacing. The handler
    # phrases its type's warning (a scanned PDF vs a JS-only page) given whether a
    # render was tried, and returns None to stay quiet (a render attempted+failed —
    # render already logged why).
    if is_thin(_thin_probe(meta), thin_chars):
        warning = handler.thin_warning(
            url, rendered=rendered, render_attempted=render_attempted
        )
        if warning is not None:
            print(warning, file=sys.stderr)


def _archive_fallback(
    con: sqlite3.Connection,
    url: web_cache.NormalizedUrl,
    *,
    existing: web_cache.PageRow | None,
    query: str | None,
    thin_chars: int,
    bounded: bool = True,
) -> None:
    """Escalate a failed live fetch to archive.org's newest capture.

    The same shape as the thin-body/render escalation, for a different failure
    mode: the escalation order is live fetch → archive fallback → human import.
    The failed live attempt was already logged by the caller — the audit keeps
    it, since a dead live page is a finding, not an obstacle.

    The row keys on the URL the session asked for; the capture address goes in
    ``raw_url`` (as fetched, pre-normalization — exactly its meaning), which is
    what makes the fallback invisible to readers (``have`` answers yes, search
    attributes to the origin site) while keeping the provenance derivable, the
    capture timestamp included. ``web_archive`` already said why on stderr when
    there is nothing to store — and it says it loudly enough to keep "the
    archive holds nothing" apart from "the archive refused to answer", because
    only the former is a negative anyone may record.

    A capture never *downgrades* the cache: when the row already holds
    evidence at least as new (a live fetch from after the archive's newest
    capture, or that very capture itself), the fallback reports the capture
    and keeps what is stored — otherwise a page cached live in August whose
    site then dies would have its text silently replaced by an older capture
    on the next routine refetch (and quotes already cited against it could
    stop verifying), and a permanently dead page would re-download its own
    byte-identical capture every freshness window.

    The stored ``http_status`` is the capture fetch's own, real 200 — unlike an
    import (status NULL, no request was made), a request was made and answered.
    A SQL consumer that means "live successes only" must also exclude
    ``raw_url LIKE 'http%://web.archive.org/web/%'``, which is the row's whole
    archive marking (no dedicated column, by design).

    Known gap, owned by Phase 3's identity adapter: CDX is asked about the
    *normalized* URL, and ``normalize_url`` mangles legacy positional IPDB
    spellings (``machine.cgi?1000`` → ``?1000=``), so such a URL can print "no
    archive capture" for a page the archive does hold under its original
    spelling. Nothing records that negative automatically — a hunt stays a
    human/session judgment — so the cost is a missed fallback, not a poisoned
    corpus.
    """
    # A URL carrying credentials must not be sent to a third party — and an
    # authenticated page can't be in the public archive anyway, so the lookup
    # would leak the secret for nothing.
    if urllib.parse.urlsplit(url).username:
        print(
            f"archive fallback skipped (URL carries credentials, not sent to "
            f"archive.org): {url}",
            file=sys.stderr,
        )
        return
    # The bound an acceptable capture must beat: what the row already holds —
    # a prior capture's own timestamp, else the live/import fetch time.
    # ``bounded=False`` is --from-archive's explicit operator judgment that the
    # stored row is the thing to replace (a soft-404 stub is *newer* than every
    # capture, so the guard would protect exactly the wrong row).
    newer_than: str | None = None
    if bounded and existing is not None:
        newer_than = web_cache.archive_capture_timestamp(existing)
        if newer_than is None and existing["last_fetched_at"]:
            newer_than = _parse_iso(existing["last_fetched_at"]).strftime(
                "%Y%m%d%H%M%S"
            )
    hit = web_archive.try_capture(url, newer_than=newer_than)
    if hit is None:
        return
    resp = hit.resp
    fetched_at = web_cache.now_iso()
    if resp.skip:
        why = (
            f"unsupported content-type {resp.content_type}"
            if resp.skip == "content-type"
            else _too_large_reason(resp)
        )
        print(f"skip archive capture ({why}): {url}", file=sys.stderr)
        web_cache.append_fetch(
            con,
            url=url,
            fetched_at=fetched_at,
            search_query=query,
            http_status=resp.status,
        )
        return
    handler = handler_for(resp.content_type)
    assert handler is not None
    assert resp.raw is not None
    # Extraction sees the *original* URL: the capture is that page's bytes, and
    # its relative links/dates belong to the origin site, not to web.archive.org.
    meta = handler.extract(resp.raw, resp.text, url)
    date = web_archive.capture_date(hit.timestamp)
    why = "live fetch failed" if bounded else "archive requested (--from-archive)"
    print(f"{why}; using archive capture from {date}: {url}")
    _store_result(
        con,
        url=url,
        raw_url=hit.capture_url,
        resp=resp,
        handler=handler,
        meta=meta,
        existing=existing,
        query=query,
        fetched_at=fetched_at,
        rendered=False,
        render_attempted=False,
        thin_chars=thin_chars,
        note=f"archive {date}",
    )


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
    archive: bool = True,
    from_archive: bool = False,
) -> None:
    try:
        url, host = _web_url(raw_url)
    except ValueError as exc:
        print(f"skip (malformed URL): {raw_url} ({exc})", file=sys.stderr)
        return
    domain = host

    # A video URL is a different transport entirely: the watch page is a JS
    # shell and the evidence is spoken. Route it to the yt-dlp caption path,
    # keyed on the canonical watch URL (youtu.be/shorts/live shapes dedup).
    video_url = web_video.canonical_video_url(url)
    if video_url is not None:
        _fetch_video_one(
            con, raw_url, video_url, query=query, force=force, max_age_days=max_age_days
        )
        return

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

    # --from-archive: an explicit operator judgment that the live answer is
    # wrong — a host serving soft 404s (HTTP 200, "page not found" body)
    # never trips the automatic escalation, and its stub, once stored, is
    # *newer* than every capture, so the downgrade guard would defend it.
    # Skips the live fetch (no request to the origin at all) and stores the
    # newest capture unbounded, keyed as ever under the requested URL.
    if from_archive:
        _archive_fallback(
            con,
            url,
            existing=existing,
            query=query,
            thin_chars=thin_chars,
            bounded=False,
        )
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
        # Any HTTP failure escalates: a 404 is the classic archive candidate, a
        # 403/503 is how a blocking host (ipdb.org serves both) says no to the
        # fetcher. Escalating on a transient 5xx costs one CDX lookup and
        # self-heals — live is always tried first, so the next post-max-age
        # fetch restores the live page.
        if archive:
            _archive_fallback(
                con, url, existing=existing, query=query, thin_chars=thin_chars
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
        print(f"FAILED: {url} ({_transport_failure_reason(exc)})", file=sys.stderr)
        web_cache.append_fetch(
            con, url=url, fetched_at=fetched_at, search_query=query, http_status=None
        )
        # A dead host is where the archive earns its keep: much of what
        # documented pinball lives on sites that no longer resolve.
        if archive:
            _archive_fallback(
                con, url, existing=existing, query=query, thin_chars=thin_chars
            )
        return

    if resp.skip:
        why = (
            f"unsupported content-type {resp.content_type}"
            if resp.skip == "content-type"
            else _too_large_reason(resp)
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

    # Dispatch extraction through the content-type handler. http_get only returns
    # skip=None for an extractable type, so a handler is always found here; it pulls
    # title/text/date from the raw bytes and/or decoded text, whichever its type
    # uses (a binary type like a PDF carries bytes and no decoded text). raw is
    # non-None whenever skip is None.
    handler = handler_for(resp.content_type)
    assert handler is not None
    assert resp.raw is not None
    meta = handler.extract(resp.raw, resp.text, url)

    # JS-only pages extract to near-nothing from the plain GET; escalate to a
    # headless render (unless disabled) and, if it succeeds, adopt its DOM as the
    # stored blob. --render forces a render even when the plain fetch isn't thin.
    # Only a render-eligible type escalates — a PDF reads as thin when scanned too,
    # but a browser can't extract its text either (that needs OCR, out of scope).
    rendered = False
    render_attempted = False
    if (
        browser is not None
        and handler.renderable
        and (force_render or is_thin(_thin_probe(meta), thin_chars))
    ):
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
            assert resp.raw is not None
            meta = handler.extract(resp.raw, resp.text, url)
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
    _store_result(
        con,
        url=url,
        raw_url=raw_url,
        resp=resp,
        handler=handler,
        meta=meta,
        existing=existing,
        query=query,
        fetched_at=fetched_at,
        rendered=rendered,
        render_attempted=render_attempted,
        thin_chars=thin_chars,
    )


# --------------------------------------------------------------------------- #
# Fetch one video (caption track via web_video)
# --------------------------------------------------------------------------- #


def _fetch_video_one(
    con: sqlite3.Connection,
    raw_url: str,
    url: str,
    *,
    query: str | None,
    force: bool,
    max_age_days: int,
) -> None:
    """Fetch one video's caption track into the cache (transport: web_video).

    Mirrors fetch_one's skeleton on the canonical watch URL: freshness skip,
    rate limit, content-addressed blob (the raw ``.vtt``), pages + fetches
    rows. A video with no captions at all logs a loud warning and an audit row
    but no page — there is no transcript to quote, and the video description
    often links the written source to cite instead.
    """
    existing = web_cache.get(url, con=con)
    fresh_row = existing or web_cache.get_by_raw_url(raw_url, con=con)
    if fresh_row and not force:
        age_days = (datetime.now(UTC) - _parse_iso(fresh_row["last_fetched_at"])).days
        if age_days <= max_age_days:
            print(f"skip (fresh, {age_days}d): {fresh_row['url']}")
            return

    _rate_limit(urllib.parse.urlsplit(url).hostname or "www.youtube.com")
    fetched_at = web_cache.now_iso()
    video = web_video.fetch_video(url)
    if video is None:
        # web_video logged why. fetches is an audit of *every* fetch.
        print(f"FAILED: {url} (video extraction)", file=sys.stderr)
        web_cache.append_fetch(
            con, url=url, fetched_at=fetched_at, search_query=query, http_status=None
        )
        return
    if video.vtt is None:
        print(
            f"WARNING: no captions: {url} — no transcript to quote (livestream "
            f"archives often have none; check the video description for a "
            f"written source)",
            file=sys.stderr,
        )
        web_cache.append_fetch(
            con, url=url, fetched_at=fetched_at, search_query=query, http_status=200
        )
        return

    handler = handler_for("text/vtt")
    assert handler is not None
    meta = handler.extract(video.vtt, handler.decode(video.vtt, None), url)
    content_sha = web_cache.content_sha(video.vtt)
    changed = existing is None or existing.get("content_sha") != content_sha
    blob = web_cache.blob_path(content_sha, ext=handler.extension)
    if not blob.exists():
        blob.write_bytes(video.vtt)

    # Title and date come from the video's metadata, not the VTT: the caption
    # file carries no title, and the video's publish date is the evidence date.
    # They go through _resolve_text with the transcript so this path obeys the
    # same rule as every other fetch — a video with no captions is exactly the
    # case someone transcribes by hand and imports under the watch URL, and
    # machine text must not overwrite that on unchanged bytes.
    meta, text_source = _resolve_text(
        meta._replace(title=video.title, last_updated=video.upload_date),
        existing,
        changed=changed,
        handler=handler,
    )
    web_cache.upsert_page(
        con,
        url=url,
        raw_url=raw_url,
        content_sha=content_sha,
        fetched_at=fetched_at,
        last_updated=meta.last_updated,
        title=meta.title,
        http_status=200,
        content_type="text/vtt",
        text=meta.text,
        ocr_text=meta.ocr_text,
        rendered=False,
        text_source=text_source,
        imported=False,
    )
    web_cache.append_fetch(
        con,
        url=url,
        fetched_at=fetched_at,
        search_query=query,
        http_status=200,
        content_sha=content_sha,
        changed=changed,
    )
    state = "new" if existing is None else ("changed" if changed else "unchanged")
    track = video.caption_note or "captions"
    title = video.title or "(no title)"
    print(f"fetched [{track}] ({state}): {url}\n    {title}")
    if is_thin(_thin_probe(meta), THIN_TEXT_CHARS):
        warning = handler.thin_warning(url, rendered=False, render_attempted=False)
        if warning is not None:
            print(warning, file=sys.stderr)


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


def _apply_document_metadata(
    con: sqlite3.Connection, raw_url: str, args: argparse.Namespace
) -> None:
    """Apply the --doc-class / --subject flags to the URL's document.

    Sugar over the shared registration library. Applies even to a
    freshness-skipped URL (the metadata is about the work, not the fetch);
    refusals are per-URL warnings so one bad flag doesn't kill a batch.

    A URL we hold nothing for is annotated anyway, by registering it as a
    document the library knows about but hasn't acquired — the state the trove
    seed leaves its documents in. These flags are a judgment about what the work
    *is*, and that stays true whether the fetch bounced off an unsupported
    content type, a 404, or a dead host.
    """
    if not (args.doc_class or args.subject_pk is not None):
        return
    try:
        url, _host = _web_url(raw_url)
    except ValueError as exc:
        # Registering it would mint a document nothing can ever be captured for,
        # and merge is the only path that deletes one.
        print(f"  cannot annotate {raw_url}: {exc}", file=sys.stderr)
        return
    # Collapse a video URL to the watch URL the fetch path keys its row on, or
    # the same video cited as youtu.be/ID and as watch?v=ID becomes two
    # documents — one of them holding the classification and no capture.
    canonical = web_video.canonical_video_url(url) or url
    rec = web_cache.get(canonical, con) or web_cache.get_by_raw_url(raw_url, con)
    if rec is not None:
        doc_id = web_cache.resolve_document(con, rec["url"])
        if doc_id is None:  # pragma: no cover — upsert_page registers; belt only
            print(f"  no document for: {rec['url']}", file=sys.stderr)
            return
    else:
        # A document registered before it was ever captured may already own the
        # address as cited, and minting a rival under the canonical form is the
        # same split. ``ensure_`` covers the mirror case, resolving an owned
        # canonical rather than minting on it.
        doc_id = web_cache.resolve_document(
            con, url
        ) or web_cache.ensure_document_for_url(con, canonical)
        print(f"  not acquired; annotating document {doc_id}: {url}")
    for document_class in args.doc_class:
        try:
            web_cache.add_document_class(con, doc_id, document_class, source="manual")
        except sqlite3.IntegrityError:
            print(
                f"  refused class {document_class!r} (not in the vocabulary); "
                "see web_docs.py classes",
                file=sys.stderr,
            )
    if args.subject_pk is not None:
        try:
            web_cache.attach_document_subject(
                con,
                doc_id,
                args.subject_scope,
                flipcommons_pk=args.subject_pk,
                label=args.subject_label,
            )
        except (sqlite3.IntegrityError, ValueError) as exc:
            print(f"  refused subject: {exc}", file=sys.stderr)
    con.commit()


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
    archive_group = parser.add_mutually_exclusive_group()
    archive_group.add_argument(
        "--no-archive",
        action="store_true",
        help=(
            "Disable the archive.org fallback (on by default: a failed live "
            "fetch escalates to the newest Wayback capture)."
        ),
    )
    archive_group.add_argument(
        "--from-archive",
        action="store_true",
        help=(
            "Skip the live fetch and store the newest Wayback capture — for a "
            "host that answers a dead page with a live 200 stub (a soft 404), "
            "which the automatic fallback can't detect. Pair with --force if "
            "the URL is cached and fresh."
        ),
    )
    doc_group = parser.add_argument_group(
        "document metadata",
        "Sugar over web_docs.py, applied to every URL this run touches "
        "(fetched or already cached). The class must exist in the vocabulary.",
    )
    doc_group.add_argument(
        "--doc-class",
        action="append",
        default=[],
        metavar="CLASS",
        help="record a class judgment (repeatable; source=manual)",
    )
    doc_group.add_argument(
        "--subject-pk",
        type=int,
        help="attach a subject by Flipcommons PK (needs --subject-label)",
    )
    doc_group.add_argument(
        "--subject-label", help="the subject's searchable name snapshot"
    )
    doc_group.add_argument(
        "--subject-scope",
        choices=["model", "corporate_entity"],
        default="model",
        help="which kind of record the subject is (default %(default)s)",
    )
    args = parser.parse_args()
    if args.subject_label and args.subject_pk is None:
        parser.error(
            "--subject-label needs --subject-pk (a label alone is not an identity)"
        )
    if args.subject_pk is not None and not args.subject_label:
        parser.error(
            "--subject-pk needs --subject-label (a PK-only subject is unsearchable)"
        )

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
                    archive=not args.no_archive,
                    from_archive=args.from_archive,
                )
            except BrowserUnavailableError as exc:
                # Render setup failed (no Chromium / no playwright). It won't fix
                # itself mid-batch, so stop with the actionable message.
                print(f"ERROR: {exc}", file=sys.stderr)
                return 1
            _apply_document_metadata(con, raw_url, args)
    finally:
        con.close()
        if browser is not None:
            browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

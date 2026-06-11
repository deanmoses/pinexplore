#!/usr/bin/env python3
"""Web cache: schema, URL normalization, upsert, and query helpers.

This is the library behind the web-scrape cache (see docs/WebCache.md). It owns
the SQLite system-of-record at ``ingest_sources/web/cache.sqlite`` plus the raw
HTML blobs at ``ingest_sources/web/html/<sha>.html``. The fetcher
(``web_fetch.py``) writes through it; patch authors read through it.

Stdlib only (sqlite3, hashlib, urllib.parse, re). The SQLite ``fts5`` extension
ships with the standard CPython build.

Layout (all under ingest_sources/web/, R2-backed and gitignored):
    cache.sqlite        pages + fetches + pages_fts (FTS5)
    html/<sha>.html     raw page blobs, content-addressed (sha = sha256(raw
                        bytes)) so every distinct version of a page is preserved

Query helpers:
    search(term)        FTS5 BM25-ranked pages (url, title, snippet)
    quote(url, needle)  sentence(s) in a page's text containing a needle
    get(url)            the full page record
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict, cast

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
WEB_DIR = REPO_ROOT / "ingest_sources" / "web"
DB_PATH = WEB_DIR / "cache.sqlite"
HTML_DIR = WEB_DIR / "html"


# A URL canonicalized by ``normalize_url`` — the ``pages`` primary key and what
# dedup/lookup key on. Distinct from a RawUrl (as-requested, pre-normalization);
# the module's dedup + redirect-skip correctness hinges on not confusing the two.
type NormalizedUrl = str
type RawUrl = str


class PageRow(TypedDict):
    """A full ``pages`` row (SELECT *), mirroring the schema below."""

    url: NormalizedUrl
    raw_url: RawUrl | None
    content_sha: str
    first_fetched_at: str
    last_fetched_at: str
    last_updated: str | None
    title: str | None
    http_status: int | None
    content_type: str | None
    html_file: str
    text: str | None
    archive_url: str | None
    archived_at: str | None


class SearchHit(TypedDict):
    """One FTS5 search result row from ``search()``."""

    url: NormalizedUrl
    title: str | None
    last_updated: str | None
    archive_url: str | None
    snippet: str


# Query params that are tracking noise, never content-bearing. Stripped on
# normalization so the same page reached via different campaigns dedups.
# Bare `ref` is deliberately NOT stripped: some sites use it as a content-bearing
# param (branch refs, content variants), and over-stripping silently collapses
# distinct pages to one row. `ref_src`/`ref_url` are unambiguous referrer tracking.
_TRACKING_PARAMS = re.compile(
    r"^(utm_[a-z_]+|fbclid|gclid|gbraid|wbraid|msclkid|mc_eid|mc_cid|"
    r"igshid|ref_src|ref_url|spm|yclid|_ga|_gl)$",
    re.IGNORECASE,
)

_DEFAULT_PORTS = {"http": "80", "https": "443"}


# --------------------------------------------------------------------------- #
# URL normalization + identity
# --------------------------------------------------------------------------- #


def normalize_url(raw_url: str) -> NormalizedUrl:
    """Canonicalize a URL for dedup and as the ``pages`` primary key.

    Lowercases scheme + host, drops default ports, strips tracking params and
    the fragment, and removes a trailing slash on non-root paths. Preserves the
    rest of the path and any content-bearing query params (sorted for
    stability). Does not touch percent-encoding of the path.
    """
    raw = raw_url.strip()
    parts = urllib.parse.urlsplit(raw)
    # Scheme-less input ("example.com/foo", "www.site.com") parses with the host
    # stuck in `path` and no netloc — assume https and re-parse so it canonicalizes
    # to a real URL (and dedups with the explicit https form).
    if not parts.scheme and not parts.netloc:
        parts = urllib.parse.urlsplit("https://" + raw)

    scheme = parts.scheme.lower() or "https"

    host = (parts.hostname or "").lower()
    port = parts.port
    netloc = host
    if port is not None and _DEFAULT_PORTS.get(scheme) != str(port):
        netloc = f"{host}:{port}"
    # Preserve userinfo if present (rare for our sources, but don't silently drop it).
    if parts.username:
        cred = parts.username
        if parts.password:
            cred += f":{parts.password}"
        netloc = f"{cred}@{netloc}"

    path = parts.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")

    kept = [
        (k, v)
        for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
        if not _TRACKING_PARAMS.match(k)
    ]
    kept.sort()
    query = urllib.parse.urlencode(kept)

    return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))


def content_sha(raw: bytes) -> str:
    """sha256 of the raw page bytes; the html blob filename stem.

    Content-addressed so each distinct version of a page is preserved: an
    unchanged refetch resolves to the same blob (no rewrite), a changed one
    writes a new file alongside the old. The ``pages`` row points at the current
    version; prior versions stay on disk and in the ``fetches`` log.
    """
    return hashlib.sha256(raw).hexdigest()


def html_path(sha: str) -> Path:
    """Absolute path to a page's raw HTML blob."""
    return HTML_DIR / f"{sha}.html"


def html_rel(sha: str) -> str:
    """The ``html_file`` value stored in the DB (relative, posix)."""
    return f"html/{sha}.html"


def now_iso() -> str:
    """Current time as ISO8601 UTC, second precision, with a 'Z' suffix."""
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# --------------------------------------------------------------------------- #
# Connection + schema
# --------------------------------------------------------------------------- #


def connect(read_only: bool = False) -> sqlite3.Connection:
    """Open the cache DB. Creates the parent dirs on a writable open."""
    if read_only:
        if not DB_PATH.exists():
            raise FileNotFoundError(f"web cache not found: {DB_PATH}")
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    else:
        HTML_DIR.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(DB_PATH)
        # DELETE (rollback-journal) mode, not WAL: this is single-writer batch
        # tooling, so WAL's concurrent-reader benefit is moot, and it leaves the
        # DB self-contained — no -wal/-shm sidecars to (a) get uploaded to R2 by
        # `make push` or (b) leave committed rows stranded outside cache.sqlite
        # where DuckDB's READ_ONLY ATTACH can't see them. Setting DELETE on a file
        # previously in WAL checkpoints and converts it back.
        con.execute("PRAGMA journal_mode=DELETE")
    con.row_factory = sqlite3.Row
    return con


_SCHEMA = """
CREATE TABLE IF NOT EXISTS pages (
  url              TEXT PRIMARY KEY,   -- normalized
  raw_url          TEXT,               -- as fetched, pre-normalization
  content_sha      TEXT NOT NULL,      -- sha256(raw bytes) of the current version
  first_fetched_at TEXT NOT NULL,      -- ISO8601 UTC
  last_fetched_at  TEXT NOT NULL,
  last_updated     TEXT,               -- page's own date if it states one, else null
  title            TEXT,
  http_status      INTEGER,
  content_type     TEXT,
  html_file        TEXT NOT NULL,      -- 'html/<content_sha>.html' (current version)
  text             TEXT,               -- extracted readable text (current version)
  archive_url      TEXT,               -- Wayback permalink (best-effort, nullable)
  archived_at      TEXT                -- when we captured/confirmed the snapshot
);

CREATE TABLE IF NOT EXISTS fetches (   -- append-only audit + version history
  id           INTEGER PRIMARY KEY,
  url          TEXT NOT NULL,
  fetched_at   TEXT NOT NULL,
  search_query TEXT,                   -- the intent that drove this fetch
  http_status  INTEGER,
  content_sha  TEXT,                   -- the version this fetch saw (blob stem)
  changed      INTEGER                 -- 1 if content differed from the prior fetch
);

CREATE INDEX IF NOT EXISTS fetches_url ON fetches(url);

CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts USING fts5(
  url, title, text, content='pages', content_rowid='rowid'
);

-- Keep the FTS index in sync with pages via triggers (external-content pattern).
CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
  INSERT INTO pages_fts(rowid, url, title, text)
  VALUES (new.rowid, new.url, new.title, new.text);
END;
CREATE TRIGGER IF NOT EXISTS pages_ad AFTER DELETE ON pages BEGIN
  INSERT INTO pages_fts(pages_fts, rowid, url, title, text)
  VALUES ('delete', old.rowid, old.url, old.title, old.text);
END;
CREATE TRIGGER IF NOT EXISTS pages_au AFTER UPDATE ON pages BEGIN
  INSERT INTO pages_fts(pages_fts, rowid, url, title, text)
  VALUES ('delete', old.rowid, old.url, old.title, old.text);
  INSERT INTO pages_fts(rowid, url, title, text)
  VALUES (new.rowid, new.url, new.title, new.text);
END;
"""


def init_schema(con: sqlite3.Connection) -> None:
    """Create tables, the FTS5 index, and sync triggers if absent (idempotent).

    There is no in-place migration: this only CREATEs missing objects, it never
    ALTERs an existing table. While the cache is unshipped a schema change is
    free — just delete ingest_sources/web/ and re-fetch. But this SQLite is the
    system-of-record (not a blow-away-safe artifact like the DuckDB tables), so
    once it holds shipped/accumulated evidence a schema change must be a real
    ALTER-based migration here, not a rebuild.
    """
    con.executescript(_SCHEMA)
    con.commit()


# --------------------------------------------------------------------------- #
# Writes
# --------------------------------------------------------------------------- #


def upsert_page(
    con: sqlite3.Connection,
    *,
    url: NormalizedUrl,
    raw_url: RawUrl,
    content_sha: str,
    html_file: str,
    fetched_at: str,
    last_updated: str | None = None,
    title: str | None = None,
    http_status: int | None = None,
    content_type: str | None = None,
    text: str | None = None,
    archive_url: str | None = None,
    archived_at: str | None = None,
) -> None:
    """Insert or refresh a page row, keyed on the normalized URL.

    On conflict, points the row at the freshly-fetched version
    (``content_sha``/``html_file``/``text``/...) and bumps ``last_fetched_at``
    while preserving ``first_fetched_at``. A null ``archive_url``/``archived_at``
    does not clobber an existing value — so a refetch keeps the prior archive
    (the caller decides whether the new content invalidates it; see
    ``clear_archive``), and ``--archive-missing`` can fill a gap.
    """
    con.execute(
        """
        INSERT INTO pages (
          url, raw_url, content_sha, first_fetched_at, last_fetched_at,
          last_updated, title, http_status, content_type, html_file, text,
          archive_url, archived_at
        ) VALUES (
          :url, :raw_url, :content_sha, :fetched_at, :fetched_at,
          :last_updated, :title, :http_status, :content_type, :html_file,
          :text, :archive_url, :archived_at
        )
        ON CONFLICT(url) DO UPDATE SET
          raw_url       = excluded.raw_url,
          content_sha   = excluded.content_sha,
          last_fetched_at = excluded.last_fetched_at,
          last_updated  = excluded.last_updated,
          title         = excluded.title,
          http_status   = excluded.http_status,
          content_type  = excluded.content_type,
          html_file     = excluded.html_file,
          text          = excluded.text,
          archive_url   = COALESCE(excluded.archive_url, pages.archive_url),
          archived_at   = COALESCE(excluded.archived_at, pages.archived_at)
        """,
        {
            "url": url,
            "raw_url": raw_url,
            "content_sha": content_sha,
            "fetched_at": fetched_at,
            "last_updated": last_updated,
            "title": title,
            "http_status": http_status,
            "content_type": content_type,
            "html_file": html_file,
            "text": text,
            "archive_url": archive_url,
            "archived_at": archived_at,
        },
    )
    con.commit()


def append_fetch(
    con: sqlite3.Connection,
    *,
    url: NormalizedUrl,
    fetched_at: str,
    search_query: str | None,
    http_status: int | None,
    content_sha: str | None = None,
    changed: bool | None = None,
) -> None:
    """Append one row to the fetch audit log + version history."""
    con.execute(
        "INSERT INTO fetches (url, fetched_at, search_query, http_status, "
        "content_sha, changed) VALUES (?, ?, ?, ?, ?, ?)",
        (
            url,
            fetched_at,
            search_query,
            http_status,
            content_sha,
            None if changed is None else int(changed),
        ),
    )
    con.commit()


def set_archive(
    con: sqlite3.Connection, *, url: NormalizedUrl, archive_url: str, archived_at: str
) -> None:
    """Record a Wayback permalink on an existing page (backfill path)."""
    con.execute(
        "UPDATE pages SET archive_url = ?, archived_at = ? WHERE url = ?",
        (archive_url, archived_at, url),
    )
    con.commit()


def clear_archive(con: sqlite3.Connection, *, url: NormalizedUrl) -> None:
    """Drop a page's Wayback permalink (e.g. when refetched content changed, so
    the old snapshot no longer matches the stored text). Leaves it null for a
    later re-archive / ``--archive-missing`` pass."""
    con.execute(
        "UPDATE pages SET archive_url = NULL, archived_at = NULL WHERE url = ?",
        (url,),
    )
    con.commit()


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #


def get(url: str, con: sqlite3.Connection | None = None) -> PageRow | None:
    """Full page record for a URL (normalized on the way in), or None."""
    own = con is None
    con = con or connect(read_only=True)
    try:
        row = con.execute(
            "SELECT * FROM pages WHERE url = ?", (normalize_url(url),)
        ).fetchone()
        return cast("PageRow", dict(row)) if row else None
    finally:
        if own:
            con.close()


def get_by_raw_url(
    raw_url: RawUrl, con: sqlite3.Connection | None = None
) -> PageRow | None:
    """Most-recently-fetched page whose ``raw_url`` (as-requested, pre-redirect)
    matches. Lets the fetcher freshness-skip a URL that 301s to a canonical
    address — the row is keyed by the post-redirect URL, but raw_url holds what
    was requested. Exact-string match on the requested form (not normalized)."""
    own = con is None
    con = con or connect(read_only=True)
    try:
        row = con.execute(
            "SELECT * FROM pages WHERE raw_url = ? "
            "ORDER BY last_fetched_at DESC LIMIT 1",
            (raw_url,),
        ).fetchone()
        return cast("PageRow", dict(row)) if row else None
    finally:
        if own:
            con.close()


def _fts_query(term: str) -> str:
    """Turn a plain search term into an FTS5 AND-of-quoted-tokens expression.

    Each whitespace token is wrapped in double quotes (a literal phrase) so FTS5
    operator characters in user input can't break the query; multiple tokens AND
    together. Note this re-quotes every token, so it does not preserve a
    hand-written FTS expression — pass plain search words.
    """
    tokens = term.split()
    return " ".join('"' + t.replace('"', '""') + '"' for t in tokens)


def search(
    term: str, limit: int = 20, con: sqlite3.Connection | None = None
) -> list[SearchHit]:
    """FTS5 BM25-ranked pages matching ``term`` (AND across whitespace tokens).

    Returns dicts of url, title, last_updated, archive_url and a text snippet,
    best match first.
    """
    own = con is None
    con = con or connect(read_only=True)
    try:
        rows = con.execute(
            """
            SELECT p.url, p.title, p.last_updated, p.archive_url,
                   snippet(pages_fts, 2, '[', ']', ' … ', 12) AS snippet
            FROM pages_fts
            JOIN pages p ON p.rowid = pages_fts.rowid
            WHERE pages_fts MATCH ?
            ORDER BY bm25(pages_fts)
            LIMIT ?
            """,
            (_fts_query(term), limit),
        ).fetchall()
        return [cast("SearchHit", dict(r)) for r in rows]
    finally:
        if own:
            con.close()


# A pragmatic sentence splitter: break after ., !, or ? followed by whitespace,
# or on a line break (paragraph/heading boundary). Good enough to isolate a
# quotable sentence; the patch author verifies verbatim against the html blob /
# archive permalink anyway.
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")


def sentences(text: str | None) -> list[str]:
    """Split readable text into trimmed, non-empty sentences."""
    return [s.strip() for s in _SENTENCE_SPLIT.split(text or "") if s.strip()]


def quote(url: str, needle: str, con: sqlite3.Connection | None = None) -> list[str]:
    """Sentences in a page's extracted text containing ``needle`` (case-insensitive).

    The starting point for a verbatim ``note:`` quote in a data patch. The
    author still confirms wording against the stored html blob or the archive
    permalink before shipping.
    """
    rec = get(url, con=con)
    if not rec or not rec.get("text"):
        return []
    low = needle.lower()
    return [s for s in sentences(rec["text"]) if low in s.lower()]


def pages_missing_archive(con: sqlite3.Connection) -> list[NormalizedUrl]:
    """Normalized URLs of stored pages that have no Wayback permalink yet."""
    rows = con.execute(
        "SELECT url FROM pages WHERE archive_url IS NULL ORDER BY last_fetched_at"
    ).fetchall()
    return [r["url"] for r in rows]

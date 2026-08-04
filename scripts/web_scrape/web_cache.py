#!/usr/bin/env python3
"""Web cache: schema, URL normalization, upsert, and query helpers.

This is the library behind the web-scrape cache (see docs/WebCache.md). It owns
the SQLite system-of-record at ``ingest_sources/web/cache.sqlite`` plus the raw
blobs at ``ingest_sources/web/raw/<sha>.<ext>``. The fetcher
(``web_fetch.py``) writes through it; patch authors read through it.

Stdlib only (sqlite3, hashlib, urllib.parse, re). The SQLite ``fts5`` extension
ships with the standard CPython build.

Layout (all under ingest_sources/web/, R2-backed and gitignored):
    cache.sqlite        pages + fetches + pages_fts (FTS5)
    raw/<sha>.<ext>     raw page blobs, content-addressed (sha = sha256(raw
                        bytes)) so every distinct version of a page is preserved.
                        The extension is derived from a row's content_type, not
                        stored — see content_types.extension_for / blob_path

The raw blobs exist for re-extraction — re-deriving pages.text when the
extraction changes (web_backfill.py) and testing new extractions against real
pages without re-hitting any source. That is their only job: nothing resolves
a historical sha, and citations verify against pages.text, not blobs. They
live on disk rather than in SQLite to keep the DB lean and the FTS index fast.

Query helpers (an escalation ladder — reach for the next rung only when the
previous one wasn't enough):
    search(term)          FTS5 BM25-ranked pages (url, title, snippet)
    quote(url, needle)    sentence(s) in a page's text containing a needle;
                          context=N widens each hit by ±N lines, clipped to
                          its section. quote_hits() is the same read with each
                          hit's enclosing heading, so one call yields a cite
    outline(url)          the page's heading tree with per-section char counts
    section(url, heading) one heading's block(s), without the whole page
    get(url)              the full page record — the last resort

Plus one read that isn't a rung, because it asks about the corpus rather than
about a page:
    have(urls)            which of these URLs are already cached — the
                          planning question, before any of the above

These are also a CLI (``python web_cache.py search|quote|outline|section|
have|get``), so pulling a quote from a shell is one command just like caching
a page is.
"""

from __future__ import annotations

import hashlib
import re
import shutil
import sqlite3
import sys
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import NamedTuple, TypedDict, cast

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
WEB_DIR = REPO_ROOT / "ingest_sources" / "web"
DB_PATH = WEB_DIR / "cache.sqlite"
RAW_DIR = WEB_DIR / "raw"


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
    text: str | None
    rendered: int | None  # 1 if the blob is a headless-browser render, else 0/null
    text_source: str | None  # how `text` was derived: html|pdf|vtt|ocr|manual
    imported: int | None  # 1 if a human handed these bytes over, else 0/null


class SearchHit(TypedDict):
    """One FTS5 search result row from ``search()``."""

    url: NormalizedUrl
    title: str | None
    last_updated: str | None
    content_type: str | None  # what kind of document the hit is
    text_source: str | None  # how the hit's text was derived (html|pdf|vtt|ocr|manual)
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
    """sha256 of the raw page bytes; the raw blob filename stem.

    Content-addressed so each distinct version of a page is preserved: an
    unchanged refetch resolves to the same blob (no rewrite), a changed one
    writes a new file alongside the old. The ``pages`` row points at the current
    version; prior versions stay on disk and in the ``fetches`` log.
    """
    return hashlib.sha256(raw).hexdigest()


def blob_path(sha: str, ext: str = "html") -> Path:
    """Absolute path to a page's raw blob, ``raw/<sha>.<ext>``.

    ``ext`` defaults to ``html``; pass a non-HTML type's extension (a fetched
    PDF as ``pdf``) so the blob is stored as ``<sha>.pdf`` and re-opens in the
    right viewer on verify, rather than being mislabeled ``.html``. The fetcher
    passes ``handler.extension``; to locate a blob from a stored ``pages`` row,
    pass ``content_types.extension_for(row["content_type"])``.
    """
    return RAW_DIR / f"{sha}.{ext}"


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
        RAW_DIR.mkdir(parents=True, exist_ok=True)
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
  content_type     TEXT,               -- canonical MIME; the blob's extension derives from it
  text             TEXT,               -- extracted readable text (current version)
  rendered         INTEGER,            -- 1 if the blob is a headless-browser render
  text_source      TEXT,               -- how `text` was derived: html|pdf|vtt|ocr|manual
  imported         INTEGER             -- 1 if a human handed these bytes over
);

CREATE TABLE IF NOT EXISTS fetches (   -- append-only audit + version history
  id           INTEGER PRIMARY KEY,
  url          TEXT NOT NULL,
  fetched_at   TEXT NOT NULL,
  search_query TEXT,                   -- the intent that drove this fetch
  http_status  INTEGER,
  content_sha  TEXT,                   -- the version this fetch saw (blob stem)
  changed      INTEGER,                -- 1 if content differed from the prior fetch
  rendered     INTEGER,                -- 1 if this fetch was a headless render
  imported     INTEGER                 -- 1 if this row is a manual import, not a fetch
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


def _backup_before_destructive_migration() -> None:
    """Copy ``cache.sqlite`` to a timestamped sibling before a destructive step.

    Called only when a destructive migration (a column drop) is actually pending,
    never on a routine open — this SQLite is the system-of-record, so a botched
    drop must be recoverable from disk rather than from R2 (`make pull` would
    overwrite the migrated cache with whatever older copy R2 holds). The copy is
    consistent: ``connect()`` forces DELETE journal mode and the caller has
    committed before any drop runs. Dot-prefixed so ``make push``'s walk (which
    skips dotfiles) never ships backups to R2.
    """
    if DB_PATH.exists():
        stamp = now_iso().replace(":", "")
        shutil.copy(DB_PATH, DB_PATH.with_name(f".{DB_PATH.name}.bak-{stamp}"))


def init_schema(con: sqlite3.Connection) -> None:
    """Create tables, the FTS5 index, and sync triggers if absent (idempotent).

    Then run column migrations: ``_SCHEMA`` is CREATE-only (it never touches an
    existing table), so a column added after a cache shipped — like ``rendered``
    — must be ALTERed in here for older ``cache.sqlite`` files, and a retired one
    dropped. This SQLite is the system-of-record (not a blow-away-safe artifact
    like the DuckDB tables), so once it holds shipped/accumulated evidence a
    schema change must be a real migration like the ones below, guarded so it's
    a no-op on fresh DBs. Destructive steps write a timestamped backup first
    (see ``_backup_before_destructive_migration``).
    """
    con.executescript(_SCHEMA)
    pages_cols = {
        r[0] for r in con.execute("SELECT name FROM pragma_table_info('pages')")
    }
    fetches_cols = {
        r[0] for r in con.execute("SELECT name FROM pragma_table_info('fetches')")
    }
    # One safety copy per run, taken before any migration touches the file and
    # only when a destructive step (a column drop) is actually pending — never
    # on a routine open. The file is consistent here: `executescript` committed
    # and nothing below has run yet.
    if "html_file" in pages_cols or "text_sha" in fetches_cols:
        _backup_before_destructive_migration()
    # `rendered` was added with the headless-render fallback; ALTER it onto caches
    # created before it (a fresh DB already has it from _SCHEMA — guard skips it).
    if "rendered" not in pages_cols:
        con.execute("ALTER TABLE pages ADD COLUMN rendered INTEGER")
    # `html_file` (a stored blob path like 'html/<sha>.html') was dropped: a blob's
    # extension now derives from its `content_type` (content_types.extension_for),
    # so the row needn't store the path. Drop it from pre-change caches; the paired
    # on-disk move html/ -> raw/ is a filesystem step, not a schema one. Guard skips
    # an already-migrated/fresh DB.
    if "html_file" in pages_cols:
        con.execute("ALTER TABLE pages DROP COLUMN html_file")
    # `text_source` arrived with image OCR, when the cache first held text of
    # materially different reliability (a Vision transcription vs a PDF's own
    # text layer). Rows written before it stay NULL: we know how they were
    # extracted, but back-filling a guess into an evidence column is exactly
    # the kind of after-the-fact assertion this store must not make.
    if "text_source" not in pages_cols:
        con.execute("ALTER TABLE pages ADD COLUMN text_source TEXT")
    # `imported` arrived with the manual-import path (web_import.py). Rows that
    # predate it were genuinely fetched, but NULL is still the honest value:
    # it says "written before this distinction existed" rather than asserting
    # something about bytes nobody recorded a provenance for.
    if "imported" not in pages_cols:
        con.execute("ALTER TABLE pages ADD COLUMN imported INTEGER")
    if "rendered" not in fetches_cols:
        con.execute("ALTER TABLE fetches ADD COLUMN rendered INTEGER")
    if "imported" not in fetches_cols:
        con.execute("ALTER TABLE fetches ADD COLUMN imported INTEGER")
    # `text_sha` (sha256 of the text each fetch stored) was dropped: nothing ever
    # read it, and its tamper-evidence rationale didn't survive scrutiny — the
    # hash lived in the same local file as the text it was meant to police, and
    # quote verification only needs to hold from patch authoring to patch commit,
    # not across the store's history. Drop it from pre-change caches.
    # The safety copy for this drop was already taken by the single top-of-run
    # backup above — a second call here could overwrite that pristine copy with
    # a mid-migration state (the filename is second-resolution).
    if "text_sha" in fetches_cols:
        con.execute("ALTER TABLE fetches DROP COLUMN text_sha")
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
    fetched_at: str,
    last_updated: str | None = None,
    title: str | None = None,
    http_status: int | None = None,
    content_type: str | None = None,
    text: str | None = None,
    rendered: bool | None = None,
    text_source: str | None = None,
    imported: bool | None = None,
) -> None:
    """Insert or refresh a page row, keyed on the normalized URL.

    On conflict, points the row at the freshly-fetched version
    (``content_sha``/``content_type``/``text``/``rendered``/...) and bumps
    ``last_fetched_at`` while preserving ``first_fetched_at``.

    ``text_source`` records how ``text`` was derived (``html``/``pdf``/``vtt``
    from the handler that extracted it, ``ocr`` for a machine-read image,
    ``manual`` for a human transcription), so a consumer can weigh a quote by
    how lossy its extraction path was.
    """
    con.execute(
        """
        INSERT INTO pages (
          url, raw_url, content_sha, first_fetched_at, last_fetched_at,
          last_updated, title, http_status, content_type, text, rendered,
          text_source, imported
        ) VALUES (
          :url, :raw_url, :content_sha, :fetched_at, :fetched_at,
          :last_updated, :title, :http_status, :content_type, :text,
          :rendered, :text_source, :imported
        )
        ON CONFLICT(url) DO UPDATE SET
          raw_url       = excluded.raw_url,
          content_sha   = excluded.content_sha,
          last_fetched_at = excluded.last_fetched_at,
          last_updated  = excluded.last_updated,
          title         = excluded.title,
          http_status   = excluded.http_status,
          content_type  = excluded.content_type,
          text          = excluded.text,
          rendered      = excluded.rendered,
          text_source   = excluded.text_source,
          imported      = excluded.imported
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
            "text": text,
            "rendered": None if rendered is None else int(rendered),
            "text_source": text_source,
            "imported": None if imported is None else int(imported),
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
    rendered: bool | None = None,
    imported: bool | None = None,
) -> None:
    """Append one row to the fetch audit log + version history.

    ``imported`` marks a row the manual importer wrote: bytes a human handed
    over, never retrieved by this code. Such a row carries no ``http_status``
    — no request was made, and a fabricated 200 would make this log lie about
    the one thing it exists to record.
    """
    con.execute(
        "INSERT INTO fetches (url, fetched_at, search_query, http_status, "
        "content_sha, changed, rendered, imported) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            url,
            fetched_at,
            search_query,
            http_status,
            content_sha,
            None if changed is None else int(changed),
            None if rendered is None else int(rendered),
            None if imported is None else int(imported),
        ),
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

    Returns ``SearchHit`` dicts, best match first.
    """
    own = con is None
    con = con or connect(read_only=True)
    try:
        rows = con.execute(
            """
            SELECT p.url, p.title, p.last_updated, p.content_type, p.text_source,
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
# quotable sentence; the patch author verifies verbatim against the raw blob
# anyway.
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")


def sentences(text: str | None) -> list[str]:
    """Split readable text into trimmed, non-empty sentences."""
    return [s.strip() for s in _SENTENCE_SPLIT.split(text or "") if s.strip()]


# --------------------------------------------------------------------------- #
# Document structure, and the reads built on it (quote / outline / section)
# --------------------------------------------------------------------------- #

# An ATX heading line as the HTML extractor emits them: 1-6 #'s, a space, text.
_ATX_HEADING = re.compile(r"^(#{1,6}) (.+)$")


class _Heading(NamedTuple):
    """One heading line in a page's stored markdown."""

    line_idx: int
    level: int  # 1-6, the ATX level
    text: str  # without the # prefix


# One step toward a heading: its ATX level and casefolded text. The level is
# part of the step's identity because a same-named heading one level down is a
# different place — nested inside its parent's block, not a repeat of it.
type _TreeStep = tuple[int, str]

# A heading's place in the document tree: each still-open ancestor in order,
# then the heading itself. Two headings share a path exactly when they are the
# same place, which is the only time ``outline()`` may collapse them to one row.
type _TreePath = tuple[_TreeStep, ...]


class _Doc(NamedTuple):
    """A page's text parsed for navigation: frontmatter span + headings."""

    lines: list[str]
    fm_close: int | None  # line index of the closing --- delimiter, or None
    headings: list[_Heading]


class OutlineEntry(TypedDict):
    """One row of ``outline()``: a heading and the size of its section."""

    level: int
    heading: str
    chars: int
    count: int  # how many blocks this name opens (>1 on a repeated heading)


class QuoteHit(TypedDict):
    """One hit from ``quote_hits()``: a span and the section it sits in."""

    text: str
    heading: str | None  # enclosing section name, or None above the first heading


def _parse_doc(text: str) -> _Doc:
    """Parse stored text into its frontmatter span and heading list.

    The HTML extractor assembles ``text`` as YAML-style frontmatter (``---`` on
    line 1, ``key: value`` lines, a closing ``---``) followed by the page as
    markdown. Recognition is positional: the frontmatter exists only when line
    1 is ``---``, and the next ``---`` line closes it — frontmatter holds only
    ``key: value`` lines, so the first such line is always ours, never a
    thematic break from the page. Headings are ATX lines outside the
    frontmatter and outside code fences. A literal paragraph starting with
    ``#`` and a space can still misparse — accepted: these helpers are
    navigation aids, and a misparse costs a slightly-off outline, never a
    wrong quote (verification and FTS read no structure).
    """
    lines = text.split("\n")
    fm_close: int | None = None
    if lines and lines[0] == "---":
        fm_close = next((i for i, ln in enumerate(lines) if i and ln == "---"), None)
    start = fm_close + 1 if fm_close is not None else 0
    headings: list[_Heading] = []
    in_fence = False
    for i in range(start, len(lines)):
        line = lines[i]
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        match = _ATX_HEADING.match(line)
        if match:
            headings.append(_Heading(i, len(match.group(1)), match.group(2).strip()))
    return _Doc(lines, fm_close, headings)


def _metadata_block(doc: _Doc) -> str | None:
    """The frontmatter's ``key: value`` lines (no delimiters), or None."""
    if doc.fm_close is None:
        return None
    return "\n".join(doc.lines[1 : doc.fm_close]).strip()


def _body_block(doc: _Doc) -> str | None:
    """Everything after the frontmatter, or None when there is none."""
    if doc.fm_close is None:
        return None
    return "\n".join(doc.lines[doc.fm_close + 1 :]).strip()


def _heading_block(doc: _Doc, k: int) -> str:
    """The block ``doc.headings[k]`` opens: its line through the line before
    the next heading at the same or a higher (smaller-number) level."""
    h = doc.headings[k]
    end = next(
        (h2.line_idx for h2 in doc.headings[k + 1 :] if h2.level <= h.level),
        len(doc.lines),
    )
    return "\n".join(doc.lines[h.line_idx : end]).strip()


class _Section(NamedTuple):
    """The section a line sits in: what to call it, and where it ends."""

    # The opening heading's line index — identity, so two same-named sections
    # stay distinct. -1 is the frontmatter, -2 the prose above the first heading.
    id: int
    name: str | None
    start: int  # first line of the section (its heading line, when it has one)
    end: int  # one past its last line


def _enclosing_section(doc: _Doc, line_idx: int) -> _Section:
    """The section ``line_idx`` sits in, and the lines it spans.

    ``name`` is a name ``section()`` accepts, so a hit's locator is also the
    way to pull its surroundings: the nearest heading at or above the line, or
    ``"metadata"`` inside the frontmatter. A page whose text opens with prose
    before its first heading yields None for that prose — there is no heading
    to name, and ``"body"`` would claim a precision the page does not have (it
    spans every other section too).

    ``start``/``end`` are the same span ``section()`` would return, so a caller
    can hold a quote inside the section it is about to be labelled with.
    """
    if doc.fm_close is not None and line_idx <= doc.fm_close:
        # Inside the delimiters, not around them — the same lines
        # `section("metadata")` returns, so a hit here is contained by the
        # block it names instead of carrying a stray `---`.
        return _Section(-1, "metadata", 1, doc.fm_close)
    # Headings are in ascending line order, so the last one at or above the
    # line is the first such heading scanning backwards.
    for k in range(len(doc.headings) - 1, -1, -1):
        h = doc.headings[k]
        if h.line_idx <= line_idx:
            end = next(
                (h2.line_idx for h2 in doc.headings[k + 1 :] if h2.level <= h.level),
                len(doc.lines),
            )
            return _Section(h.line_idx, h.text, h.line_idx, end)
    body_start = doc.fm_close + 1 if doc.fm_close is not None else 0
    first_heading = doc.headings[0].line_idx if doc.headings else len(doc.lines)
    return _Section(-2, None, body_start, first_heading)


class _Window(NamedTuple):
    """The lines shown for one hit: a match plus its padding, within a section."""

    start: int
    end: int  # one past its last line
    match_line: int  # where the heading is taken from, never the window's edges
    section_id: int  # the _Section it is clipped to; merges never cross it


def quote_hits(
    url: str,
    needle: str,
    *,
    context: int = 0,
    con: sqlite3.Connection | None = None,
) -> list[QuoteHit]:
    """``quote()`` with each hit's enclosing heading — a cite in one call.

    A patch cite needs a verbatim ``quote`` **and** a ``locator`` saying where
    it sits, and this answers both at once. Each hit carries ``heading``: the
    nearest heading at or above it (or ``"metadata"`` inside the frontmatter,
    None above the first heading), ready to become ``locator: in the <x>
    section`` and to pass straight back to ``section()``.

    A hit never leaves the section it names — the name is the match's own, and
    padding clips to the section's bounds — so ``context`` changes how much you
    see without changing where the evidence is said to live, and any span
    lifted out of a hit can carry that hit's locator.

    The heading is only as good as the page's own markup. A page-builder site
    whose tab labels are real ``<h2>``s yields locators like ``$7,995``;
    faithful to the document, and no more wrong than the outline it comes from.
    """
    rec = get(url, con=con)
    if not rec or not rec.get("text"):
        return []
    doc = _parse_doc(rec["text"] or "")
    low = needle.lower()
    if context <= 0:
        # Per-line, then per-sentence: identical spans to splitting the whole
        # text at once (_SENTENCE_SPLIT breaks on \n+, so no sentence ever
        # crosses a line) but each one keeps the line index its heading needs.
        return [
            QuoteHit(text=s, heading=_enclosing_section(doc, i).name)
            for i, line in enumerate(doc.lines)
            for s in sentences(line)
            if low in s.lower()
        ]
    # Both the clip and the section check keep a hit's label true of every word
    # in it: unclipped padding spills across a heading, and windows either side
    # of one can abut exactly, so a merged pair would hold evidence from two
    # sections under a single name. Near an edge that yields less than ±N.
    windows: list[_Window] = []
    for i, line in enumerate(doc.lines):
        if low not in line.lower():
            continue
        sec = _enclosing_section(doc, i)
        start = max(sec.start, i - context)
        end = min(sec.end, i + context + 1)
        last = windows[-1] if windows else None
        if last is not None and start <= last.end and sec.id == last.section_id:
            windows[-1] = last._replace(end=max(last.end, end))
        else:
            windows.append(_Window(start, end, i, sec.id))
    return [
        QuoteHit(
            text="\n".join(doc.lines[w.start : w.end]).strip(),
            heading=_enclosing_section(doc, w.match_line).name,
        )
        for w in windows
    ]


def quote(
    url: str,
    needle: str,
    *,
    context: int = 0,
    con: sqlite3.Connection | None = None,
) -> list[str]:
    """Text in a page containing ``needle`` (case-insensitive), document order.

    With ``context=0`` (the default): the matching sentences, one per hit — the
    starting point for a verbatim ``cite.quote`` in a data patch. The author
    still confirms wording against the stored raw blob before shipping.

    With ``context=N``: each hit widened to ±N surrounding lines, so confirming
    a span's surroundings rarely needs the whole page. Overlapping windows
    merge and duplicate matches collapse; results stay in document order — no
    reordering toward "better-looking" matches, which would conflict with the
    windows and buy skim-comfort at the cost of a confusing contract.

    Just the spans. ``quote_hits()`` is the same read with each hit's enclosing
    heading attached; this stays the plain-text form because it is what the
    quote gate consumes — a verbatim span and nothing to strip back off.
    """
    return [h["text"] for h in quote_hits(url, needle, context=context, con=con)]


def outline(url: str, con: sqlite3.Connection | None = None) -> list[OutlineEntry]:
    """The page's heading tree with a per-section char count, document order.

    A few hundred chars that say where a long page's weight sits ("intro 2K,
    machine list 4K, 41 comments 32K"), so a session can pull one section with
    ``section()`` instead of reading the whole text. On an assembled page the
    tree is led by two level-0 pseudo-sections, ``metadata`` (the frontmatter)
    and ``body`` (everything after it). Each count is the size of the block
    ``section()`` would return for that name — subsections included, so a
    parent's count contains its children's. Pure read over ``pages.text``.

    A name repeated **in the same place in the tree** — same ancestors, same
    level — collapses to one row carrying ``count`` and the summed size, held
    at its first appearance. Page-builder sites make this the difference
    between a map and a wall: a real product page came back with 98 headings,
    35 distinct. Nothing actionable is lost, since ``section()`` answers a
    repeated name with every matching block anyway.

    Two things narrow that collapse, and each prevents a specific lie:

    - **Rows collapse on their whole ``_TreePath``**, not on the bare name, so
      a name in two places stays two rows. A manufacturer index repeating
      ``back to top`` under each maker would otherwise file them all under the
      first, asserting the rest have none.
    - **A repeat that opens a subsection** is never collapsed. The list is
      flat, so a row's parent is whatever precedes it at a lower level, and
      folding away a repeated parent strands its children under whatever
      printed last.

    So a name recurring across places stays several rows, and their counts
    together add up to what ``section()`` returns for it.
    """
    rec = get(url, con=con)
    if not rec or not rec.get("text"):
        return []
    doc = _parse_doc(rec["text"] or "")
    entries: list[OutlineEntry] = []
    meta_block, body_block = _metadata_block(doc), _body_block(doc)
    if meta_block is not None:
        entries.append(
            OutlineEntry(level=0, heading="metadata", chars=len(meta_block), count=1)
        )
    if body_block is not None:
        entries.append(
            OutlineEntry(level=0, heading="body", chars=len(body_block), count=1)
        )
    # Collapse repeats of a name in the same place in the tree, held at first
    # appearance so the tree still reads top-down. Popping the closed ancestors
    # first leaves `stack` as this heading's own path, which is the key.
    seen: dict[_TreePath, int] = {}
    stack: list[_Heading] = []
    for k, h in enumerate(doc.headings):
        while stack and stack[-1].level >= h.level:
            stack.pop()
        stack.append(h)
        path: _TreePath = tuple((a.level, a.text.casefold()) for a in stack)
        chars = len(_heading_block(doc, k))
        # A deeper next heading is this occurrence's child; collapsing such an
        # occurrence would strand that child under whatever row printed last.
        opens_subsection = (
            k + 1 < len(doc.headings) and doc.headings[k + 1].level > h.level
        )
        if path in seen and not opens_subsection:
            entries[seen[path]]["chars"] += chars
            entries[seen[path]]["count"] += 1
            continue
        seen[path] = len(entries)
        entries.append(
            OutlineEntry(level=h.level, heading=h.text, chars=chars, count=1)
        )
    return entries


def section(url: str, heading: str, con: sqlite3.Connection | None = None) -> list[str]:
    """The block(s) under a heading: from its line to the next heading at the
    same or a higher level, heading line included — so a session that navigated
    here can cite "in the <heading> section" for free. Case-insensitive exact
    match on the heading text; if it matches more than once, every matching
    block is returned — ambiguity should surface, not silently pick one. On an
    assembled page two pseudo-sections are addressable too: ``"metadata"`` (the
    frontmatter's ``key: value`` lines) and ``"body"`` (everything after it)."""
    rec = get(url, con=con)
    if not rec or not rec.get("text"):
        return []
    doc = _parse_doc(rec["text"] or "")
    target = heading.strip().casefold()
    blocks: list[str] = []
    pseudo = {"metadata": _metadata_block, "body": _body_block}.get(target)
    if pseudo is not None:
        block = pseudo(doc)
        if block is not None:
            blocks.append(block)
    blocks.extend(
        _heading_block(doc, k)
        for k, h in enumerate(doc.headings)
        if h.text.casefold() == target
    )
    return blocks


class Holding(TypedDict):
    """What the cache holds for one requested URL — ``have()``'s answer."""

    asked: RawUrl  # the URL as the caller wrote it
    page: PageRow | None  # the stored row, or None if nothing is cached
    stored_url: NormalizedUrl | None  # where it actually lives, when redirected
    error: str | None  # why it couldn't be looked up at all (unparseable URL)


def _alias_index(con: sqlite3.Connection) -> dict[NormalizedUrl, NormalizedUrl]:
    """Map each redirected page's requested address to where it was filed.

    One pass over the redirected rows rather than a scan per lookup, and only
    the two key columns — selecting whole rows would pull the corpus's text
    into memory for nothing. Newest first plus ``setdefault``, so when one
    requested address has redirected to different destinations over time the
    alias resolves to the current one rather than to whichever row SQLite
    happened to return first.
    """
    index: dict[NormalizedUrl, NormalizedUrl] = {}
    for row in con.execute(
        "SELECT url, raw_url FROM pages WHERE raw_url IS NOT NULL "
        "ORDER BY last_fetched_at DESC"
    ):
        try:
            index.setdefault(normalize_url(row["raw_url"]), row["url"])
        except ValueError:
            # A stored address that no longer parses must not take out every
            # lookup in the batch; it just contributes no alias.
            continue
    return index


def have(urls: list[str], con: sqlite3.Connection | None = None) -> list[Holding]:
    """Which of ``urls`` the cache already holds, in the order asked.

    The planning read: before a campaign, the question is which of N sources
    are already in hand and which still need fetching — content-blind, so
    ``search()`` can't answer it, and N-at-a-time, so ``get()`` alone means
    re-writing the same loop each time.

    A URL is held if it is cached under its normalized form **or** as the
    ``raw_url`` of a page that redirected elsewhere, in which case
    ``stored_url`` says where it landed. That fallback is why this belongs in
    the library: 8% of the current corpus lives under a different address than
    the one requested, and a plain ``get()`` loop calls all of those missing.
    Aliases match normalized, like every other lookup here.

    It is best-effort, though: ``raw_url`` holds the **most recent** fetch's
    address, so refetching through the canonical URL replaces the alias and the
    old address reports missing again. Closing that would take a permanent
    alias table in the system-of-record, which is a lot to buy a planning
    convenience when the cost of the gap is one redundant polite refetch.

    A URL that doesn't parse gets a ``Holding`` carrying ``error`` instead of
    raising. That is not the same as missing — missing means looked up and not
    found, and this one could not be looked up at all — so callers should keep
    the two apart. It says nothing about whether the fetcher would accept the
    URL; that is ``web_fetch``'s call and it makes it per URL.
    """
    own = con is None
    con = con or connect(read_only=True)
    try:
        # None, not {}: the index costs a scan of every redirected page, so it
        # waits for the first miss and a list that is entirely in hand never
        # pays for it.
        aliases: dict[NormalizedUrl, NormalizedUrl] | None = None
        holdings: list[Holding] = []
        for asked in urls:
            try:
                page = get(asked, con=con)
                stored_url = None
                if page is None:
                    if aliases is None:
                        aliases = _alias_index(con)
                    target = aliases.get(normalize_url(asked))
                    if target is not None:
                        page = get(target, con=con)
                        # Never report a location for a page also being called
                        # absent.
                        if page is not None:
                            stored_url = target
            except ValueError as exc:
                # One bad line in a campaign list must not cost the answer for
                # the other sixty.
                holdings.append(
                    Holding(asked=asked, page=None, stored_url=None, error=str(exc))
                )
                continue
            holdings.append(
                Holding(asked=asked, page=page, stored_url=stored_url, error=None)
            )
        return holdings
    finally:
        if own:
            con.close()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _require_page(url: str) -> PageRow:
    """The cached page for ``url``, or a clean CLI exit if there isn't one."""
    rec = get(url)
    if rec is None:
        print(f"no cached page: {url}", file=sys.stderr)
        raise SystemExit(1)
    return rec


# Friendly type labels for search hits. Web pages are the unlabeled common
# case; anything else says what kind of document it is. An unmapped non-HTML
# content type falls back to its raw MIME label rather than passing as web.
_TYPE_LABELS = {
    "text/html": None,
    "application/xhtml+xml": None,
    "application/pdf": "pdf",
    "image/jpeg": "image",
    "image/jpg": "image",
    "image/png": "image",
    "text/vtt": "video transcript",
}


def _cmd_search(term: str, limit: int) -> int:
    hits = search(term, limit=limit)
    if not hits:
        print(f"no pages match: {term}", file=sys.stderr)
        return 1
    for i, hit in enumerate(hits):
        if i:
            print()
        print(f"url: {hit['url']}")
        print(f"title: {hit['title'] or '(no title)'}")
        if hit["last_updated"]:
            # The page's own stated publish/modified date — not when we fetched.
            print(f"last_updated: {hit['last_updated']}")
        content_type = hit["content_type"]
        type_label = _TYPE_LABELS.get(content_type or "", content_type)
        if type_label:
            print(f"type: {type_label}")
        if hit["text_source"] not in (None, "html"):
            # Flag hits whose text isn't a web page's own words — a PDF's text
            # layer, a caption track, OCR or a transcription — so the reader
            # knows to weigh (and for ocr, review) before quoting.
            print(f"text_source: {hit['text_source']}")
        # The snippet quotes the page mid-flow, so it can span stored line
        # breaks; collapse them for a one-line display. Matches are [bracketed].
        print(f"snippet: {' '.join(hit['snippet'].split())}")
    return 0


def _cmd_quote(url: str, needle: str, context: int) -> int:
    _require_page(url)
    hits = quote_hits(url, needle, context=context)
    if not hits:
        print(f"no matches for {needle!r} in {url}", file=sys.stderr)
        return 1
    # The locator prints on stdout with its span, not on stderr like section's
    # ambiguity note: a heading belongs to one hit, and the two streams give no
    # ordering guarantee to pair them by. Nothing is lost — quote's output is a
    # hit list, never a document, so `get`/`section` remain the pure-text reads.
    for i, hit in enumerate(hits):
        if i:
            print()
        if hit["heading"]:
            print(f"[{hit['heading']}]")
        print(hit["text"])
    return 0


def _cmd_outline(url: str, min_chars: int) -> int:
    _require_page(url)
    entries = outline(url)
    if not entries:
        print(f"no headings in {url}", file=sys.stderr)
        return 1
    shown = [e for e in entries if e["chars"] >= min_chars]
    for entry in shown:
        indent = "  " * entry["level"]
        repeat = f"  x{entry['count']}" if entry["count"] > 1 else ""
        print(f"{indent}{entry['heading']}{repeat}  [{entry['chars']} chars]")
    if len(shown) < len(entries):
        # Say what was withheld rather than letting a filtered map read as the
        # whole one.
        print(
            f"{len(entries) - len(shown)} headings under {min_chars} chars hidden "
            f"(--min-chars 0 to show all)",
            file=sys.stderr,
        )
    return 0


def _section_miss_hint(url: str, heading: str) -> str | None:
    """Why ``section()`` missed, when the page can tell us — else None.

    A miss has three very different causes, and undifferentiated they all read
    as "not on this page", each costing a round trip to tell apart: the needle
    names no heading but *is* part of one, it appears only as body text (a
    styled div the extractor faithfully kept as prose — the common shape on
    page-builder sites), or the page genuinely never says it.

    Order matters. ``section()`` matches exactly, so a heading that merely
    contains the needle is a near miss and must be reported as one; calling it
    body text would be false.
    """
    target = heading.strip().casefold()
    if not target:
        # The empty string is inside every heading — hinting on it would recite
        # the whole outline and say nothing.
        return None
    near = [e["heading"] for e in outline(url) if target in e["heading"].casefold()]
    if near:
        return (
            f"headings match exactly, not by substring; did you mean: {', '.join(near)}"
        )
    # No heading contains it, so any hit below is necessarily body text.
    where = dict.fromkeys(
        h["heading"] for h in quote_hits(url, heading) if h["heading"]
    )
    if where:
        return f"not a heading; appears as text in section(s): {', '.join(where)}"
    return None


def _cmd_section(url: str, heading: str) -> int:
    _require_page(url)
    blocks = section(url, heading)
    if not blocks:
        print(f"no section {heading!r} in {url}", file=sys.stderr)
        hint = _section_miss_hint(url, heading)
        if hint:
            print(hint, file=sys.stderr)
        return 1
    if len(blocks) > 1:
        # The note goes to stderr so stdout stays pure page text.
        print(f"{len(blocks)} sections match {heading!r}", file=sys.stderr)
    print("\n\n".join(blocks))
    return 0


def _read_url_list(path: str) -> list[tuple[str, str]]:
    """``(url, source line)`` pairs from a file. Blank and ``#`` lines skipped.

    The same shape ``web_fetch.py --from-file`` reads — its ``url<TAB>query``
    TSV — so one list drives both: check what you hold, fetch what you don't.
    The whole line is kept because the query column is the search intent, which
    ``web_fetch`` logs as provenance; re-emitting a miss as a bare URL would
    fetch the same page while dropping the record of why it was wanted.
    """
    pairs: list[tuple[str, str]] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        url = line.partition("\t")[0].strip()
        if url:
            pairs.append((url, line))
    return pairs


def _cmd_have(urls: list[str], from_file: str | None, missing_only: bool) -> int:
    # A URL given on the command line has no query column, so it stands as its
    # own source line.
    sources: list[tuple[str, str]] = [(u, u) for u in urls]
    if from_file:
        try:
            sources += _read_url_list(from_file)
        except OSError as exc:
            # A traceback would bury which path was wrong.
            print(f"cannot read {from_file}: {exc.strerror or exc}", file=sys.stderr)
            return 2
    if not sources:
        print("no URLs given (pass URLs or --from-file)", file=sys.stderr)
        return 2
    holdings = have([u for u, _ in sources])
    # "No answer" is not "not cached": these entries couldn't be looked up, so
    # they are called out rather than swept into the miss list, where they
    # would read as a considered verdict that the page isn't held.
    invalid = [h for h in holdings if h["error"]]
    missing = [
        (h, line)
        for h, (_, line) in zip(holdings, sources, strict=True)
        if h["page"] is None and not h["error"]
    ]
    if missing_only:
        for _, line in missing:
            print(line)
        for h in invalid:
            print(
                f"skipping unparseable URL: {h['asked']} ({h['error']})",
                file=sys.stderr,
            )
        return 1 if missing or invalid else 0
    for h in holdings:
        page = h["page"]
        if h["error"]:
            print(f"INVALID  {h['asked']}  ({h['error']})")
            continue
        if page is None:
            print(f"MISSING  {h['asked']}")
            continue
        # Size in chars, because that is what a read of this page will cost.
        facts = [f"{len(page['text'] or '')} chars"]
        content_type = page["content_type"]
        if content_type is None:
            # The column is nullable, and "html" is the wrong guess to print
            # for a row that never recorded one — say what is known instead.
            facts.append("type unrecorded")
        else:
            label = _TYPE_LABELS.get(content_type, content_type)
            facts.append(label or "html")
        if page["text_source"] not in (None, "html"):
            facts.append(page["text_source"] or "")
        if page["rendered"]:
            facts.append("rendered")
        if page["imported"]:
            facts.append("imported")
        print(f"cached   {h['asked']}  {'  '.join(facts)}")
        if h["stored_url"]:
            # Say so rather than quietly reporting a hit under another address:
            # the caller asked about one URL and is being answered about another.
            print(f"         ↳ stored as {h['stored_url']} (redirected)")
    # stdout is block-buffered when redirected while stderr is not, so the tally
    # would otherwise print above the list it is summarizing.
    sys.stdout.flush()
    cached = len(holdings) - len(missing) - len(invalid)
    tally = f"{cached}/{len(holdings)} cached"
    if invalid:
        tally += f", {len(invalid)} unparseable"
    print(tally, file=sys.stderr)
    return 1 if missing or invalid else 0


def _cmd_get(url: str) -> int:
    # Row metadata on stderr, text on stdout — so `get <url> > page.md` lands
    # just the document.
    rec = _require_page(url)
    for key, value in rec.items():
        if key != "text":
            print(f"{key}: {value}", file=sys.stderr)
    if rec["text"]:
        print(rec["text"])
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI over the query helpers — the same escalation ladder, printed.

    The Python API above stays the interface for programmatic use (flippatch's
    quote gate, multi-step sessions); this is the shell-friendly face of the
    same five reads, so pulling a quote is one command just like caching a
    page is.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Query the web evidence cache (see docs/WebCache.md)."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_search = sub.add_parser("search", help="FTS5 BM25-ranked pages matching a term")
    p_search.add_argument("term")
    p_search.add_argument("--limit", type=int, default=20)

    p_quote = sub.add_parser(
        "quote", help="sentences containing a needle, each labelled with its section"
    )
    p_quote.add_argument("url")
    p_quote.add_argument("needle")
    p_quote.add_argument(
        "--context",
        type=int,
        default=0,
        help="widen each hit by ±N lines, clipped to its section "
        "(hits that then overlap merge)",
    )

    p_outline = sub.add_parser("outline", help="heading tree with section sizes")
    p_outline.add_argument("url")
    p_outline.add_argument(
        "--min-chars",
        type=int,
        default=0,
        help="hide headings whose block is under N chars (UI chrome, mostly)",
    )

    p_section = sub.add_parser("section", help="one heading's block(s)")
    p_section.add_argument("url")
    p_section.add_argument("heading")

    p_have = sub.add_parser("have", help="which of these URLs are already cached")
    p_have.add_argument("urls", nargs="*", help="URLs to check")
    p_have.add_argument(
        "--from-file", help="file of URLs, one per line (web_fetch's TSV works too)"
    )
    p_have.add_argument(
        "--missing",
        action="store_true",
        help="print only the uncached entries, each as its source line with the "
        "query column intact — feeds web_fetch.py --from-file",
    )

    p_get = sub.add_parser("get", help="full page record (text on stdout)")
    p_get.add_argument("url")

    args = parser.parse_args(argv)
    match args.command:
        case "search":
            return _cmd_search(args.term, args.limit)
        case "quote":
            return _cmd_quote(args.url, args.needle, args.context)
        case "outline":
            return _cmd_outline(args.url, args.min_chars)
        case "section":
            return _cmd_section(args.url, args.heading)
        case "have":
            return _cmd_have(args.urls, args.from_file, args.missing)
        case "get":
            return _cmd_get(args.url)
    raise AssertionError(f"unhandled command {args.command}")


if __name__ == "__main__":
    sys.exit(main())

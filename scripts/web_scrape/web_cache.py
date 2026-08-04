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
                          context=N widens each hit to ±N lines
    outline(url)          the page's heading tree with per-section char counts
    section(url, heading) one heading's block(s), without the whole page
    get(url)              the full page record — the last resort

The same five reads are also a CLI (``python web_cache.py search|quote|
outline|section|get``), so pulling a quote from a shell is one command just
like caching a page is.
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
    """
    rec = get(url, con=con)
    if not rec or not rec.get("text"):
        return []
    low = needle.lower()
    if context <= 0:
        return [s for s in sentences(rec["text"]) if low in s.lower()]
    lines = (rec["text"] or "").split("\n")
    windows: list[tuple[int, int]] = []
    for i, line in enumerate(lines):
        if low not in line.lower():
            continue
        start, end = max(0, i - context), min(len(lines), i + context + 1)
        if windows and start <= windows[-1][1]:
            windows[-1] = (windows[-1][0], max(windows[-1][1], end))
        else:
            windows.append((start, end))
    return ["\n".join(lines[s:e]).strip() for s, e in windows]


# --------------------------------------------------------------------------- #
# Structural reads (outline / section)
# --------------------------------------------------------------------------- #

# An ATX heading line as the HTML extractor emits them: 1-6 #'s, a space, text.
_ATX_HEADING = re.compile(r"^(#{1,6}) (.+)$")


class _Heading(NamedTuple):
    """One heading line in a page's stored markdown."""

    line_idx: int
    level: int  # 1-6, the ATX level
    text: str  # without the # prefix


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


def outline(url: str, con: sqlite3.Connection | None = None) -> list[OutlineEntry]:
    """The page's heading tree with a per-section char count, document order.

    A few hundred chars that say where a long page's weight sits ("intro 2K,
    machine list 4K, 41 comments 32K"), so a session can pull one section with
    ``section()`` instead of reading the whole text. On an assembled page the
    tree is led by two level-0 pseudo-sections, ``metadata`` (the frontmatter)
    and ``body`` (everything after it). Each count is the size of the block
    ``section()`` would return for that name — subsections included, so a
    parent's count contains its children's. Pure read over ``pages.text``.
    """
    rec = get(url, con=con)
    if not rec or not rec.get("text"):
        return []
    doc = _parse_doc(rec["text"] or "")
    entries: list[OutlineEntry] = []
    meta_block, body_block = _metadata_block(doc), _body_block(doc)
    if meta_block is not None:
        entries.append(OutlineEntry(level=0, heading="metadata", chars=len(meta_block)))
    if body_block is not None:
        entries.append(OutlineEntry(level=0, heading="body", chars=len(body_block)))
    entries.extend(
        OutlineEntry(level=h.level, heading=h.text, chars=len(_heading_block(doc, k)))
        for k, h in enumerate(doc.headings)
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
    matches = quote(url, needle, context=context)
    if not matches:
        print(f"no matches for {needle!r} in {url}", file=sys.stderr)
        return 1
    print("\n\n".join(matches))
    return 0


def _cmd_outline(url: str) -> int:
    _require_page(url)
    entries = outline(url)
    if not entries:
        print(f"no headings in {url}", file=sys.stderr)
        return 1
    for entry in entries:
        indent = "  " * entry["level"]
        print(f"{indent}{entry['heading']}  [{entry['chars']} chars]")
    return 0


def _cmd_section(url: str, heading: str) -> int:
    _require_page(url)
    blocks = section(url, heading)
    if not blocks:
        print(f"no section {heading!r} in {url}", file=sys.stderr)
        return 1
    if len(blocks) > 1:
        # The note goes to stderr so stdout stays pure page text.
        print(f"{len(blocks)} sections match {heading!r}", file=sys.stderr)
    print("\n\n".join(blocks))
    return 0


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

    p_quote = sub.add_parser("quote", help="sentences in a page containing a needle")
    p_quote.add_argument("url")
    p_quote.add_argument("needle")
    p_quote.add_argument(
        "--context", type=int, default=0, help="widen each hit to ±N lines"
    )

    p_outline = sub.add_parser("outline", help="heading tree with section sizes")
    p_outline.add_argument("url")

    p_section = sub.add_parser("section", help="one heading's block(s)")
    p_section.add_argument("url")
    p_section.add_argument("heading")

    p_get = sub.add_parser("get", help="full page record (text on stdout)")
    p_get.add_argument("url")

    args = parser.parse_args(argv)
    match args.command:
        case "search":
            return _cmd_search(args.term, args.limit)
        case "quote":
            return _cmd_quote(args.url, args.needle, args.context)
        case "outline":
            return _cmd_outline(args.url)
        case "section":
            return _cmd_section(args.url, args.heading)
        case "get":
            return _cmd_get(args.url)
    raise AssertionError(f"unhandled command {args.command}")


if __name__ == "__main__":
    sys.exit(main())

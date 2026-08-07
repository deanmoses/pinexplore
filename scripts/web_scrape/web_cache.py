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
    search(term)          FTS5 BM25-ranked pages, each with its match count and
                          how many sections it matched in — per tier: `text`
                          (the citable layer) and `ocr` (machine-read sheet
                          images; findable, verified by rendering the sheet,
                          never quoted). Units AND together and a double-quoted
                          run is one phrase, so '"upper magnet" knocker' is
                          phrase-and-word
    search_sections(url, term)
                          that document's matching sections, document order,
                          each with its count — where in a long document the
                          term actually lives
    search_matches(url, term, section=…)
                          each match in a section, with surrounding words of
                          context. A sheet range (pages=(40, 50)) is the same
                          read reached by a different address
    quote(url, needle)    sentence(s) in a page's text containing a needle —
                          matching ignores case, smart quotes, and whitespace
                          runs (so a phrase spanning a line break still hits);
                          context=N widens each hit by ±N lines, clipped to
                          its section. quote_hits() is the same read with each
                          hit's enclosing heading and, on a PDF, the PDF page
                          number(s) the shown text sits on — one call, one cite
    outline(url)          the page's heading tree with per-section char counts
    section(url, heading) one heading's block(s), without the whole page
    get(url)              the full page record — the last resort

Plus one read that isn't a rung, because it asks about the corpus rather than
about a page:
    have(urls)            which of these URLs are already cached — the
                          planning question, before any of the above

These are also a CLI (``python web_cache.py search|quote|outline|section|
have|get``), so pulling a quote from a shell is one command just like caching
a page is. The three search scopes are one command there too, narrowed by
``--url`` and ``--section``/``--pages``.
"""

from __future__ import annotations

import hashlib
import re
import shutil
import sqlite3
import sys
import urllib.parse
from bisect import bisect_right
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, TypedDict, cast

if TYPE_CHECKING:
    # Type-only: the CLI imports argparse inside main(), so a library consumer
    # of the query helpers never pays for it.
    import argparse

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
    # Machine-read (OCR) text for the row's sheet/pixel content — the findability
    # tier. Never citable: quotes verify against `text` only (see PdfOcr.md).
    ocr_text: str | None
    rendered: int | None  # 1 if the blob is a headless-browser render, else 0/null
    text_source: str | None  # how `text` was derived: html|pdf|vtt|manual
    imported: int | None  # 1 if a human handed these bytes over, else 0/null


class SearchHit(TypedDict):
    """One document in ``search()``'s ranking — the global scope.

    One row per document, both tiers presented together: the asymmetry between
    a document's two counts (``95 (text) · 12 (ocr)``) is a triage signal that
    two rows thirty ranks apart could never carry. Ranked by its better tier.
    """

    url: NormalizedUrl
    title: str | None
    last_updated: str | None
    content_type: str | None  # what kind of document the hit is
    text_source: str | None  # how the citable text was derived (html|pdf|vtt|manual)
    snippet: str | None  # None on a row with nothing to snippet
    snippet_tier: str  # which tier the snippet shows; "ocr" text is not citable
    # Per tier: matched phrases plus matched loose words, and how many sections
    # they fall in. None when the count could not be taken (a marker collision,
    # see MarkerCollisionError); 0 when the tier exists but holds no match.
    matches: int | None
    sections: int | None
    ocr_matches: int | None
    ocr_sections: int | None
    # False when the row holds no text in that layer at all. Told apart from a
    # layer that simply doesn't contain the term, which also counts 0 — see
    # ``_match_label`` for why the two must not print alike.
    has_text: bool
    has_ocr: bool


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
  ocr_text         TEXT,               -- machine-read (OCR) text; findable, never citable
  rendered         INTEGER,            -- 1 if the blob is a headless-browser render
  text_source      TEXT,               -- how `text` was derived: html|pdf|vtt|manual
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

-- The OCR tier's own index — a second external-content table over the same
-- rowid, NOT an ocr_text column on pages_fts: FTS5 normalizes bm25 by a row's
-- total token count across every column, so a shared table would penalize a
-- document's *text*-tier rank for having been OCR'd even at weight zero. Two
-- tables give two independent bm25 spaces (see docs/plans/pdf_ocr/PdfOcr.md).
-- ocr_text alone — url/title stay pages_fts's, or an address token would match
-- a document once per tier.
CREATE VIRTUAL TABLE IF NOT EXISTS ocr_fts USING fts5(
  ocr_text, content='pages', content_rowid='rowid'
);

-- Guarded, unlike the pages_fts triggers above: FTS5's external-content
-- 'delete' command corrupts the index ("database disk image is malformed")
-- when issued for a rowid it never indexed, and most pages rows have no
-- ocr_text — including every row written before this table existed. The
-- guards keep the index holding exactly the rows with a non-NULL ocr_text,
-- so a delete can only ever name a row that is in it. (pages_fts needs no
-- guard: url is NOT NULL, so every row is indexed.) Deliberate side effect:
-- the tier's bm25 statistics describe only documents that actually have OCR,
-- instead of an average dragged to near zero by hundreds of empty rows. The
-- cost of the partial mirror is that FTS5's own 'rebuild'/'integrity-check'
-- commands assume a full one and must never be pointed at this table.
CREATE TRIGGER IF NOT EXISTS ocr_ai AFTER INSERT ON pages BEGIN
  INSERT INTO ocr_fts(rowid, ocr_text)
  SELECT new.rowid, new.ocr_text WHERE new.ocr_text IS NOT NULL;
END;
CREATE TRIGGER IF NOT EXISTS ocr_ad AFTER DELETE ON pages BEGIN
  INSERT INTO ocr_fts(ocr_fts, rowid, ocr_text)
  SELECT 'delete', old.rowid, old.ocr_text WHERE old.ocr_text IS NOT NULL;
END;
CREATE TRIGGER IF NOT EXISTS ocr_au AFTER UPDATE ON pages BEGIN
  INSERT INTO ocr_fts(ocr_fts, rowid, ocr_text)
  SELECT 'delete', old.rowid, old.ocr_text WHERE old.ocr_text IS NOT NULL;
  INSERT INTO ocr_fts(rowid, ocr_text)
  SELECT new.rowid, new.ocr_text WHERE new.ocr_text IS NOT NULL;
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
    # A pre-ocr_text cache may hold rows whose `text` is machine-read (labelled
    # text_source='ocr'); moving that text to `ocr_text` below NULLs a text
    # column on the system-of-record, so it counts as destructive too. The
    # existence check needs the column to exist, hence the guard.
    legacy_ocr_rows = "text_source" in pages_cols and bool(
        con.execute(
            "SELECT EXISTS(SELECT 1 FROM pages WHERE text_source = 'ocr')"
        ).fetchone()[0]
    )
    # One safety copy per run, taken before any migration touches the file and
    # only when a destructive step (a column drop, the ocr move) is actually
    # pending — never on a routine open. The file is consistent here:
    # `executescript` committed and nothing below has run yet.
    if "html_file" in pages_cols or "text_sha" in fetches_cols or legacy_ocr_rows:
        _backup_before_destructive_migration()
    # `ocr_text` arrived with PDF OCR (docs/plans/pdf_ocr/PdfOcr.md): the
    # machine-read tier, stored apart from `text` because quotes verify against
    # `text` alone and OCR is not character-exact enough to cite. First among
    # the migrations because the ocr_* triggers _SCHEMA just created reference
    # the column, and SQLite re-validates every trigger on a table during an
    # ALTER ... DROP COLUMN — so on a legacy cache the drops below would fail
    # with "no such column: new.ocr_text" until this has run.
    if "ocr_text" not in pages_cols:
        con.execute("ALTER TABLE pages ADD COLUMN ocr_text TEXT")
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
    # With `ocr_text` in place, `text_source='ocr'` retires: machine-read words
    # belong in `ocr_text` whatever produced them, and `text_source` goes back
    # to answering one question — how the *citable* layer was derived. Moving
    # rather than clearing, so a host that cannot OCR loses nothing: the old
    # reading stands as the row's OCR tier until a Mac replaces it. Idempotent
    # by construction (once run, nothing matches); the ocr_au/pages_au triggers
    # keep both FTS indexes in step with the move.
    if legacy_ocr_rows:
        con.execute(
            "UPDATE pages SET ocr_text = text, text = NULL, text_source = NULL "
            "WHERE text_source = 'ocr'"
        )
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
    ocr_text: str | None = None,
    rendered: bool | None = None,
    text_source: str | None = None,
    imported: bool | None = None,
) -> None:
    """Insert or refresh a page row, keyed on the normalized URL.

    On conflict, points the row at the freshly-fetched version
    (``content_sha``/``content_type``/``text``/``rendered``/...) and bumps
    ``last_fetched_at`` while preserving ``first_fetched_at``.

    ``text_source`` records how ``text`` was derived (``html``/``pdf``/``vtt``
    from the handler that extracted it, ``manual`` for a human transcription),
    so a consumer can weigh a quote by how lossy its extraction path was.

    ``ocr_text`` is the machine-read tier and follows a different rule, because
    most writers have no opinion about it (PDF OCR is a separate macOS-only
    pass, never part of a fetch): a supplied value is stored, and ``None``
    means *keep the stored value while it still describes these bytes* — kept
    when ``content_sha`` is unchanged, cleared when the bytes changed. So a
    refetch on a host that can't OCR never strands stale OCR against new
    bytes, and an unchanged refetch never discards an OCR pass's work.

    Deliberate conflation, worth naming: a Vision run that *succeeded and
    found nothing* also arrives as ``None``, and on unchanged bytes it
    preserves too. That inverts the text layer's rule, where successful-empty
    is a finding that overwrites — and the inversion is the point. ``text``
    is the citable record of what extraction currently yields and the quote
    gate verifies against it, so it must track the current extractor;
    ``ocr_text`` is a recall-only tier nothing may cite, the stored reading
    came from these very bytes via a deterministic recognizer (so it is never
    stale while the sha holds), and keeping the richer of two readings of
    identical bytes can mislead no one. A supplied/result flag here would
    exist only to let a weaker rerun delete evidence the bytes still support.
    """
    con.execute(
        """
        INSERT INTO pages (
          url, raw_url, content_sha, first_fetched_at, last_fetched_at,
          last_updated, title, http_status, content_type, text, ocr_text,
          rendered, text_source, imported
        ) VALUES (
          :url, :raw_url, :content_sha, :fetched_at, :fetched_at,
          :last_updated, :title, :http_status, :content_type, :text,
          :ocr_text, :rendered, :text_source, :imported
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
          ocr_text      = CASE
            WHEN excluded.ocr_text IS NOT NULL THEN excluded.ocr_text
            WHEN pages.content_sha = excluded.content_sha THEN pages.ocr_text
            ELSE NULL
          END,
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
            "ocr_text": ocr_text,
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
    was requested.

    Matched on the **normalized** form, like every other lookup here: a source
    list that spells the old address with a trailing slash or different host
    casing names the same page, and comparing raw text would send the fetcher
    back over the network for something already held. That means a scan rather
    than an indexed equality — normalization happens in Python — but it runs
    only when a URL missed under its own key, and it reads two columns rather
    than whole rows so the corpus's text stays on disk.
    """
    own = con is None
    con = con or connect(read_only=True)
    try:
        target = normalize_url(raw_url)
        for row in con.execute(
            "SELECT url, raw_url FROM pages WHERE raw_url IS NOT NULL "
            "ORDER BY last_fetched_at DESC"
        ):
            try:
                if normalize_url(row["raw_url"]) == target:
                    return get(row["url"], con=con)
            except ValueError:
                # A stored address that no longer parses contributes no alias
                # rather than breaking the lookup.
                continue
        return None
    finally:
        if own:
            con.close()


def _fts_units(term: str) -> tuple[list[str], bool]:
    """Split a search term into units on whitespace *outside* double quotes,
    plus whether a quote was left open.

    A double-quoted run is one unit however much whitespace it contains, which
    is what lets a caller ask for a phrase; bare words split as they always did.
    A quote is consumed rather than escaped into the output, so no quote
    character ever reaches the FTS parser — but it is replaced by a space, not
    deleted. That is the difference between preserving a term's meaning and
    changing it: ``unicode61`` reads a quote as a separator, so ``a"b`` is the
    two tokens ``a b``, and deleting the quote would silently make it the one
    token ``ab`` — a different document. Units are whitespace-normalized after
    the substitution, so the extra spaces leave no trace, and a run that holds
    no content (``""``) contributes no unit.

    A NUL is substituted the same way, and for the same reason: FTS5 scans its
    query as a C string, so a NUL truncates it mid-token and raises
    ``unterminated string`` — the one character besides ``"`` that this guard
    cannot let through. ``unicode61`` reads it as a separator too, so a space
    stands in for it losslessly. It is not a quote, though, and does not toggle.

    An unbalanced quote runs its unit to the end of the string — the guard here
    exists so no input raises, and rejecting would reintroduce exactly the error
    path it removes. The flag lets the CLI say what it ran.
    """
    units: list[str] = []
    buf: list[str] = []
    in_quote = False
    for ch in term:
        if ch == '"':
            in_quote = not in_quote
            buf.append(" ")  # a space, never nothing — see the docstring
        elif ch == "\0":
            buf.append(" ")  # likewise, but delimits nothing
        elif ch.isspace() and not in_quote:
            units.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    units.append("".join(buf))
    normalized = (" ".join(u.split()) for u in units)
    return [u for u in normalized if u], in_quote


def _fts_expr(units: list[str]) -> str:
    """Already-split units as an FTS5 expression: each one a quoted phrase,
    ANDed by juxtaposition. Split out so the CLI can show what it ran without
    re-parsing the term."""
    return " ".join(f'"{u}"' for u in units)


def _fts_query(term: str) -> str:
    """Turn a search term into an FTS5 AND-of-phrases expression.

    Each unit from ``_fts_units`` goes out as one double-quoted FTS5 phrase, so
    ``"upper magnet" knocker`` searches for the phrase *and* the word, while
    ``upper magnet`` ANDs two words as before. Quoting every unit is the
    injection guard: FTS5 operator characters inside one stay inert, so no user
    input can reach the parser as syntax.

    Returns ``""`` for a term holding no unit at all. FTS5 rejects an empty match
    expression, so callers must treat that as "no hits" rather than pass it on.
    """
    units, _unbalanced = _fts_units(term)
    return _fts_expr(units)


# A pragmatic sentence splitter: break after ., !, or ? followed by whitespace,
# or on a line/page break (paragraph, heading, or PDF page boundary — a form
# feed breaks like a newline whatever precedes it). Good enough to isolate a
# quotable sentence; the patch author verifies verbatim against the raw blob
# anyway. Every separator this consumes is a whitespace run, which is what
# lets _sentence_extent reuse match offsets computed on the whole line.
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|[\n\f]+")


def sentences(text: str | None) -> list[str]:
    """Split readable text into trimmed, non-empty sentences."""
    return [s.strip() for s in _SENTENCE_SPLIT.split(text or "") if s.strip()]


# Smart-quote straightening for quote matching, mirroring flippatch's quote
# gate (quote_verify/verify_quotes.py, _SMART): a phrase typed off a rendered
# page carries straight quotes where the stored text may have curly ones.
# RUF001 waived: the curly characters are the mapping's whole subject.
_MATCH_SMART = str.maketrans({"“": '"', "”": '"', "‘": "'", "’": "'"})  # noqa: RUF001


def _match_norm(s: str) -> str:
    """``s`` as the quote matcher compares it: smart quotes straightened,
    whitespace runs collapsed to single spaces (never deleted — word boundaries
    survive, so ``abc`` still does not match ``ab\\nc``), ends trimmed, and
    lowercased.

    This is flippatch's ``make verify-quotes`` normalization plus lowercasing:
    case-insensitivity is ``quote()``'s documented contract, and being one step
    more permissive than the gate is safe because a hit's text is always stored
    lines verbatim — over-matching can only surface a span whose typography
    differs from the needle typed, never invent one. ``lower()``, not
    ``casefold()``: the aggressive cross-script fold would quietly widen the
    documented contract for nothing this corpus contains.
    """
    return " ".join(s.translate(_MATCH_SMART).split()).lower()


class _Span(NamedTuple):
    """One match's extent across a unit list (a document's lines, or a line's
    sentences): the first and last unit it touches, plus the match's char
    offsets — ``start_off`` within the *first* unit's normalized text,
    ``end_off`` (exclusive) within the *last*'s. On a multi-unit span those
    are offsets into two different strings, so they are only meaningful
    together when ``first == last`` — the sentence-narrowing case they
    exist for."""

    first: int
    last: int
    start_off: int
    end_off: int


def _unit_bounds(units: list[str]) -> tuple[list[tuple[int, int, int]], str]:
    """Each non-empty unit's ``(start, end, index)`` in the normalized join,
    plus that join: unit norms separated by single spaces.

    Empty-normalizing units (blank lines, ``\\f`` markers) occupy no space and
    contribute no separator, so they can never skew a match's mapping back to
    its units — and can never be matched themselves.
    """
    bounds: list[tuple[int, int, int]] = []
    parts: list[str] = []
    pos = 0
    for idx, unit in enumerate(units):
        norm = _match_norm(unit)
        if not norm:
            continue
        if parts:
            pos += 1  # the single joining space
        bounds.append((pos, pos + len(norm), idx))
        parts.append(norm)
        pos += len(norm)
    return bounds, " ".join(parts)


def _find_spans(units: list[str], needle_norm: str) -> list[_Span]:
    """Non-overlapping occurrences of a normalized needle across ``units``,
    in document order, each mapped to the units it touches.

    One rule in each direction: comparison ignores whitespace (the units join
    through ``_unit_bounds``, so a needle spanning a unit boundary matches),
    output preserves it (spans map back to whole units; the caller returns
    those verbatim). ``needle_norm`` must be non-empty and ``_match_norm``-ed
    already, so it can never begin or end inside a collapsed run.
    """
    bounds, joined = _unit_bounds(units)
    starts = [b[0] for b in bounds]
    spans: list[_Span] = []
    at = joined.find(needle_norm)
    while at != -1:
        end = at + len(needle_norm)
        # The match's edge chars are non-space (the needle is normalized), so
        # each lies inside some unit's interval — never on a separator.
        first = bisect_right(starts, at) - 1
        last = bisect_right(starts, end - 1) - 1
        spans.append(
            _Span(
                bounds[first][2],
                bounds[last][2],
                at - bounds[first][0],
                end - bounds[last][0],
            )
        )
        at = joined.find(needle_norm, end)
    return spans


def _sentence_extent(sents: list[str], start_off: int, end_off: int) -> tuple[int, int]:
    """The inclusive range of ``sents`` a match at these offsets touches.

    The offsets come from ``_find_spans`` over whole lines and are relative to
    one line's normalized text; they transfer because the sentence norms joined
    on single spaces reproduce the line norm exactly — every separator
    ``_SENTENCE_SPLIT`` consumes is a whitespace run, which normalization would
    have collapsed to the same single space.
    """
    bounds, _ = _unit_bounds(sents)
    starts = [b[0] for b in bounds]
    first = bisect_right(starts, start_off) - 1
    last = bisect_right(starts, end_off - 1) - 1
    return bounds[first][2], bounds[last][2]


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
    """A page's text parsed for navigation: frontmatter, headings, PDF pages."""

    lines: list[str]
    fm_close: int | None  # line index of the closing --- delimiter, or None
    headings: list[_Heading]
    # First line of each PDF page, in order; [] when the document has no page
    # structure — deliberately distinct from a one-page document's [0].
    page_starts: list[int]


class OutlineEntry(TypedDict):
    """One row of ``outline()``: a heading and the size of its section."""

    level: int
    heading: str
    chars: int
    count: int  # how many blocks this name opens (>1 on a repeated heading)
    # Which layer the row maps: "text" (citable), or "ocr" on a dark document
    # whose only page map is machine-read — verify by rendering the sheet.
    tier: str


class SectionBlock(TypedDict):
    """One block ``section()`` returned, and which layer it came from.

    The tier rides the text rather than living only in the rung that found it:
    a function whose contract is verbatim citable text must not quietly start
    returning machine-read ink, and the tier is the whole of the difference.
    """

    text: str
    tier: str  # "text" (citable) or "ocr" (render the sheet to cite)


class QuoteHit(TypedDict):
    """One hit from ``quote_hits()``: a span, its section, and its PDF pages."""

    text: str
    # The match's enclosing section name; None above the first heading, and
    # None when the match itself crosses a section boundary (no single name
    # can be true of such a hit).
    heading: str | None
    # 1-based ordinal position within the PDF *file* of every page the shown
    # text touches (a merged context window can cross pages), inclusive of
    # pages it merely passes through. None — not [1] — when the document has
    # no page structure. Deliberately not named "page": the number printed on
    # the sheet is often different ink, and the locator convention names both
    # ("printed page 17, PDF document page 27").
    pdf_document_page_numbers: list[int] | None


def _parse_doc(text: str, *, assembled: bool) -> _Doc:
    """Parse stored text into its frontmatter span, headings, and PDF pages.

    The HTML extractor assembles ``text`` as YAML-style frontmatter (``---`` on
    line 1, ``key: value`` lines, a closing ``---``) followed by the page as
    markdown. Recognition is positional — the frontmatter exists only when
    line 1 is ``---``, and the next ``---`` line closes it — but **only on an
    assembled text** (``assembled`` is true when the row's ``text_source`` is
    ``html``, the one extractor that writes frontmatter). That gate matters
    more than it used to: quote matching treats the frontmatter as synthetic —
    delimiters unmatchable, no span across the boundary — which is correct for
    text we assembled and *suppresses real evidence* on a document whose own
    first line happens to be ``---`` (a PDF's rule line, a transcript). A
    document with unrecorded provenance gets its own-text reading, since lost
    metadata labels are the cheaper error. Headings are ATX lines outside the
    frontmatter and outside code fences. A literal paragraph starting with
    ``#`` and a space can still misparse — accepted: a heading misparse costs
    a slightly-off outline, never a wrong quote (verification and FTS read no
    structure).

    Pages come from the text itself, no content-type threading: a line that is
    exactly ``"\\f"`` is a page boundary, and only the PDF extractor writes one
    (the human-text paths replace stray form feeds — see web_pdftext.py), so
    "has page markers" and "is a paginated document" are the same predicate. A
    line's page number is 1 + the count of marker lines above it. Both guard
    halves below matter: without the empty case an unpaginated document would
    read as one-page, and without the length filter the trailing terminator
    poppler emits would mint a phantom last page.
    """
    lines = text.split("\n")
    fm_close: int | None = None
    if assembled and lines and lines[0] == "---":
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
    markers = [i for i, ln in enumerate(lines) if ln == "\f"]
    page_starts = (
        [] if not markers else [0] + [i + 1 for i in markers if i + 1 < len(lines)]
    )
    return _Doc(lines, fm_close, headings, page_starts)


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


# A page pseudo-section's name, matched against an already-casefolded target.
# A positive decimal with no leading zero, so the name a page is addressed by
# is the same one `outline()` printed and `quote` reported — `page 041` and
# `page 41a` name nothing rather than quietly resolving to sheet 41.
_PAGE_NAME = re.compile(r"^page ([1-9][0-9]*)$")

# A `--pages` sheet range, both ends inclusive. Same digit rule as _PAGE_NAME, so
# a range is written the way the sheets it spans are named.
_PAGE_RANGE = re.compile(r"^([1-9][0-9]*)-([1-9][0-9]*)$")


def _page_block(doc: _Doc, n: int) -> str | None:
    """PDF document page ``n``'s text (1-based), or None when there is no such
    page — the one definition of what counts as in range, so a caller reporting
    "out of range" and a caller returning the text can never disagree.

    The page runs from its first line to the marker that ends it, exclusive —
    a boundary is not content, the same rule ``_render_hit`` follows when it
    drops markers from a quoted span. The last page has no marker after it when
    poppler's trailing form feed was the final line, which ``_parse_doc``
    already declines to treat as opening a page. A sheet that exists but
    yielded no text is ``""``, distinct from None.
    """
    if not 1 <= n <= len(doc.page_starts):
        return None
    start = doc.page_starts[n - 1]
    end = next(
        (i for i in range(start, len(doc.lines)) if doc.lines[i] == "\f"),
        len(doc.lines),
    )
    return "\n".join(doc.lines[start:end]).strip()


def _doc_of(rec: PageRow, tier: str = "text") -> _Doc:
    """A row's stored text parsed — the one place the ``assembled`` gate is
    derived from ``text_source``, so the CLI can't disagree with the library.

    ``tier`` picks the column: ``text`` (the default everywhere outside the
    search scopes) or ``ocr`` for the machine-read layer, which is never
    assembled — no extractor writes frontmatter into it."""
    if tier == _OCR_TIER:
        return _parse_doc(rec.get("ocr_text") or "", assembled=False)
    return _parse_doc(rec["text"] or "", assembled=rec["text_source"] == "html")


def _page_doc(rec: PageRow) -> tuple[_Doc, str]:
    """The parsed column that defines ``page N`` for this row, and its tier.

    ``text`` is primary: its markers are the page map when it has any, and only
    otherwise ``ocr_text``'s — which is what gives a fully dark document (no
    text layer, OCR'd sheets) a page map at all. Falls back to the text parse
    when neither column is paginated, so callers can test ``page_starts``
    without caring which absence they got."""
    doc = _doc_of(rec)
    if doc.page_starts:
        return doc, _TEXT_TIER
    ocr_doc = _doc_of(rec, _OCR_TIER)
    if ocr_doc.page_starts:
        return ocr_doc, _OCR_TIER
    return doc, _TEXT_TIER


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


class _Hit(NamedTuple):
    """The unrendered form of one ``QuoteHit`` — everything but the text lookup.

    ``start``/``end`` are the shown line range; ``sentence_span`` (context<=0,
    one-line match only) narrows it further to a sentence extent within that
    line. ``match_line`` is the match's own first line — where the heading is
    read, never the window's edges. ``section_ids`` are the section(s) the
    *match* touches, in order: the merge guard compares whole tuples, so an
    ordinary hit ``(id,)`` merges exactly as before, while a cross-section hit
    can never absorb — or be absorbed by — a neighbour, and more than one id
    means ``heading=None``.
    """

    start: int
    end: int  # one past its last line
    match_line: int
    section_ids: tuple[int, ...]
    sentence_span: tuple[int, int] | None  # inclusive sentence indices


def _match_sections(doc: _Doc, first: int, last: int) -> list[_Section]:
    """The section(s) lines ``first``..``last`` (inclusive) pass through, in
    order, each entered at the previous one's ``end``.

    Deliberately coarser than nearest-heading granularity: a parent section's
    block runs through its nested subsections, so a span starting in the
    parent's own prose and running into a child stays one entry — the parent —
    and keeps its label. That is not the misattribution the cross-section rule
    exists to prevent: the parent's name is true of every line it covers
    (``section(parent)`` returns the child's text too), unlike a span across
    *sibling* sections, where no returned name would be. Only a span reaching
    past the block's end collects a second id and goes unlabeled.
    """
    at = first
    secs = [_enclosing_section(doc, at)]
    while secs[-1].end <= last:
        # A section's end is one past its last line, so the next one opens
        # there. max() guards the frontmatter quirk where the closing ---
        # line sits at its own section's end and would probe in place.
        at = max(secs[-1].end, at + 1)
        secs.append(_enclosing_section(doc, at))
    return secs


def _render_hit(doc: _Doc, hit: _Hit) -> QuoteHit:
    """A ``_Hit`` as the public ``QuoteHit``: text, heading, PDF pages.

    ``\\f`` marker lines are dropped from the shown text — a character no
    viewer renders is residue, not evidence, and the page numbers carry the
    fact of the boundary in a field instead. The drop is scoped to hit text
    only: ``get()``/``section()`` keep their markers, where a whole-document
    read needs the page structure visible.

    The page numbers describe the *shown* text, bisected from the first and
    last lines that survive into it — after the marker drop and the edge
    trim — so a window opening on a dropped marker or a blank line is never
    labelled with a page contributing nothing. Interior pages a merged window
    crosses stay included: whoever renders the hit needs every sheet the ink
    came from. (Contrast ``heading``, which names the match, not the window —
    a heading is a claim about meaning and must not move when the display
    widens; a page range is a claim about where the ink is and must cover
    everything shown.)
    """
    if hit.sentence_span is not None:
        sents = sentences(doc.lines[hit.match_line])
        text = " ".join(sents[hit.sentence_span[0] : hit.sentence_span[1] + 1])
        shown = [hit.match_line]
    else:
        shown = [i for i in range(hit.start, hit.end) if doc.lines[i] != "\f"]
        # Blank edge lines are dropped from the text, so drop them from the
        # page bookkeeping too — the label has to describe what is shown.
        # Only whole blank lines go: a content line's own indentation is
        # stored text, and this read's promise is to return that verbatim.
        while shown and not doc.lines[shown[0]].strip():
            shown.pop(0)
        while shown and not doc.lines[shown[-1]].strip():
            shown.pop()
        text = "\n".join(doc.lines[i] for i in shown)
    heading = (
        _enclosing_section(doc, hit.match_line).name
        if len(hit.section_ids) == 1
        else None
    )
    pages: list[int] | None = None
    if doc.page_starts and shown:
        first = bisect_right(doc.page_starts, shown[0])
        last = bisect_right(doc.page_starts, shown[-1])
        pages = list(range(first, last + 1))
    return QuoteHit(text=text, heading=heading, pdf_document_page_numbers=pages)


def quote_hits(
    url: str,
    needle: str,
    *,
    context: int = 0,
    con: sqlite3.Connection | None = None,
) -> list[QuoteHit]:
    """``quote()`` with each hit's enclosing heading and PDF page(s) — a cite
    in one call.

    A patch cite needs a verbatim ``quote`` **and** a ``locator`` saying where
    it sits, and this answers both at once. Each hit carries ``heading``: the
    nearest heading at or above the match (or ``"metadata"`` inside the
    frontmatter; None above the first heading, and None when the match itself
    crosses a section boundary), ready to become ``locator: in the <x>
    section`` and to pass straight back to ``section()``. On a PDF each hit
    also carries ``pdf_document_page_numbers`` — the page(s) of the file its
    shown text sits on, the number that navigates a PDF reader to the page
    worth rendering.

    Matching ignores case, smart quotes, and whitespace runs, so a phrase
    spanning a stored line break is still found; what comes back is always the
    stored text verbatim — whole lines when a match or window spans several
    (whitespace is the surviving evidence of structure: a needle matching
    across two table cells comes back as two lines, and the boundary renders),
    the matching sentence(s) when a ``context=0`` match sits within one.

    A hit never leaves the section(s) its match touches — the name is the
    match's own, and padding clips to the section's bounds — so ``context``
    changes how much you see without changing where the evidence is said to
    live, and any span lifted out of a hit can carry that hit's locator. The
    page numbers, by contrast, describe the whole shown window; see
    ``_render_hit`` for why the two rules differ.

    The heading is only as good as the page's own markup. A page-builder site
    whose tab labels are real ``<h2>``s yields locators like ``$7,995``;
    faithful to the document, and no more wrong than the outline it comes from.
    """
    rec = get(url, con=con)
    if not rec or not rec.get("text"):
        return []
    doc = _doc_of(rec)
    needle_norm = _match_norm(needle)
    if not needle_norm:
        # The empty string is at every position, and a scan advancing by its
        # length would never advance. No hits, same as _section_miss_hint's
        # treatment of the identical degenerate input.
        return []
    # The frontmatter is this cache's own assembly, not page text, so its two
    # sides are scanned as separate ranges: the delimiter lines fall outside
    # both (a reader never sees them as content, same rule as \f markers, and
    # no match can sit on a line the metadata section's bounds exclude), and a
    # span joining metadata to body — our synthetic text reading as the
    # document's, the splice hazard in one more costume — is never a
    # candidate. Never-a-candidate matters: a post-scan reject would already
    # have consumed the scan position, hiding a valid body match that
    # overlaps the rejected one.
    if doc.fm_close is None:
        spans = _find_spans(doc.lines, needle_norm)
    else:
        spans = [
            s._replace(first=s.first + start, last=s.last + start)
            for start, segment in (
                (1, doc.lines[1 : doc.fm_close]),
                (doc.fm_close + 1, doc.lines[doc.fm_close + 1 :]),
            )
            for s in _find_spans(segment, needle_norm)
        ]
    # One path for every context: find spans, widen by context (possibly
    # zero), narrow to sentences only at context<=0 on a one-line match —
    # keyed on the context *setting*, never on whether a widened result
    # happens to occupy one line, which would silently sentence-clip a
    # --context call on a one-line document.
    hits: list[_Hit] = []
    for span in spans:
        secs = _match_sections(doc, span.first, span.last)
        section_ids = tuple(s.id for s in secs)
        if context <= 0 and span.first == span.last:
            extent = _sentence_extent(
                sentences(doc.lines[span.first]), span.start_off, span.end_off
            )
            hit = _Hit(span.first, span.first + 1, span.first, section_ids, extent)
        else:
            # Both the clip and the tuple check keep a hit's label true of
            # every word in it: unclipped padding spills across a heading, and
            # windows either side of one can abut exactly, so a merged pair
            # would hold evidence from two sections under a single name. Near
            # an edge this yields less than ±N.
            hit = _Hit(
                max(secs[0].start, span.first - max(context, 0)),
                min(secs[-1].end, span.last + 1 + max(context, 0)),
                span.first,
                section_ids,
                None,
            )
        last = hits[-1] if hits else None
        if last is None:
            hits.append(hit)
        elif context > 0:
            if hit.start <= last.end and hit.section_ids == last.section_ids:
                hits[-1] = last._replace(end=max(last.end, hit.end))
            else:
                hits.append(hit)
        elif hit != last:
            # context<=0 never merges, but two occurrences resolving to the
            # same extent — twice inside one sentence — are one hit; two
            # matching sentences on one line stay two (distinct extents).
            hits.append(hit)
    return [_render_hit(doc, h) for h in hits]


def quote(
    url: str,
    needle: str,
    *,
    context: int = 0,
    con: sqlite3.Connection | None = None,
) -> list[str]:
    """Text in a page containing ``needle``, document order.

    Matching ignores case, smart quotes, and whitespace runs — a needle typed
    off a rendered page finds the stored text even across a line break or a
    curly apostrophe — while the returned text is always the stored lines
    verbatim, so every hit still passes the quote gate.

    With ``context=0`` (the default): the matching sentence(s), one hit per
    match — the starting point for a verbatim ``cite.quote`` in a data patch.
    The author still confirms wording against the stored raw blob before
    shipping. A match spanning several lines returns those lines whole.

    With ``context=N``: each hit widened to ±N surrounding lines, so confirming
    a span's surroundings rarely needs the whole page. Overlapping windows
    merge and duplicate matches collapse; results stay in document order — no
    reordering toward "better-looking" matches, which would conflict with the
    windows and buy skim-comfort at the cost of a confusing contract.

    Just the spans. ``quote_hits()`` is the same read with each hit's enclosing
    heading and PDF page numbers attached; this stays the plain-text form
    because it is what the quote gate consumes — a verbatim span and nothing
    to strip back off.
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

    **A paginated document is mapped by page instead.** A PDF's text has no
    headings to tree, so pages are the only division it has: one ``level=0``
    row per sheet, named ``page <N>``, counted the same way, and ``section()``
    takes that name back. That is what lets a long PDF be read a piece at a
    time — the 206KB Houdini manual against a schema, sheet by sheet, instead
    of one whole-document read. It is a cruder division than headings (a
    credits list can straddle a sheet), and it is the only one available.

    **An unpaginated text layer is followed by the OCR page map.** Whenever
    ``text`` carries no markers of its own — a fully dark row, or a hand-typed
    transcription of a scanned PDF — the machine-read page map is what defines
    ``page N`` (see ``_page_doc``), so it must appear here or this map and
    ``section()``/``search`` would speak different address vocabularies. Its
    rows are ``tier='ocr'``: what the sheets hold is ink, verified by
    rendering, but where the sheets are is a fact the map can state. On a
    transcription the heading rows still lead — they map the citable text,
    which is this read's first job — with the sheet rows appended after. A
    page-shaped heading in the transcription (``# Page 1``) is withheld: the
    map that owns the namespace reserves it, as on a text-paginated document,
    and listing a name that resolves to different text would be a false map.
    """
    rec = get(url, con=con)
    if not rec:
        return []
    if not rec.get("text"):
        # No citable layer at all: the OCR page map, or nothing.
        return _ocr_page_entries(rec)
    doc = _doc_of(rec)
    if doc.page_starts:
        return [
            OutlineEntry(
                level=0,
                heading=f"page {n}",
                chars=len(_page_block(doc, n) or ""),
                count=1,
                tier=_TEXT_TIER,
            )
            for n in range(1, len(doc.page_starts) + 1)
        ]
    entries: list[OutlineEntry] = []
    meta_block, body_block = _metadata_block(doc), _body_block(doc)
    if meta_block is not None:
        entries.append(
            OutlineEntry(
                level=0, heading="metadata", chars=len(meta_block), count=1,
                tier=_TEXT_TIER,
            )
        )
    if body_block is not None:
        entries.append(
            OutlineEntry(
                level=0, heading="body", chars=len(body_block), count=1,
                tier=_TEXT_TIER,
            )
        )
    # The text layer has no markers (or this branch wouldn't run), so if the
    # OCR tier has a page map it is the one defining `page N` — it must be
    # listed below, or the addresses section()/search answer to would be
    # missing from the map. Computed before the heading loop because owning
    # the namespace also *reserves* it there.
    ocr_entries = _ocr_page_entries(rec)
    # Collapse repeats of a name in the same place in the tree, held at first
    # appearance so the tree still reads top-down. Popping the closed ancestors
    # first leaves `stack` as this heading's own path, which is the key.
    seen: dict[_TreePath, int] = {}
    stack: list[_Heading] = []
    for k, h in enumerate(doc.headings):
        while stack and stack[-1].level >= h.level:
            stack.pop()
        stack.append(h)
        if ocr_entries and _PAGE_NAME.match(h.text.strip().casefold()):
            # A page-shaped heading in the transcription ("# Page 1") names
            # something `section()` will never resolve to it — the page
            # namespace is reserved for the map that owns it, exactly as on a
            # text-paginated document, where such headings are deliberately
            # unaddressable and never listed. Printing the row would show an
            # address that resolves to different text. It stays on the stack:
            # its children are real sections and keep their tree identity.
            continue
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
            OutlineEntry(
                level=h.level, heading=h.text, chars=chars, count=1, tier=_TEXT_TIER
            )
        )
    entries.extend(ocr_entries)
    return entries


def _ocr_page_entries(rec: PageRow) -> list[OutlineEntry]:
    """The OCR tier's page map as outline rows, or none when it has no markers
    (an image's OCR, or no OCR at all)."""
    ocr_doc = _doc_of(rec, _OCR_TIER)
    return [
        OutlineEntry(
            level=0,
            heading=f"page {n}",
            chars=len(_page_block(ocr_doc, n) or ""),
            count=1,
            tier=_OCR_TIER,
        )
        for n in range(1, len(ocr_doc.page_starts) + 1)
    ]


def section(
    url: str, heading: str, con: sqlite3.Connection | None = None
) -> list[SectionBlock]:
    """The block(s) under a heading: from its line to the next heading at the
    same or a higher level, heading line included — so a session that navigated
    here can cite "in the <heading> section" for free. Case-insensitive exact
    match on the heading text; if it matches more than once, every matching
    block is returned — ambiguity should surface, not silently pick one. On an
    assembled page two pseudo-sections are addressable too: ``"metadata"`` (the
    frontmatter's ``key: value`` lines) and ``"body"`` (everything after it).
    Each block carries its ``tier``: ``text`` for everything above, ``ocr``
    only on the dark-document page path below.

    On a paginated document ``"page <N>"`` names one PDF sheet — the name
    ``outline()`` prints and the axis ``quote`` reports, so walking a manual is
    ``outline`` then ``section`` exactly as it is on an HTML page. **The page
    number is the sheet's position in the file, never a number printed on it**:
    printed numbering restarts per chapter (``4-39``, ``6-76``), is often
    absent, and is not recoverable from the text. **On a paginated document the
    page namespace is reserved**: ``page <N>`` is that sheet and nothing else,
    unlike ``metadata``/``body``, which coexist with a same-named heading. The
    two differ because those name the same kind of thing a heading does, where
    returning both is the honest answer to an ambiguous name, while a sheet and
    a heading are different kinds — and a heading block runs to the next
    heading at its level, so it can span several sheets. Composing them would
    answer "give me sheet 2" with text from sheets 3 and 4, and let a
    ``page 99`` that no sheet answers look like a hit because the extractor
    misparsed a ``# page 99`` line somewhere. Non-page names still match
    headings on a PDF, which is what keeps a ``quote`` hit's ``[label]``
    resolvable here.

    **A dark document answers ``page <N>`` from its OCR page map**, each block
    ``tier='ocr'``: what comes back is machine-read ink, verified by rendering
    the sheet, never quoted. Text stays primary — the OCR map defines the page
    axis only when the text layer has no markers of its own.

    A sheet with no extracted text comes back ``[]``, like a name that is
    absent — the return is blocks of text, and such a sheet has none. The two
    are told apart by ``_section_miss_hint``, which is where every other reason
    for an empty result is already explained, and by ``outline()``, which lists
    such a sheet as a row with 0 chars."""
    rec = get(url, con=con)
    if not rec:
        return []
    target = heading.strip().casefold()
    page_match = _PAGE_NAME.match(target)
    if page_match is not None:
        # Truthiness, not `is not None`: an out-of-range sheet and one that
        # yielded no text both give no block, and neither belongs in a list of
        # text blocks. On a document where *neither* column is paginated,
        # `page 2` reserves nothing and falls through, so a page with a
        # heading by that name still answers to it.
        page_doc, page_tier = _page_doc(rec)
        if page_doc.page_starts:
            page_block = _page_block(page_doc, int(page_match.group(1)))
            return (
                [SectionBlock(text=page_block, tier=page_tier)] if page_block else []
            )
    if not rec.get("text"):
        return []
    doc = _doc_of(rec)
    blocks: list[SectionBlock] = []
    pseudo = {"metadata": _metadata_block, "body": _body_block}.get(target)
    if pseudo is not None:
        block = pseudo(doc)
        if block is not None:
            blocks.append(SectionBlock(text=block, tier=_TEXT_TIER))
    blocks.extend(
        SectionBlock(text=_heading_block(doc, k), tier=_TEXT_TIER)
        for k, h in enumerate(doc.headings)
        if h.text.casefold() == target
    )
    return blocks


# --------------------------------------------------------------------------- #
# Search scopes: global, document, section
# --------------------------------------------------------------------------- #

# Marks each match for counting and slicing, so these must be characters no
# cached text contains. Measured: `[`/`]` are in a third of documents, and
# \x03 is the Elvis manual's space character. Escapes, never literals — a
# private-use literal is invisible in an editor and survives no copy-paste,
# and an empty marker would match everywhere, hence the guard.
_MARK_OPEN = "\ue000"
_MARK_CLOSE = "\ue001"
if len(_MARK_OPEN) != 1 or len(_MARK_CLOSE) != 1:  # pragma: no cover - import guard
    raise AssertionError("highlight markers must be exactly one character each")

# Which text layer a hit's counts and windows describe. `text` is the citable
# layer quotes verify against; `ocr` is machine-read ink — findable, verified
# by rendering the sheet, never by quoting. The tier says where the words came
# from, not which reading is more accurate: on the mojibake manuals the text
# layer is a cipher and the OCR is the faithful reading.
_TEXT_TIER = "text"
_OCR_TIER = "ocr"

# The pages column each tier reads. The one mapping, so the roundtrip check,
# the document parse, and the FTS joins cannot disagree about what a tier is.
_TIER_COLUMN = {_TEXT_TIER: "text", _OCR_TIER: "ocr_text"}


def _missing_ocr_schema(exc: sqlite3.OperationalError) -> bool:
    """Whether ``exc`` is the one failure the OCR tier tolerates: a cache from
    before the tier existed, read through a read-only connection that can't
    migrate it (the first writable open adds the table and column).

    Deliberately this narrow. The tier queries must not swallow
    ``OperationalError`` wholesale — a malformed FTS index or a SQL regression
    would then degrade into an empty result, and in this system "no matches"
    reads as a considered answer about the corpus, not as a failure."""
    message = str(exc)
    return "no such table: ocr_fts" in message or (
        "no such column" in message and "ocr_text" in message
    )

# Shared by the CLI and the library so a scripted read and a typed one agree.
_DEFAULT_SURROUNDING_WORDS = 30

# Names text above the first heading, and a document with no headings at all.
# An address `--section` takes back. Not `body`: that addresses the whole body
# including every headed section, answering a three-match question with 60K.
NO_HEADING = "(no heading)"


class MarkerCollisionError(RuntimeError):
    """A document's own text contains a highlight marker, so it cannot be
    counted or sliced. The markers were chosen against a corpus that keeps
    growing, so this is possible; the global scope catches it per hit rather
    than losing the whole result set."""


class SectionHit(TypedDict):
    """One row of the document scope."""

    section: str | None  # None is the unheaded region; printed as NO_HEADING
    matches: int
    tier: str


class MatchHit(TypedDict):
    """One window from the section scope; ``text`` is stored lines verbatim."""

    text: str
    # The window's address, or None when no single one is true of it. Which of
    # the two reasons applies is `straddles` — a paginated document has no
    # unheaded region, so calling a cross-sheet match "(no heading)" would be a
    # false locator rather than a vague one.
    section: str | None
    straddles: bool  # the match itself runs past its section's end
    matches: int  # matches inside this window; >1 where neighbours merged
    tier: str


class _Region(NamedTuple):
    """The unit all three scopes count and address."""

    name: str | None
    start: int
    end: int  # one past its last line


def _row_minus(row: sqlite3.Row, *computed: str) -> PageRow:
    """A result row as a ``PageRow``, less the columns the query computed.

    ``SELECT p.*`` so a column added to ``pages`` needs no change here."""
    fields = dict(row)
    for name in computed:
        del fields[name]
    return cast("PageRow", fields)


class _MatchSpan(NamedTuple):
    """One match's line extent: where its highlight opens, and where it closes.

    Two line indexes rather than one because a match need not sit on a single
    line: ``unicode61`` reads a newline as a separator, so ``upper\\nmagnet``
    holds the adjacent tokens the phrase ``"upper magnet"`` asks for. A PDF's
    text layer wraps constantly, which makes that the ordinary case. Anything
    reading only ``first`` returns half a match.

    The two differing also means a match can straddle a section boundary — the
    same thing ``_Span`` records for ``quote``, and it gets ``quote``'s answer:
    no single section name is true of such a hit.
    """

    first: int
    last: int


def _match_spans(
    rec: PageRow, highlighted: str, tier: str = _TEXT_TIER
) -> list[_MatchSpan]:
    """Every match's line extent in a row's highlighted tier column.

    ``highlight()`` inserts markers instead of eliding like ``snippet()``, so
    line i here is line i of the stored text — which is what lets a match be
    placed by line index alone. The roundtrip check proves that per document,
    against the tier's own column."""
    stripped = highlighted.replace(_MARK_OPEN, "").replace(_MARK_CLOSE, "")
    if stripped != (rec.get(_TIER_COLUMN[tier]) or ""):
        raise MarkerCollisionError(rec["url"])
    opens: list[int] = []
    closes: list[int] = []
    for i, line in enumerate(highlighted.split("\n")):
        opens += [i] * line.count(_MARK_OPEN)
        closes += [i] * line.count(_MARK_CLOSE)
    # FTS5 marks non-overlapping regions in document order, so the k-th opening
    # marker is closed by the k-th closing one. ``strict`` turns any violation
    # of that into an error rather than a quietly mispaired span.
    return [_MatchSpan(o, c) for o, c in zip(opens, closes, strict=True)]


def _region_of(doc: _Doc, line_idx: int) -> _Region:
    """The region a line sits in, named the way ``outline()`` names it.

    Deliberately the same division, so every name this returns is one
    ``section()`` accepts. The cost is that an unpaginated PDF is carved up by
    whatever ATX lines the extractor misparsed out of its parts list; better
    names here would be addresses the next rung cannot resolve."""
    if doc.page_starts:
        n = bisect_right(doc.page_starts, line_idx)
        end = doc.page_starts[n] if n < len(doc.page_starts) else len(doc.lines)
        return _Region(f"page {n}", doc.page_starts[n - 1], end)
    sec = _enclosing_section(doc, line_idx)
    return _Region(sec.name, sec.start, sec.end)


def _matched_regions(
    doc: _Doc, spans: list[_MatchSpan]
) -> list[tuple[_Region, list[_MatchSpan]]]:
    """Every region holding at least one match, in document order, with them.

    A match belongs to the region it **starts** in, so one straddling a boundary
    is counted once and the section counts still sum to the document total.
    Regions are monotonic in the line index, so this needs no grouping table."""
    found: list[tuple[_Region, list[_MatchSpan]]] = []
    for span in spans:
        region = _region_of(doc, span.first)
        if found and found[-1][0] == region:
            found[-1][1].append(span)
        else:
            found.append((region, [span]))
    return found


# One tier's scoped highlight query: the same index the global scope ranks,
# filtered to one document, so all scopes count a term identically.
_SCOPED_SQL = {
    _TEXT_TIER: """
        SELECT p.*, highlight(pages_fts, 2, ?, ?) AS hl
        FROM pages_fts
        JOIN pages p ON p.rowid = pages_fts.rowid
        WHERE pages_fts MATCH ? AND p.url = ?
    """,
    _OCR_TIER: """
        SELECT p.*, highlight(ocr_fts, 0, ?, ?) AS hl
        FROM ocr_fts
        JOIN pages p ON p.rowid = ocr_fts.rowid
        WHERE ocr_fts MATCH ? AND p.url = ?
    """,
}


def _scoped(
    url: str, term: str, con: sqlite3.Connection | None = None
) -> tuple[PageRow, dict[str, list[_MatchSpan]]] | None:
    """One document's row and its match spans per tier, or None if no tier
    matches.

    Each tier filters its own index — the very one the global scope ranks it
    by. ``highlight()`` over a NULL text column is NULL, which is no spans
    rather than an error (a url/title-only match); a tier whose index holds no
    match for this document simply contributes no key."""
    query = _fts_query(term)
    if not query:
        return None
    own = con is None
    con = con or connect(read_only=True)
    try:
        rec: PageRow | None = None
        spans: dict[str, list[_MatchSpan]] = {}
        for tier, sql in _SCOPED_SQL.items():
            try:
                row = con.execute(
                    sql, (_MARK_OPEN, _MARK_CLOSE, query, normalize_url(url))
                ).fetchone()
            except sqlite3.OperationalError as exc:
                # Only a pre-OCR cache, and only for the tier it can't hold —
                # the text tier answers regardless. Anything else raises: see
                # _missing_ocr_schema.
                if tier == _OCR_TIER and _missing_ocr_schema(exc):
                    continue
                raise
            if row is None:
                continue
            rec = _row_minus(row, "hl")
            if row["hl"] is not None:
                spans[tier] = _match_spans(rec, row["hl"], tier)
    finally:
        if own:
            con.close()
    if rec is None:
        return None
    return rec, spans


# One tier's global-scope ranking query. Two independent bm25 spaces — see the
# ocr_fts comment in _SCHEMA for why the tiers must not share a table.
_SEARCH_SQL = {
    _TEXT_TIER: """
        SELECT p.*, bm25(pages_fts) AS score,
               snippet(pages_fts, 2, '[', ']', ' … ', 12) AS snippet,
               highlight(pages_fts, 2, ?, ?) AS hl
        FROM pages_fts
        JOIN pages p ON p.rowid = pages_fts.rowid
        WHERE pages_fts MATCH ?
        ORDER BY bm25(pages_fts)
        LIMIT ?
    """,
    _OCR_TIER: """
        SELECT p.*, bm25(ocr_fts) AS score,
               snippet(ocr_fts, 0, '[', ']', ' … ', 12) AS snippet,
               highlight(ocr_fts, 0, ?, ?) AS hl
        FROM ocr_fts
        JOIN pages p ON p.rowid = ocr_fts.rowid
        WHERE ocr_fts MATCH ?
        ORDER BY bm25(ocr_fts)
        LIMIT ?
    """,
}


# The backfill read for a document one tier ranked and the other didn't: the
# scoped highlight plus a snippet, so the merged row is whole either way.
_BACKFILL_SQL = {
    _TEXT_TIER: """
        SELECT snippet(pages_fts, 2, '[', ']', ' … ', 12) AS snippet,
               highlight(pages_fts, 2, ?, ?) AS hl
        FROM pages_fts
        JOIN pages p ON p.rowid = pages_fts.rowid
        WHERE pages_fts MATCH ? AND p.url = ?
    """,
    _OCR_TIER: """
        SELECT snippet(ocr_fts, 0, '[', ']', ' … ', 12) AS snippet,
               highlight(ocr_fts, 0, ?, ?) AS hl
        FROM ocr_fts
        JOIN pages p ON p.rowid = ocr_fts.rowid
        WHERE ocr_fts MATCH ? AND p.url = ?
    """,
}


class _TierData(NamedTuple):
    """One tier's answer about one document, before counting."""

    score: float | None  # bm25; None when this tier was backfilled, not ranked
    snippet: str | None
    hl: str | None


def _tier_counts(
    rec: PageRow, data: _TierData | None, tier: str
) -> tuple[int | None, int | None]:
    """(matches, sections) for one tier of one document. None/None on a marker
    collision — that one tier of that one document, never the result set."""
    if data is None or data.hl is None:
        return 0, 0
    try:
        spans = _match_spans(rec, data.hl, tier)
    except MarkerCollisionError:
        return None, None
    regions = _matched_regions(_doc_of(rec, tier), spans)
    # Distinct names: two blocks sharing a heading are one address.
    return len(spans), len({region.name for region, _ in regions})


def search(
    term: str, limit: int = 20, con: sqlite3.Connection | None = None
) -> list[SearchHit]:
    """BM25-ranked documents matching ``term`` — the global scope, both tiers.

    Units AND together; a double-quoted run is one phrase
    (``'"upper magnet" knocker'``). Returns ``SearchHit`` dicts, best match
    first — or none at all when ``term`` holds nothing searchable. ``limit <= 0``
    returns every hit.

    Each tier ranks in its own index and the results merge to one row per
    document, ordered by its better (lower) raw bm25 score; each tier is
    over-fetched at twice the limit, which loses nothing — a document in
    neither tier's top can't be in the merged top. Comparing raw scores across
    two indexes is knowingly imperfect (different corpora, lengths, and
    columns), and stays anyway: any fusion rule is arbitrary, and rank-based
    merging would be systematically worse — it grants "best of each tier"
    parity, vaulting a tier's single trivial hit over another's many strong
    ones — while raw scores at least carry real per-document signal across the
    boundary. The ordering is soft triage; the per-tier counts riding every
    row are the decision signal, the same division SearchScopes.md draws for
    url/title matches. A document selected through one tier still reports the
    other tier's counts (backfilled with a scoped query), so the row's
    asymmetry is always real. The snippet comes from the citable tier when it
    has matches, else from the OCR tier, labelled so.

    The match counts are the triage signal a snippet cannot carry: they say
    whether the snippet shown is representative or one of hundreds. Counting
    costs a whole-column read and a parse per hit, so it scales with ``limit``,
    never with the corpus."""
    query = _fts_query(term)
    if not query:
        return []
    own = con is None
    con = con or connect(read_only=True)
    try:
        # SQLite reads a negative LIMIT as no limit, which is the shape
        # `--limit 0` asks for at every scope.
        fetch = limit * 2 if limit > 0 else -1
        recs: dict[NormalizedUrl, PageRow] = {}
        tiers: dict[NormalizedUrl, dict[str, _TierData]] = {}
        arrival: dict[NormalizedUrl, int] = {}  # ranked-order tiebreak
        for tier, sql in _SEARCH_SQL.items():
            try:
                rows = con.execute(
                    sql, (_MARK_OPEN, _MARK_CLOSE, query, fetch)
                ).fetchall()
            except sqlite3.OperationalError as exc:
                # Only a pre-OCR cache, and only for the tier it can't hold —
                # the text tier answers regardless. Anything else raises: see
                # _missing_ocr_schema.
                if tier == _OCR_TIER and _missing_ocr_schema(exc):
                    continue
                raise
            for row in rows:
                rec = _row_minus(row, "score", "snippet", "hl")
                url = rec["url"]
                recs[url] = rec
                arrival.setdefault(url, len(arrival))
                tiers.setdefault(url, {})[tier] = _TierData(
                    row["score"], row["snippet"], row["hl"]
                )
        ranked = sorted(
            recs,
            key=lambda url: (
                min(
                    data.score
                    for data in tiers[url].values()
                    if data.score is not None
                ),
                arrival[url],
            ),
        )
        if limit > 0:
            ranked = ranked[:limit]
        # A document one tier ranked may still hold the other tier's matches
        # past that tier's over-fetch cutoff; backfill so its row is whole.
        for url in ranked:
            for tier in _SEARCH_SQL:
                if tier in tiers[url]:
                    continue
                try:
                    row = con.execute(
                        _BACKFILL_SQL[tier], (_MARK_OPEN, _MARK_CLOSE, query, url)
                    ).fetchone()
                except sqlite3.OperationalError as exc:
                    if tier == _OCR_TIER and _missing_ocr_schema(exc):
                        continue  # pre-OCR cache, as above
                    raise
                if row is not None:
                    tiers[url][tier] = _TierData(None, row["snippet"], row["hl"])
    finally:
        if own:
            con.close()

    hits: list[SearchHit] = []
    for url in ranked:
        rec = recs[url]
        text_data = tiers[url].get(_TEXT_TIER)
        ocr_data = tiers[url].get(_OCR_TIER)
        matches, sections = _tier_counts(rec, text_data, _TEXT_TIER)
        ocr_matches, ocr_sections = _tier_counts(rec, ocr_data, _OCR_TIER)
        # The citable tier's snippet leads whenever it has something to show —
        # a reader trained onto OCR snippets is being trained off the one habit
        # keeping machine readings out of the catalog.
        if text_data is not None and (matches is None or matches > 0):
            snippet, snippet_tier = text_data.snippet, _TEXT_TIER
        elif ocr_data is not None and ocr_data.snippet is not None:
            snippet, snippet_tier = ocr_data.snippet, _OCR_TIER
        elif text_data is not None:
            snippet, snippet_tier = text_data.snippet, _TEXT_TIER
        else:
            snippet, snippet_tier = None, _TEXT_TIER
        hits.append(
            SearchHit(
                url=url,
                title=rec["title"],
                last_updated=rec["last_updated"],
                content_type=rec["content_type"],
                text_source=rec["text_source"],
                snippet=snippet,
                snippet_tier=snippet_tier,
                matches=matches,
                sections=sections,
                ocr_matches=ocr_matches,
                ocr_sections=ocr_sections,
                has_text=bool((rec["text"] or "").strip()),
                has_ocr=bool((rec.get("ocr_text") or "").strip()),
            )
        )
    return hits


def _sheet_ordinal(name: str | None) -> int | None:
    """A section name's sheet number, or None when it isn't a page address."""
    match = _PAGE_NAME.match(name or "")
    return int(match.group(1)) if match else None


def _interleave[T](rows: list[tuple[str | None, int, int, T]]) -> list[T]:
    """Order two tiers' rows as one list: by sheet where both speak in sheets.

    ``rows`` is ``(section_name, tier_index, doc_order, item)``. When every name
    is a page address — the case where two tiers genuinely share an axis, since
    the write-time sheet-count assertion makes their ordinals mean the same
    sheet — rows sort by sheet, text tier first within one. Any other mix has
    no shared axis to merge on, so tiers keep their own document order, text
    first. A single-tier list is unchanged either way."""
    if all(_sheet_ordinal(name) is not None for name, _, _, _ in rows):
        ordered = sorted(rows, key=lambda r: (_sheet_ordinal(r[0]), r[1], r[2]))
    else:
        ordered = sorted(rows, key=lambda r: (r[1], r[2]))
    return [item for _, _, _, item in ordered]


def search_sections(
    url: str, term: str, con: sqlite3.Connection | None = None
) -> list[SectionHit]:
    """The sections of one document that match ``term``, both tiers interleaved
    in sheet order.

    Unranked, because ranking would need BM25 at section grain and HTML leaf
    blocks are short enough that nav fragments would win. One row per **name
    per tier**, summing that name's blocks — a sheet matching in both layers is
    two rows, and the asymmetry between their counts is the thing worth
    reading. Raises ``MarkerCollisionError``."""
    scoped = _scoped(url, term, con=con)
    if scoped is None:
        return []
    rec, spans_by_tier = scoped
    rows: list[tuple[str | None, int, int, SectionHit]] = []
    for tier_idx, tier in enumerate((_TEXT_TIER, _OCR_TIER)):
        spans = spans_by_tier.get(tier)
        if not spans:
            continue
        totals: dict[str | None, int] = {}
        for region, region_spans in _matched_regions(_doc_of(rec, tier), spans):
            totals[region.name] = totals.get(region.name, 0) + len(region_spans)
        # dict order is insertion order, and regions arrive in document order.
        rows.extend(
            (name, tier_idx, order, SectionHit(section=name, matches=n, tier=tier))
            for order, (name, n) in enumerate(totals.items())
        )
    return _interleave(rows)


class _Window(NamedTuple):
    """One shown span: its line range, its section, and the matches inside it.

    ``section`` rides the window because a straddling match has none, and such a
    window must not merge with a labelled neighbour it happens to abut."""

    start: int
    end: int  # one past its last line
    matches: int
    section: str | None
    straddles: bool


def _window_extent(
    doc: _Doc, opens_in: _Region, closes_in: _Region, span: _MatchSpan, words: int
) -> tuple[int, int]:
    """The line range shown around a match: sized in words, clipped to its region.

    Only the padding is negotiable — clipping the match itself would return a
    span missing the words it was found for. Padding stops at the region so a
    window can carry its hit's locator, as ``quote --context`` does.

    Words, not lines, because PDF lines are short and irregular; whole lines
    still come out, keeping table rows intact. ``words`` caps lines either side
    too, or a run of blank lines (no words, so free to cross) would merge two
    distant matches into one window of whitespace."""
    start, end = span.first, span.last
    before = after = 0
    while start > opens_in.start and before < words and span.first - start < words:
        start -= 1
        before += len(doc.lines[start].split())
    while end + 1 < closes_in.end and after < words and end - span.last < words:
        end += 1
        after += len(doc.lines[end].split())
    return start, end + 1


def _window_text(doc: _Doc, start: int, end: int) -> str:
    """A window's lines as stored text, verbatim.

    ``\\f`` markers and blank edge lines drop; nothing else is touched and
    nothing is marked, so any part of the result can be lifted into a cite's
    ``quote``. Brackets would cost that, and the corpus is full of its own."""
    shown = [i for i in range(start, end) if doc.lines[i] != "\f"]
    while shown and not doc.lines[shown[0]].strip():
        shown.pop(0)
    while shown and not doc.lines[shown[-1]].strip():
        shown.pop()
    return "\n".join(doc.lines[i] for i in shown)


def _addresses(
    region: _Region, section: str | None, pages: tuple[int, int] | None
) -> bool:
    """Whether a region answers to the address asked for — no address, all of them.

    ``section`` arrives casefolded, matching ``section()``. A sheet range tests
    the region's name rather than a separate ordinal, so an unpaginated document
    yields nothing instead of hits under numbers that mean nothing."""
    if pages is not None:
        match = _PAGE_NAME.match(region.name or "")
        return match is not None and pages[0] <= int(match.group(1)) <= pages[1]
    if section is None:
        return True
    if region.name is None:
        return section == NO_HEADING.casefold()
    return region.name.casefold() == section


def search_matches(
    url: str,
    term: str,
    *,
    section: str | None = None,
    pages: tuple[int, int] | None = None,
    surrounding_words: int = _DEFAULT_SURROUNDING_WORDS,
    con: sqlite3.Connection | None = None,
) -> list[MatchHit]:
    """Each match in a document, with surrounding words — the section scope.

    ``section`` is a name ``search_sections()`` returned (``"(no heading)"`` for
    the unheaded region); ``pages`` is an inclusive sheet range. Two names for
    one thing, so pass at most one; with neither, every match comes back.

    Both tiers answer, interleaved in sheet order like ``search_sections``; a
    window's ``tier`` says whether its text is citable (``text``) or machine-read
    ink to verify by rendering the sheet (``ocr``). Overlapping windows merge
    within their tier and report how many matches they absorbed. Every window
    is returned — capping is the caller's job, so it can say what it withheld.
    Raises ``MarkerCollisionError``."""
    if section is not None and pages is not None:
        raise ValueError("section and pages name the same thing; pass at most one")
    scoped = _scoped(url, term, con=con)
    if scoped is None:
        return []
    rec, spans_by_tier = scoped
    target = None if section is None else section.strip().casefold()
    words = max(surrounding_words, 0)
    rows: list[tuple[str | None, int, int, MatchHit]] = []
    for tier_idx, tier in enumerate((_TEXT_TIER, _OCR_TIER)):
        spans = spans_by_tier.get(tier)
        if not spans:
            continue
        doc = _doc_of(rec, tier)
        order = 0
        for region, region_spans in _matched_regions(doc, spans):
            if not _addresses(region, target, pages):
                continue
            # Merged as they are laid down: the matches arrive in document
            # order, so an overlap can only ever be with the window just placed.
            windows: list[_Window] = []
            for span in region_spans:
                closes_in = (
                    region if span.last < region.end else _region_of(doc, span.last)
                )
                name = region.name if closes_in == region else None
                start, end = _window_extent(doc, region, closes_in, span, words)
                last = windows[-1] if windows else None
                if last is not None and start <= last.end and last.section == name:
                    windows[-1] = last._replace(
                        end=max(last.end, end),
                        matches=last.matches + 1,
                        straddles=last.straddles or closes_in != region,
                    )
                else:
                    windows.append(_Window(start, end, 1, name, closes_in != region))
            for w in windows:
                rows.append(
                    (
                        region.name,
                        tier_idx,
                        order,
                        MatchHit(
                            text=_window_text(doc, w.start, w.end),
                            section=w.section,
                            straddles=w.straddles,
                            matches=w.matches,
                            tier=tier,
                        ),
                    )
                )
                order += 1
    return _interleave(rows)


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


def _blob_line(rec: PageRow) -> str | None:
    """``blob: <path>`` for a row, or None when its type maps to no extension.

    Assembled here because a row carries only ``content_sha``, and reaching the
    file from that also takes the ``raw/<sha>.<ext>`` layout and the
    content-type-to-extension mapping.
    """
    # Function-level: the handler registry is needed nowhere else here, and
    # web_cache's readers import it for sqlite-backed reads alone.
    from content_types import extension_for

    content_type = rec["content_type"]
    ext = extension_for(content_type) if content_type is not None else None
    if ext is None:
        return None
    return f"blob: {blob_path(rec['content_sha'], ext)}"


def _render_handoff_line(rec: PageRow) -> str | None:
    """``_blob_line``, for the commands that name a sheet to render.

    Qualifying is a property of the content type, not of the extraction method:
    a PDF with a reviewed manual transcription renders as well as one poppler
    read, and an image displays whatever its OCR found. An HTML blob is left
    out because the stored markdown is the better read of it — ``get`` calls
    ``_blob_line`` directly, a full-record dump withholding nothing.
    """
    content_type = rec["content_type"] or ""
    if content_type == "application/pdf" or content_type.startswith("image/"):
        return _blob_line(rec)
    return None


def _row_facts(rec: PageRow) -> list[str]:
    """What a reader needs to know about the *document*, hit or miss.

    The render handoff: a PDF hit's payoff step is Read(<blob>, pages=N), so
    say what that call needs — on a miss too, where the wanted words may be
    printed on the sheet as artwork the text layer never held.

    The blob path prints when the row has a blob worth looking at (a property
    of the content type, not the extraction method: a PDF with a reviewed
    manual transcription renders as well as one poppler read; an OCR'd image
    displays too). The page line is keyed on the content type alone — an OCR'd
    JPEG has a renderable blob but telling someone their JPEG has no PDF pages
    is noise — and exists because silence would read as "one page" when this
    row may be a 103-page manual whose text predates the page markers.

    A row with no text at all says so *instead* of the page line: no text
    means no markers, so the page line would be the duller way of saying the
    same thing. What it must not say is which of the two no-text cases this
    is. The extractor keeps them apart — an image-only document is a finding,
    an extraction that was unavailable is no opinion (see ``web_pdftext``'s
    error pair) — but only the first fetch's warning did, and the row records
    neither. Both still mean nothing here can match, which is the part worth
    saying rather than leaving as a bare "no matches". It names the two causes
    only when a blob line went out above it: those causes belong to documents
    that are pictures, and nothing else can act on "read the blob" anyway.
    """
    facts: list[str] = []
    is_pdf = rec["content_type"] == "application/pdf"
    line = _render_handoff_line(rec)
    blob_shown = line is not None
    if line is not None:
        facts.append(line)
    text = rec["text"] or ""
    ocr = rec.get("ocr_text") or ""
    if not text.strip():
        if ocr.strip():
            facts.append(
                "no citable text layer — matches here are OCR (machine-read); "
                "cite by rendering the sheet, never by quoting"
            )
        else:
            facts.append(
                "no stored text, so nothing can match it — the document may be "
                "image-only, or extraction may have been unavailable when it "
                "was fetched; read the blob to find out which"
                if blob_shown
                else "no stored text, so nothing can match it"
            )
    elif is_pdf and not _page_doc(rec)[0].page_starts:
        facts.append("pdf document pages: unavailable")
    if is_pdf and rec.get("ocr_text") is None:
        facts.append(
            "not yet OCR'd: sheet-image content is invisible to search "
            "(scripts/web_scrape/web_pdfocr.py reads it)"
        )
    return facts


def _plural(n: int, one: str, many: str) -> str:
    return f"{n} {one if n == 1 else many}"


def _warn_unbalanced(term: str) -> None:
    """Say what was actually searched when a quote was left open.

    The term is reinterpreted rather than rejected, so the expression that ran
    has to be visible. Showing the whole expression, not "the" open quote's
    phrase: a quote can own part of a unit or none of one."""
    units, unbalanced = _fts_units(term)
    if unbalanced:
        print(
            f"unbalanced quote; searched {_fts_expr(units) or 'nothing'}",
            file=sys.stderr,
        )


def _tier_segment(matches: int | None, sections: int | None, tier: str) -> str:
    """One tier's piece of a count line, tier-labelled."""
    if matches is None:
        return f"count unavailable ({tier}): the text contains a highlight marker"
    return (
        f"{matches} in {_plural(sections or 0, 'section', 'sections')} ({tier})"
    )


def _match_label(hit: SearchHit) -> str:
    """One global-scope hit's count line, a segment per tier that has matches.

    A document without an OCR tier keeps the old single-count shape — a tier
    label on every HTML hit would be noise about a distinction those rows
    don't have. The pages_fts index covers url and title too, so a document
    can match with no text match; the ways that happens differ (no text layer
    at all, a layer that lacks the term, no OCR yet) and `0 in 0 sections` for
    any of them would read as a broken counter."""
    segments: list[str] = []
    if hit["matches"] is None or hit["matches"]:
        segments.append(_tier_segment(hit["matches"], hit["sections"], _TEXT_TIER))
    if hit["ocr_matches"] is None or hit["ocr_matches"]:
        segments.append(
            _tier_segment(hit["ocr_matches"], hit["ocr_sections"], _OCR_TIER)
        )
    if segments:
        if not hit["has_ocr"] and len(segments) == 1 and hit["matches"] is not None:
            # No second tier exists: today's plain shape, unlabelled.
            return (
                f"{hit['matches']} in "
                f"{_plural(hit['sections'] or 0, 'section', 'sections')}"
            )
        return " · ".join(segments)
    if not hit["has_text"]:
        if hit["has_ocr"]:
            return "url/title match, no text layer, 0 ocr matches"
        return "url/title match, no text layer"
    if hit["has_ocr"]:
        return "url/title match, 0 matches in either tier"
    return "url/title match, 0 text matches"


def _ocr_coverage_note(con: sqlite3.Connection | None = None) -> str | None:
    """A stderr line when the corpus holds un-OCR'd PDFs, else None.

    So a thin result set says so rather than implying completeness: an
    un-OCR'd PDF's image-only sheets are invisible to every scope."""
    own = con is None
    con = con or connect(read_only=True)
    try:
        try:
            n = con.execute(
                "SELECT count(*) FROM pages "
                "WHERE content_type = 'application/pdf' AND ocr_text IS NULL"
            ).fetchone()[0]
        except sqlite3.OperationalError as exc:
            if not _missing_ocr_schema(exc):
                raise
            # A pre-OCR cache has no ocr_text column and nothing OCR'd: the
            # whole PDF shelf is un-read, which is what the note should say.
            n = con.execute(
                "SELECT count(*) FROM pages WHERE content_type = 'application/pdf'"
            ).fetchone()[0]
    finally:
        if own:
            con.close()
    if not n:
        return None
    return (
        f"{_plural(n, 'cached PDF is', 'cached PDFs are')} not yet OCR'd — "
        f"their image-only sheets are invisible to this search "
        f"(scripts/web_scrape/web_pdfocr.py reads them)"
    )


def _cmd_search(term: str, limit: int) -> int:
    _warn_unbalanced(term)
    hits = search(term, limit=limit)
    coverage = _ocr_coverage_note()
    if not hits:
        print(f"no pages match: {term}", file=sys.stderr)
        if coverage:
            print(coverage, file=sys.stderr)
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
            # layer, a caption track, or a transcription — so the reader knows
            # to weigh it before quoting.
            print(f"text_source: {hit['text_source']}")
        print(f"matches: {_match_label(hit)}")
        if hit["snippet"]:
            # The snippet spans stored line breaks; collapse for one line. The
            # (ocr) label is the machine-read flag: such a hit is verified by
            # rendering the sheet, and its wording is never quoted.
            label = "snippet (ocr)" if hit["snippet_tier"] == _OCR_TIER else "snippet"
            print(f"{label}: {' '.join(hit['snippet'].split())}")
    if coverage:
        sys.stdout.flush()
        print(coverage, file=sys.stderr)
    return 0


# Aligns a page map's counts. Long headings overflow rather than truncate —
# the name is an address to paste into --section.
_SECTION_COLUMN = 24


# The one-line meaning of an (ocr) marker, printed once per command whose
# output carries any — on stderr, so stdout stays the data.
_OCR_TIER_NOTE = (
    "(ocr) = machine-read from the sheet image; not citable — render the "
    "sheet and cite what you read off it"
)


def _print_sections(sections: list[SectionHit]) -> None:
    for entry in sections:
        name = entry["section"] or NO_HEADING
        count = _plural(entry["matches"], "match", "matches")
        tier = f"  ({entry['tier']})" if entry["tier"] != _TEXT_TIER else ""
        print(f"{name:<{_SECTION_COLUMN}}  {count}{tier}")
    if any(e["tier"] == _OCR_TIER for e in sections):
        sys.stdout.flush()
        print(_OCR_TIER_NOTE, file=sys.stderr)


def _print_matches(hits: list[MatchHit], limit: int) -> None:
    shown = hits if limit <= 0 else hits[:limit]
    for i, hit in enumerate(shown):
        if i:
            print()
        # Not an address, and shaped so nobody pastes it back into --section.
        label = hit["section"] or (
            "section boundary" if hit["straddles"] else NO_HEADING
        )
        if hit["tier"] != _TEXT_TIER:
            label += f" ({hit['tier']})"
        if hit["matches"] > 1:
            # Merged neighbours — say so, or the window count reads as the match
            # count and silently disagrees with the section list.
            label += f"  {_plural(hit['matches'], 'match', 'matches')}"
        print(f"[{label}]")
        print(hit["text"])
    # stdout is block-buffered when redirected while stderr is not, so these
    # would otherwise print above the windows they are qualifying.
    if any(hit["tier"] == _OCR_TIER for hit in shown):
        sys.stdout.flush()
        print(_OCR_TIER_NOTE, file=sys.stderr)
    if len(shown) < len(hits):
        sys.stdout.flush()
        withheld = len(hits) - len(shown)
        noun = "window" if withheld == 1 else "windows"
        print(f"{withheld} more {noun} not shown (--limit 0 for all)", file=sys.stderr)


def _print_address_miss(
    address: str, sections: list[SectionHit], url: NormalizedUrl
) -> None:
    """An address that named nothing, plus where the matches actually are.

    A sheet in range holding none is not an error, so the useful reply names
    what would have worked. Capped, or a long manual recites its whole map."""
    print(f"no matches in {address} of {url}", file=sys.stderr)
    names = [e["section"] or NO_HEADING for e in sections]
    listed = ", ".join(names[:10])
    if len(names) > 10:
        listed += f", … (+{len(names) - 10} more)"
    print(f"sections with matches: {listed}", file=sys.stderr)


def _cmd_search_document(
    url: str,
    term: str,
    *,
    section: str | None,
    pages: tuple[int, int] | None,
    surrounding_words: int,
    limit: int,
) -> int:
    """The document and section scopes: `search TERM --url` and its narrowings."""
    rec = _require_page(url)
    _warn_unbalanced(term)
    for line in _row_facts(rec):
        print(line, file=sys.stderr)
    if pages is not None and not _page_doc(rec)[0].page_starts:
        print(
            "no page markers in this document; --pages names PDF sheets",
            file=sys.stderr,
        )
        return 1
    try:
        sections = search_sections(url, term)
    except MarkerCollisionError:
        print(
            f"cannot count matches in {url}: its own text contains one of the "
            "markers this read depends on",
            file=sys.stderr,
        )
        return 1
    if not sections:
        print(f"no matches for {term!r} in {url}", file=sys.stderr)
        return 1
    addressed = section is not None or pages is not None
    if not addressed and len(sections) > 1:
        # A one-section document would only restate the global count.
        if surrounding_words != _DEFAULT_SURROUNDING_WORDS:
            print(
                f"--surrounding-words {surrounding_words} ignored: it sizes a "
                "match window, and this document needs --section or --pages "
                "first",
                file=sys.stderr,
            )
        _print_sections(sections)
        return 0
    # Cannot raise: search_sections just cleared the same roundtrip check.
    hits = search_matches(
        url, term, section=section, pages=pages, surrounding_words=surrounding_words
    )
    if not hits:
        # Only reachable with an address: matching sections imply windows.
        named = f"pages {pages[0]}-{pages[1]}" if pages else f"section {section!r}"
        _print_address_miss(named, sections, rec["url"])
        return 1
    _print_matches(hits, limit)
    return 0


def _cmd_quote(url: str, needle: str, context: int) -> int:
    rec = _require_page(url)
    hits = quote_hits(url, needle, context=context)
    if not hits:
        print(f"no matches for {needle!r} in {url}", file=sys.stderr)
    # Row facts to stderr, hit facts to stdout — the split `get` makes, so
    # stdout is the hit list and nothing else.
    for line in _row_facts(rec):
        print(line, file=sys.stderr)
    if not hits:
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
        pages = hit["pdf_document_page_numbers"]
        if pages is not None:
            print(f"pdf document pages: {', '.join(str(p) for p in pages)}")
    return 0


def _cmd_outline(url: str, min_chars: int) -> int:
    rec = _require_page(url)
    paginated = bool(_page_doc(rec)[0].page_starts)
    # Before the empty check: a PDF with nothing to map is when going to look
    # at the blob is the only move left.
    handoff = _render_handoff_line(rec)
    if handoff is not None:
        print(handoff, file=sys.stderr)
    entries = outline(url)
    if not entries:
        # Three different absences, and only the last is about structure. A
        # row with no text has nothing to map whatever its type — a fifth of
        # the cached PDFs are image-only scans — and blaming page markers
        # there would send a reader to re-extract a document that has none to
        # find. `quote` draws the same line; these reads must not disagree.
        if not (rec["text"] or "").strip() and not (rec.get("ocr_text") or "").strip():
            print(f"no stored text in {url}", file=sys.stderr)
        elif rec["content_type"] == "application/pdf":
            print(f"no page markers in {url}", file=sys.stderr)
        else:
            print(f"no headings in {url}", file=sys.stderr)
        return 1
    if paginated:
        # No filtering on a page map. Hiding sheets would break the ordinal
        # reading the map exists to provide — page 42 following page 40 asserts
        # a 41 that isn't there — and the thin sheets are often the ones worth
        # rendering. Keyed on the value, not on whether the flag was passed:
        # an explicit `--min-chars 0` asks to see everything, which is what
        # this already shows, so there is nothing withheld to report.
        if min_chars > 0:
            print(
                f"--min-chars {min_chars} ignored: a page map is shown in full",
                file=sys.stderr,
            )
        min_chars = 0
    shown = [e for e in entries if e["chars"] >= min_chars]
    for entry in shown:
        indent = "  " * entry["level"]
        repeat = f"  x{entry['count']}" if entry["count"] > 1 else ""
        tier = f"  ({entry['tier']})" if entry["tier"] != _TEXT_TIER else ""
        print(f"{indent}{entry['heading']}{repeat}  [{entry['chars']} chars]{tier}")
    if any(e["tier"] == _OCR_TIER for e in shown):
        sys.stdout.flush()
        print(_OCR_TIER_NOTE, file=sys.stderr)
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

    A ``page <N>`` name has its own causes, answered first and alone: the row
    holds no text, the document is not paginated, the sheet is past the end, or
    the sheet exists and yielded no text. The last is the reason this matters —
    a seventh of the corpus's mapped sheets are that shape, and a walk over a
    manual must be able to tell "no text on that sheet" from "no such sheet"
    without opening the blob. It says nothing about whether the sheet is empty:
    a full-page scan yields no text and is not blank, so the wording must not
    claim more than the text layer knows. Range is asked of ``_page_block``,
    not recomputed here, so this can never call a page absent that
    ``section()`` would have returned.

    Those answers are given only where ``section()`` read the name as a page —
    a paginated document, or a PDF row that lost its markers. On an ordinary
    HTML page ``page 2`` is just a heading name, matched as one, so the hint
    must stay the heading hint: a forum index headed ``Page 2 of 5`` needs
    "did you mean", not a remark about PDF sheets it was never going to have.
    """
    target = heading.strip().casefold()
    if not target:
        # The empty string is inside every heading — hinting on it would recite
        # the whole outline and say nothing.
        return None
    rec = get(url)
    if rec is None:
        return None
    doc = _doc_of(rec)
    page_match = _PAGE_NAME.match(target)
    if page_match is not None and (
        _page_doc(rec)[0].page_starts or rec["content_type"] == "application/pdf"
    ):
        # The page-defining column (text when it has markers, else the OCR
        # tier) — the same resolution `section()` just used to miss.
        page_doc = _page_doc(rec)[0]
        n = int(page_match.group(1))
        if (
            not (rec["text"] or "").strip()
            and not (rec.get("ocr_text") or "").strip()
        ):
            return "no stored text in this row, so no page holds any"
        if not page_doc.page_starts:
            return "no page markers in this document; `page N` names a PDF sheet"
        if _page_block(page_doc, n) is None:
            return f"out of range; this document has {len(page_doc.page_starts)} pages"
        return f"page {n} has no extracted text; the sheet itself may still hold ink"
    if doc.page_starts:
        # The rows of a page map are sheets, not headings. Offering "did you
        # mean: page 41, page 42, …" for a substring of "page" would recite the
        # map, and there are no headings here for the body-text hint to name.
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
    rec = _require_page(url)
    # A sheet's text is often not the answer — the page hints below say a blank
    # sheet may still hold ink, and naming that sheet without saying where it
    # lives leaves the reader nowhere to go.
    handoff = _render_handoff_line(rec)
    if handoff is not None:
        print(handoff, file=sys.stderr)
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
    if any(b["tier"] == _OCR_TIER for b in blocks):
        # Before the text, unlike the other commands' trailing note: stdout
        # here is often piped straight into a quote draft, and the reader must
        # meet the warning before the ink.
        print(_OCR_TIER_NOTE, file=sys.stderr)
    print("\n\n".join(b["text"] for b in blocks))
    return 0


def _validated_pages(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> tuple[int, int] | None:
    """Check ``search``'s scope flags, and return ``--pages`` as a sheet range.

    Every way of asking for something undeliverable is rejected rather than
    dropped, so a flag never silently narrows the question asked."""
    # `is not None`, never truthiness: an empty value is something somebody
    # typed, most likely an unset shell variable, and `--url ""` reading as
    # "search everything" is a silent scope change.
    if args.url is not None and not args.url.strip():
        parser.error(
            "--url is empty: pass a cached document's URL, or drop "
            "the flag to search every document"
        )
    if args.url is None:
        unusable = [
            flag
            for flag, value in (("--section", args.section), ("--pages", args.pages))
            if value is not None
        ]
        if unusable:
            verb = "need" if len(unusable) > 1 else "needs"
            parser.error(
                f"{' and '.join(unusable)} {verb} --url: a section address is "
                "only meaningful within one document"
            )
        if args.surrounding_words != _DEFAULT_SURROUNDING_WORDS:
            parser.error(
                "--surrounding-words needs --url: it sizes the context around a "
                "match, which only a section scope prints"
            )
    if args.section is not None and args.pages is not None:
        parser.error(
            "--section and --pages are two ways of naming the same thing; pass one"
        )
    if args.pages is None:
        return None
    match = _PAGE_RANGE.match(args.pages.strip())
    if match is None:
        parser.error(
            '--pages takes a sheet range like 40-50 (one sheet is --section "page 41")'
        )
    first, last = int(match.group(1)), int(match.group(2))
    if first > last:
        parser.error(f"--pages {args.pages}: the range runs backwards")
    return first, last


def _read_url_list(path: str) -> list[str]:
    """URLs from a file. Blank and ``#`` lines skipped.

    Reads the same file ``web_fetch.py --from-file`` takes — its
    ``url<TAB>query`` TSV — taking the URL column, so one list drives both:
    check what you hold, then fetch what you don't.
    """
    urls: list[str] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        url = line.partition("\t")[0].strip()
        if url:
            urls.append(url)
    return urls


def _cmd_have(urls: list[str], from_file: str | None) -> int:
    if from_file:
        try:
            urls = urls + _read_url_list(from_file)
        except OSError as exc:
            # A traceback would bury which path was wrong.
            print(f"cannot read {from_file}: {exc.strerror or exc}", file=sys.stderr)
            return 2
    if not urls:
        print("no URLs given (pass URLs or --from-file)", file=sys.stderr)
        return 2
    holdings = have(urls)
    # "No answer" is not "not cached": these entries couldn't be looked up, so
    # they are counted apart rather than tallied as absent, which would read as
    # a considered verdict that the page isn't held.
    invalid = [h for h in holdings if h["error"]]
    missing = [h for h in holdings if h["page"] is None and not h["error"]]
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
        if page.get("ocr_text"):
            facts.append(f"{len(page['ocr_text'] or '')} ocr chars")
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
    # just the document. ocr_text is summarized rather than dumped with the
    # metadata: it is a second document-sized column, not a row fact.
    rec = _require_page(url)
    ocr_chars = len(rec.get("ocr_text") or "")
    for key, value in rec.items():
        if key == "ocr_text":
            if ocr_chars:
                print(
                    f"ocr_text: {ocr_chars:,} chars (machine-read; render the "
                    f"blob to cite)",
                    file=sys.stderr,
                )
            else:
                print(f"ocr_text: {value}", file=sys.stderr)
        elif key != "text":
            print(f"{key}: {value}", file=sys.stderr)
    # Derived, so it follows the stored columns — and printed for every type,
    # since a full-record read withholds nothing.
    line = _blob_line(rec)
    if line is not None:
        print(line, file=sys.stderr)
    if rec["text"]:
        print(rec["text"])
    elif rec.get("ocr_text"):
        # The row's only readable content; a full-record read withholds
        # nothing, and the note above stdout says what this is.
        print("text: (none); printing ocr_text — machine-read, never citable",
              file=sys.stderr)
        print(rec["ocr_text"])
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

    p_search = sub.add_parser(
        "search",
        help="ranked documents matching a term; --url and --section narrow it",
        description=(
            "Three scopes. A term alone ranks documents, each with its match "
            "count. --url lists that document's matching sections with theirs. "
            "--section (or --pages, a sheet range) shows the matches themselves "
            "with surrounding words. A count is matched phrases plus matched "
            "loose words, so '\"camel toes\" bananas' reads two phrase hits and "
            "five bananas as seven; overlapping phrases merge into one, so "
            "'\"a b\" \"b c\"' over 'a b c' counts one."
        ),
    )
    p_search.add_argument(
        "term",
        help="words AND together; a double-quoted run is one phrase. Wrap the "
        "whole term in single quotes so the shell keeps the double ones: "
        "search '\"upper magnet\" knocker'",
    )
    p_search.add_argument(
        "--url", help="scope to one document: its matching sections, with counts"
    )
    p_search.add_argument(
        "--section",
        help="scope to one section (needs --url): its matches, with context. "
        'Takes a name the section list printed, including "(no heading)"',
    )
    p_search.add_argument(
        "--pages",
        help="scope to a PDF sheet range (needs --url), e.g. 40-50. One sheet "
        'is --section "page 41"',
    )
    p_search.add_argument(
        "--surrounding-words",
        type=int,
        default=_DEFAULT_SURROUNDING_WORDS,
        help="words of context either side of each match, clipped to its "
        "section (default %(default)s); whole lines are returned",
    )
    p_search.add_argument(
        "--limit",
        type=int,
        default=20,
        help="documents to rank, or match windows to show once scoped "
        "(default %(default)s; 0 for all)",
    )

    p_quote = sub.add_parser(
        "quote",
        help="text containing a needle (case/whitespace/smart-quote-"
        "insensitive), labelled with its section and, on a PDF, its PDF "
        "page number(s) and blob path",
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

    p_outline = sub.add_parser(
        "outline", help="heading tree with section sizes (a PDF maps by page)"
    )
    p_outline.add_argument("url")
    p_outline.add_argument(
        "--min-chars",
        type=int,
        default=0,
        help="hide headings whose block is under N chars (UI chrome, mostly); "
        "not applied to a page map",
    )

    p_section = sub.add_parser(
        "section", help='one heading\'s block(s), or "page N" on a PDF'
    )
    p_section.add_argument("url")
    p_section.add_argument("heading")

    p_have = sub.add_parser("have", help="which of these URLs are already cached")
    p_have.add_argument("urls", nargs="*", help="URLs to check")
    p_have.add_argument(
        "--from-file", help="file of URLs, one per line (web_fetch's TSV works too)"
    )
    p_get = sub.add_parser("get", help="full page record (text on stdout)")
    p_get.add_argument("url")

    args = parser.parse_args(argv)
    match args.command:
        case "search":
            pages = _validated_pages(parser, args)
            if args.url is None:
                return _cmd_search(args.term, args.limit)
            return _cmd_search_document(
                args.url,
                args.term,
                section=args.section,
                pages=pages,
                surrounding_words=args.surrounding_words,
                limit=args.limit,
            )
        case "quote":
            return _cmd_quote(args.url, args.needle, args.context)
        case "outline":
            return _cmd_outline(args.url, args.min_chars)
        case "section":
            return _cmd_section(args.url, args.heading)
        case "have":
            return _cmd_have(args.urls, args.from_file)
        case "get":
            return _cmd_get(args.url)
    raise AssertionError(f"unhandled command {args.command}")


if __name__ == "__main__":
    sys.exit(main())

# Web Scrape Cache

This project has a searchable, durable, growing cache of fetched web pages used as **evidence** for catalog data.

Most catalog data going forward will be sourced not from IPDB/OPDB but from the web: manufacturer sites, Pinball News, forums, Wikipedia, foreign-language press. We require a verbatim, attributed quote per claim, and we often re-hit the same pages to re-pull those quotes.

This cache fetches each page **once** and reuses it, so we:

- **stop re-hitting sources** — fetch once, reuse forever;
- get **reproducible verbatim quotes** even after a page changes or the site dies (critical for _defunct_ makers, whose sites vanish);
- build a **searchable corpus** of pinball evidence that grows over years;
- capture **provenance** — when we fetched, the search intent that led there, and the page's own publish/modified date.

New catalog data / corrections to existing catalog data are written as curated [data patches](#data-patches).

## Relationship to the main DuckDB

The corpus is read back into the main DuckDB so web evidence can be joined against the IPDB/OPDB/pindata tables already there.

## Architecture

The **SQLite database is the system-of-record**; the main DuckDB is an analytical lens that materializes it during `make explore`. SQLite is the OLTP store (row-by-row upserts, FTS5 full-text search, an archival-stable file format); DuckDB is the OLAP engine for joins against the catalog. DuckDB reads SQLite first-class, so nothing is lost.

```text
ingest_sources/web/          ← durable (R2-backed, gitignored), NOT in git
  cache.sqlite                 system-of-record: pages + fetches + pages_fts (FTS5)
  html/<sha256(raw)>.html      raw page blobs, content-addressed + versioned

scripts/web_scrape/
  web_cache.py               store: schema, URL normalization, upsert,
                             search() / quote() / get()
  web_http.py                transport: GET, charset decode, wire-safe URLs
  web_extract.py             extraction: HTML → title/text/date (PDF later)
  web_render.py              headless-render fallback for JS-only pages
  web_fetch.py               CLI + per-URL orchestration (writes sqlite + html/)

sql/
  03_raw_web.sql             ATTACHes the sqlite, materializes web_pages/web_fetches
                             (raw-ingestion band, alongside 02_raw.sql)
```

The raw HTML blob stays the copy we **re-verify quotes against**; it is kept on disk (not in SQLite) to keep the DB lean and the FTS index fast.

### SQLite schema

Defined in [`web_cache.py`](../scripts/web_scrape/web_cache.py); two tables plus an FTS index:

- **`pages`** — current state per normalized URL: the current version's
  `content_sha` + `html_file`, the extracted `title`/`text`/`last_updated`, and a
  `rendered` flag (1 when the stored blob is a headless-browser render, not the
  bytes the server sent — see [JS-rendered pages](#javascript-rendered-pages)).
- **`fetches`** — append-only audit + version history: one row per fetch, with the
  `search_query` that drove it, the `content_sha` it saw, a `changed` flag, and a
  `rendered` flag.

A fetch upserts `pages` (preserving `first_fetched_at`) and appends one `fetches`
row. An `fts5` virtual table (`pages_fts`) indexes url+title+text, trigger-synced
to `pages`.

**HTML blobs are content-addressed and versioned.** A blob lives at
`html/<sha256(raw bytes)>.html`, so every distinct version of a page is preserved: an unchanged refetch resolves to the same file (no rewrite), a changed one writes a new blob alongside the old. `pages` points at the current version; prior versions stay on disk and in the `fetches` log. This is what makes "reproducible quotes after a page changes" true.

## Lifecycle

```text
web_fetch.py   →  writes cache.sqlite + html/ (localhost)
   make push   →  R2 (durable; rides the existing ingest_sources manifest)
   make explore→  rebuilds web_pages / web_fetches from the sqlite
   query       →  scripts/web_scrape/web_cache.py helpers, or the main DuckDB

restore: make pull + make explore
```

The cache is **never committed to git** (`ingest_sources/` is gitignored); R2 is the durable store, reached by the same `make push` / `make pull` the other ingest sources use — no extra wiring.

## Fetching

```bash
uv run python scripts/web_scrape/web_fetch.py <url> --query "haggis closed 2024"
```

`--query` records the search intent that led there. Batch with `--from-file` (a `url<TAB>query` TSV); see `--help` for `--force` and `--max-age`.

Scrape behavior:

- **Polite** — descriptive User-Agent, per-domain rate limit, and an idempotent skip when the URL was fetched within the freshness window.
- **Normalized** — URLs are canonicalized (host lowercased, tracking params and fragment stripped, trailing slash dropped) so the same page dedups to one row; UTF-8 preserved, including non-ASCII in foreign-language quotes.
- **Extracted** with [`trafilatura`](https://trafilatura.readthedocs.io/): readable text and title, plus a `last_updated` date extracted conservatively (htmldate, `extensive_search=False`) — a real date the page states, else null. We deliberately don't pad a weak year-only signal up to a fabricated `Jan 1`: for evidence, no date beats a wrong one.

### JavaScript-rendered pages

A client-rendered (JavaScript-only) site returns a skeleton document to the plain `urllib` GET — trafilatura extracts little or no text, so there's nothing to quote. When the extracted text comes back **thin** (under `--thin-chars`, default 200), the fetcher escalates to a **headless-Chromium render** (Playwright), executes the page's JavaScript, and stores _that_ DOM as the blob, marked `rendered`. The fast stdlib path stays the default; the browser fires only on the thin fallback. See [JsFetch.md](JsFetch.md) for the full design.

```bash
uv run playwright install chromium    # one-time: download the browser binary (~150MB)
```

Flags: `--no-render` (pure stdlib, never render), `--render` (force a render even when the plain fetch isn't thin, for sites known to be JS-only — pair with `--force` to re-render a page that's already cached and fresh), `--thin-chars N` (tune the threshold). The browser is launched once per run, lazily — an all-stdlib batch never pays browser startup.

Two honest caveats about rendered blobs: the stored bytes are the **rendered DOM, not what the server sent** (hence the `rendered` flag, so a citation's provenance is clear), and their `content_sha` is **non-deterministic** (hydration, timestamps), so the unchanged-refetch dedup degrades — a `--force` on a JS page typically writes a _new_ blob alongside the old each time.

## Querying

Python helpers (`scripts/web_scrape/web_cache.py`):

```python
import sys; sys.path.insert(0, "scripts/web_scrape")
import web_cache
web_cache.search("haggis closed")   # FTS5 BM25-ranked: url, title, snippet
web_cache.quote(url, "2024")         # sentences in the page containing a needle
web_cache.get(url)                   # full page record
```

`quote()` is the starting point for a verbatim `note:` — confirm wording against
the stored blob before shipping. `make explore` also materializes the cache into
the `web_pages` / `web_fetches` DuckDB tables (via `03_raw_web.sql`) for joining
against the catalog.

## Data patches

The cache feeds the two evidence fields of a [data
patch](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatches.md)
(authored in the [flippatch](https://github.com/deanmoses/flippatch) repo):

- **`note:`** — a verbatim quote from `web_cache.quote()`, formatted with flippatch's
  `patchkit.source_note()`.
- **`cite:`** — the page URL.

See DataPatches.md for the cite rules (a URL cite needs its website root seeded
first; a known-scheme URL like `ipdb.org` cites as `scheme:id`).

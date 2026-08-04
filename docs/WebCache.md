# Web Scrape Cache

The web scrape cache is a searchable, durable, growing cache of fetched web pages / PDF / image OCR / video transcripts used as **evidence** for catalog data.

Flipcommons catalog data are written as curated [data patches](#data-patches). This web scrape cache is used to source that data from web. This cache:

- fetches a resource once and caches it forever, avoiding rate-limiting and speeding access to slow foreign sites
- extracts text from web sites, PDFs, video transcripts
- supports [automated quote verification](#automated-quote-verification)
- provides a **searchable corpus** of pinball evidence that grows over years
- captures **provenance**: when we fetched, the search intent that led there, and the page's own publish/modified date

## The loop, end to end

Cache a source once, then pull a verbatim quote from it whenever a claim needs evidence:

```bash
# Cache a source page
uv run python scripts/web_scrape/web_fetch.py \
  https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation \
  --query "haggis pinball closed"
```

```python
# Pull verbatim quote
import sys; sys.path.insert(0, "scripts/web_scrape")
import web_cache

web_cache.search("haggis closed")
# → [{'url': 'https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation',
#     'title': 'HAGGIS PINBALL IN LIQUIDATION', 'last_updated': '2024-07-18', 'snippet': …}]

web_cache.quote("https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation",
                "ceased trading", context=1)
# → ['# HAGGIS PINBALL IN LIQUIDATION\n
#     Australian pinball manufacturer Haggis Pinball has ceased trading and appointed liquidators.\n
#     The business failed to secure financing to continue its operations.']
```

`context=1` widens the hit to a line either side, so you see the quotable span in its surroundings. A **verbatim substring** of it becomes the `quote` of a cite in a data patch (authored in flippatch):

```yaml
cite:
  ref: https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation
  quote: "Haggis Pinball has ceased trading and appointed liquidators."
```

In Flippatch, `make verify-quotes` then confirms the quote against the cached text before the patch ships. Everything below is detail on the three steps:

1. [fetching](#fetching) (and [importing](#manual-import) what won't fetch),
2. [querying](#querying), and
3. [citing](#data-patches)

## Automated quote verification

One job of this cache is to back flippatch's automated quote verification. That gate must be **fast, deterministic and offline** — it runs against stored text, never a live fetch. A quote needs to verify for one brief window: from the moment a session authors a data patch containing it, to the moment that patch is committed. That is when `make verify-quotes` runs. After that the quote is shipped and immutable, and nothing re-checks it.

`make verify-quotes` checks a quote against `pages.text`, and it requires the cite's `quote` to be a verbatim substring once smart quotes are straightened and whitespace runs collapsed. **If a quote doesn't verify, the presumption is that the quote is wrong** — changing cached text to match a claim is a deliberate human act, never a side effect of making a check pass.

## Architecture

The **SQLite database is the system-of-record**; `make explore` materializes it into the `web_pages` / `web_fetches` DuckDB tables (via `03_raw_web.sql`) so web evidence can be joined against the IPDB/OPDB/pindata tables.

```text
ingest_sources/web/          ← durable (R2-backed, gitignored), NOT in git
  cache.sqlite                 system-of-record: pages + fetches + pages_fts (FTS5)
  raw/<sha256(raw)>.<ext>      raw page blobs, content-addressed
                               (kept for re-extraction; citations verify
                                against pages.text, never blobs)

scripts/web_scrape/
  web_cache.py               store: schema, URL normalization, upsert,
                             search() / quote() / outline() / section() / get()
  web_http.py                transport: GET, content-type gate, wire-safe URLs
  web_video.py               transport: YouTube caption tracks via yt-dlp
  content_types/             one handler per document type (the registry)
  web_ocr.py                 OCR backend for images (macOS Vision)
  web_render.py              headless-render fallback for JS-only pages
  web_fetch.py               CLI + per-URL orchestration (writes sqlite + raw/)
  web_import.py              CLI: file a hand-obtained file as evidence
  web_backfill.py            CLI: re-extract all cached HTML from stored blobs

sql/
  03_raw_web.sql             ATTACHes the sqlite, materializes web_pages/web_fetches
```

Two tables plus an FTS index (schema and invariants documented in [`web_cache.py`](../scripts/web_scrape/web_cache.py)): **`pages`** is current state per normalized URL — the extracted `title`/`text`/`last_updated`, plus provenance flags `rendered` (see [JS-rendered pages](#javascript-rendered-pages)), `text_source` (see [How the text was derived](#how-the-text-was-derived)) and `imported` (see [Manual import](#manual-import)). **`fetches`** is the append-only audit log: one row per fetch, with the `search_query` that drove it, the `content_sha` it saw, and a `changed` flag. Blobs are content-addressed, so every distinct version of a page stays on disk.

## Lifecycle

```text
web_fetch.py   →  writes cache.sqlite + raw/ (localhost)
web_import.py  →  same, for files the fetcher can't retrieve
   make push   →  R2 (durable; rides the existing ingest_sources manifest)
   make explore→  rebuilds web_pages / web_fetches from the sqlite
   query       →  scripts/web_scrape/web_cache.py helpers, or the main DuckDB

restore: make pull + make explore
```

The cache is **never committed to git** (`ingest_sources/` is gitignored); R2 is the durable store, reached by the same `make push` / `make pull` the other ingest sources use.

## Fetching

```bash
uv run python scripts/web_scrape/web_fetch.py <url> --query "haggis closed 2024"
```

`--query` records the search intent that led there. Batch with `--from-file` (a `url<TAB>query` TSV); see `--help` for `--force` and `--max-age`.

Scrape behavior:

- **Polite** — descriptive User-Agent, per-domain rate limit, and an idempotent skip when the URL was fetched within the freshness window.
- **Normalized** — URLs are canonicalized (host lowercased, tracking params and fragment stripped, trailing slash dropped) so the same page dedups to one row; UTF-8 preserved, including non-ASCII in foreign-language quotes.
- **Extracted whole** — the entire document as block-level markdown (see [What the stored text looks like](#what-the-stored-text-looks-like)), plus a `last_updated` date extracted conservatively — a real date the page states, else null. We deliberately don't pad a weak year-only signal up to a fabricated `Jan 1`: for evidence, no date beats a wrong one.

### What the stored text looks like

`pages.text` for an HTML page is the **whole document** — footers, nav, comments, forum replies, a manufacturer index kept in a `<select>` dropdown, etc.

The text is YAML-style frontmatter followed by the page as markdown:

```markdown
---
title: Wizard Pinball | Pinside
description: A classic game.
---

# Wizard

Intro text.

## Specifications

| Players | 4 |
```

- The **frontmatter** carries the `<title>` and an allowlist of `<head>` tags (`description`, `og:*`, `twitter:*`, `article:*`, …) as `key: value` lines. The delimiters are always present, even when no metadata was found.
- The **body** is the `<body>` as markdown, block-level only. Page headings keep their **source** ATX levels (an `<h1>` is `#`) with nothing above them to outrank, so the document is well-formed markdown and `"in the Specifications section"` is a locator the text itself supports. Inline markup (`<a>`, `<b>`, `<em>`…) is deliberately not converted — its markers would land inside quotable spans — so no `**` or `[text](url)` appears anywhere. Table rows keep their pipes with empty cells preserved (`| Cavalier | 1979 | | … |` — the gap says "no month recorded"). Scripts, styles, SVG internals, HTML comments, JSON-LD and recognized cookie-consent widgets never reach the text; `<noscript>`, `<template>` and dropdown option text do.

The `title` column is `og:title` → `<title>` → first `<h1>`, stored verbatim — no site-suffix stripping, because no separator heuristic can tell `… | Jersey Jack Pinball` from `Sirmo : Magic Screen`, where the separator joins two halves of one real title.

After an extraction change, run `web_backfill.py` to re-derive `text`/`title` for every cached HTML page from its stored blob (skipping `manual` and other non-`html` text sources, and never blanking a non-empty row).

### JavaScript-rendered pages

A client-rendered (JavaScript-only) site returns a skeleton document to the plain GET. When the extracted **body** comes back thin (under `--thin-chars`, default 200 — the frontmatter doesn't count, since JS-only pages ship rich `og:` tags), the fetcher escalates to a **headless-Chromium render** (Playwright), executes the page's JavaScript, and stores _that_ DOM as the blob, marked `rendered`.

```bash
uv run playwright install chromium    # one-time: download the browser binary (~150MB)
```

Flags: `--no-render` (pure stdlib, never render), `--render` (force a render for sites known to be JS-only — pair with `--force` if the page is already cached and fresh), `--thin-chars N`. Rendered blobs are the rendered DOM, not what the server sent — the `rendered` flag keeps a citation's provenance clear — and their `content_sha` is non-deterministic, so a `--force` on a JS page typically writes a new blob each time.

### PDF documents

PDFs (rulesheets, flyers, press releases) are first-class evidence, fetched like any other URL: detected by content type or `%PDF-` magic bytes when a server mislabels them, stored as the raw bytes the server sent, text and title via pypdf, `last_updated` from the PDF's own `/ModDate` (then `/CreationDate`) — a real date or null. The blob is the unmodified document, so a citation re-verifies against the exact bytes. A scanned/image-only PDF extracts to no text (there is no OCR on this path) and prints a loud warning; see `--text-source` under [Manual import](#manual-import).

### Images (OCR)

Images are evidence whose text is **printed**: a scanned flyer, a photographed manual page, a screenshot of a page that won't scrape. A JPEG or PNG stores the raw bytes, and its text comes from **OCR** via macOS Vision — no system binary, no model download, no network. A picture with no legible text prints a loud warning rather than silently caching a blank page.

OCR is deliberately deterministic, not smart: it garbles stylized lettering but never invents fluent sentences that would sail through the verbatim quote gate — and it cannot tell you when it _is_ wrong. So OCR'd text is a **draft**: fine to index and search, but review it against the picture before citing (see [Manual import](#manual-import)). The image's `title` and `last_updated` stay null. OCR is macOS-only; on another platform the bytes still cache with a warning and the text comes in by hand — and a host that can't OCR never blanks text a Mac already stored.

### How the text was derived

Every page row carries a **`text_source`** label saying what turned the bytes into text: `html` (the markdown conversion), `pdf` (pypdf's text layer), `vtt` (a caption track), `ocr` (machine-read pixels), or `manual` (a human transcription). These are not equally trustworthy — a PDF's text layer is what the document contains, OCR is a guess about pixels, captions a guess about audio — so weigh a quote by its label:

```sql
SELECT url, title FROM web_pages WHERE text_source = 'ocr';  -- read the picture before quoting
```

`text_source` is independent of `rendered` (which says where the **bytes** came from): a rendered page is still `text_source = 'html'`. Rows cached before the column existed are NULL.

### Video transcripts (YouTube)

A YouTube URL routes automatically to the caption-track transport: yt-dlp pulls the video's metadata and best caption track, the raw `.vtt` becomes the blob and the parsed transcript the page text — searchable and quotable like any page. Every URL shape (`watch?v=`, `youtu.be/`, `/shorts/`, `/live/`, `/embed/`) collapses to the one canonical `watch?v=<id>` cache key; `title` is the video title, `last_updated` its upload date. Manual subtitles beat auto-captions, and among auto-captions the original spoken language beats YouTube's machine translations; the `fetches` log records which track was taken. Timestamps stay in the `.vtt` blob for a citation's `locator:` moment.

A video with **no captions at all** (common for livestream archives) logs a loud warning and no page — there is no transcript to quote. Check the video's description for the written source it usually links, and cite that instead.

## Manual import

Some sources won't be fetched — `ipdb.org` answers HTTP 403 site-wide to the fetcher, others sit behind a login or a Cloudflare challenge — while a person with a browser gets the same page fine. `web_import.py` takes the file that person saved and files it as evidence like anything else: content-addressed blob, extracted text, FTS-indexed, quotable and citable. This is the minority path, not a routine alternative to fetching; any type the cache understands can come in this way. See `--help` for `--title`, `--date`, and `--force`.

```bash
uv run python scripts/web_scrape/web_import.py flyer.jpg --url https://www.ipdb.org/images/4583/image-3.jpg --dry-run
uv run python scripts/web_scrape/web_import.py flyer.jpg --url https://www.ipdb.org/images/4583/image-3.jpg --text-file flyer.txt --query "mecatronics space shuttle flyer"
```

The rules that bind an import:

- **An imported row never pretends to be a fetch.** `imported = 1` on the page and its audit row, and `http_status` stays NULL — no request was made. Query fetched-only evidence with `WHERE imported IS DISTINCT FROM 1`.
- **Store the bytes under the URL that serves them** — the image URL, not the viewer page that displays it. The file type is taken from the magic bytes, not the filename.
- **Text is mandatory.** An import with nothing to quote is refused: supply a transcription with `--text-file` (recorded as `text_source = 'manual'`) or let the file's handler extract it.
- **A reviewed transcription outranks a later re-extraction.** A refetch never replaces `manual` text while the bytes are unchanged; when the bytes did change, the new extraction wins loudly. Changing a transcription is a deliberate act through `web_import.py --force`.
- **`--text-source` labels machine-read text** that a machine, not a person, produced — the scanned-PDF case, where the words come from OCR run outside this tool. It keeps `manual` meaning "a person is answerable for these words".

The intended flow for an image, and the reason `--dry-run` exists:

1. **Draft** — run with `--dry-run` to see exactly what would be stored, including the full OCR text. Nothing is written.
2. **Review** — compare the draft against the picture. Correct only what the document itself contradicts — never "correct" toward what the text is expected to say.
3. **Import** — pass the reviewed text with `--text-file`.

Keep the document's line structure in a transcription; the quote gate's whitespace collapsing handles the rest.

## Querying

The helpers (`scripts/web_scrape/web_cache.py`) are an **escalation ladder** — each rung reads more of a page than the one before, so reach for the next rung only when the previous one wasn't enough. Whole-document text is long-tailed (the median page is ~6K chars, but a comment-heavy page can run 60x that), and the needle-driven helpers cost the same however big the page is:

```python
import sys; sys.path.insert(0, "scripts/web_scrape")
import web_cache
web_cache.search("haggis closed")          # 1. FTS5 BM25-ranked: url, title, snippet
web_cache.quote(url, "2024")               # 2. sentences containing a needle
web_cache.quote(url, "2024", context=3)    #    …each hit widened to ±3 lines
web_cache.outline(url)                     # 3. heading tree + per-section char counts
web_cache.section(url, "Specifications")   # 4. one section's block, not the page
web_cache.get(url)                         # 5. full page record — the last resort
```

`quote()` is the starting point for a patch's **`cite.quote`** — the verbatim span, not the `note:` (see [Data patches](#data-patches)). `outline()` tells you where a long page's weight sits ("intro 2K, machine list 4K, 41 comments 32K") for a couple hundred chars; `section()` then pulls just the block you need, and a quote found that way carries its locator for free (`"in the Specifications section"`). If a heading matches more than once, `section()` returns every matching block — ambiguity surfaces rather than silently picking one.

**Whole-page reasoning belongs in a subagent.** When a question genuinely needs a long page read end to end ("do any of the 41 replies dispute the production count?"), don't pull 38K chars into the main session to extract two sentences — spawn a subagent whose prompt is "read this page's text, return the verbatim sentences about X with their section headings", and let only the extracted spans enter the main session's context.

## Data patches

The cache is where the evidence in a [data patch](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatches.md) comes from (patches are authored in the [flippatch](https://github.com/deanmoses/flippatch) repo). A cached page becomes a cite in mapping form:

```yaml
cite:
  ref: https://www.tilt.it/flipper_pinball/ipdb/cea # the page URL
  quote: "Fly Man – ss – 1p" # verbatim, from web_cache.quote()
  locator: in the 1978 machine list # optional: where in the page it sits
```

- **`quote`** is the only field that must match the source word for word — it is what `make verify-quotes` checks. Take it from `web_cache.quote()`.
- **`locator`** is freeform for a web page, and says where the excerpt lives so a reader can find it.
- **`note:`** is the edit summary — rationale beyond the evidence, uncertainty, why the value follows. It is never a verbatim excerpt, and a cite carrying a quote usually needs no note at all.

`cite:` also takes a list, and the policy for AI-authored patches is to corroborate a fact from as many separate sources as possible.

See DataPatches.md for the full cite grammar (a URL cite needs its website root seeded first; a known-scheme URL like `ipdb.org` cites as `scheme:id`), and [DataPatchAuthoring.md](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatchAuthoring.md) for the authoring rules on quoting.

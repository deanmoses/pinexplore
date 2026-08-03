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
  raw/<sha256(raw)>.<ext>      raw page blobs, content-addressed + versioned
                               (HTML as <sha>.html, a fetched PDF as <sha>.pdf;
                                the extension derives from the row's content_type)

scripts/web_scrape/
  web_cache.py               store: schema, URL normalization, upsert,
                             search() / quote() / get()
  web_http.py                transport: GET, content-type gate, wire-safe URLs
  web_video.py               transport: YouTube caption tracks via yt-dlp
  content_types/             one handler per document type (the registry)
    base.py                  the ContentHandler interface + ExtractedMeta
    html.py                  HTML: charset decode + trafilatura → title/text/date
    pdf.py                   PDF: %PDF- sniff + pypdf → title/text/date
    vtt.py                   WebVTT captions → spoken-line transcript
    image.py                 JPEG/PNG: bytes stored verbatim, text via OCR
  web_ocr.py                 OCR backend for images (macOS Vision)
  web_render.py              headless-render fallback for JS-only pages
  web_fetch.py               CLI + per-URL orchestration (writes sqlite + raw/)
  web_import.py              CLI: file a hand-obtained file as evidence

sql/
  03_raw_web.sql             ATTACHes the sqlite, materializes web_pages/web_fetches
                             (raw-ingestion band, alongside 02_raw.sql)
```

The raw blob stays the copy we **re-verify quotes against**; it is kept on disk (not in SQLite) to keep the DB lean and the FTS index fast.

### SQLite schema

Defined in [`web_cache.py`](../scripts/web_scrape/web_cache.py); two tables plus an FTS index:

- **`pages`** — current state per normalized URL: the current version's
  `content_sha`, the (canonical) `content_type`, the extracted `title`/`text`/`last_updated`, a
  `rendered` flag (1 when the stored blob is a headless-browser render, not the
  bytes the server sent — see [JS-rendered pages](#javascript-rendered-pages)), and a
  `text_source` label (see [How the text was derived](#how-the-text-was-derived)). The blob's on-disk path is deliberately **not** a column — it's derived (see below).
- **`fetches`** — append-only audit + version history: one row per fetch, with the
  `search_query` that drove it, the `content_sha` it saw, a `changed` flag, a
  `rendered` flag, an `imported` flag (see [Manual import](#manual-import)), and
  the `text_sha` of the text that fetch stored (see below).

A fetch upserts `pages` (preserving `first_fetched_at`) and appends one `fetches`
row. An `fts5` virtual table (`pages_fts`) indexes url+title+text, trigger-synced
to `pages`.

**Blobs are content-addressed and versioned.** A blob lives at
`raw/<sha256(raw bytes)>.<ext>`, so every distinct version of a page is preserved: an unchanged refetch resolves to the same file (no rewrite), a changed one writes a new blob alongside the old. `pages` points at the current version (by `content_sha`); prior versions stay on disk and in the `fetches` log. This is what makes "reproducible quotes after a page changes" true.

**Text is the one mutable field, so each fetch logs its `text_sha`.** Everything else about a version is already tamper-evident: bytes are content-addressed, every version stays on disk, and the prior `content_sha` stays in the append-only log — so changed bytes are always visible and the old ones recoverable. `pages.text` has none of that. Re-storing a page with a corrected transcription leaves the blob untouched and would otherwise surface only as "an import happened", which makes the field most worth scrutinising the one with no record. The per-fetch hash of the stored text closes that: a text-only change becomes provable rather than inferable — two audit rows with the same `content_sha` and different `text_sha`.

```sql
-- transcriptions that changed while the bytes stayed identical
SELECT url, count(DISTINCT text_sha) AS versions FROM web_fetches
WHERE text_sha IS NOT NULL GROUP BY url, content_sha HAVING versions > 1;
```

This matters most where a patch author and the evidence live in the same filesystem: `make verify-quotes` checks a quote against `pages.text`, so the cheapest way to clear a failing check is to edit the source it's checked against. **If a quote doesn't verify, the presumption is that the quote is wrong** — changing cached text to match a claim is a deliberate human act, never a side effect of making a check pass. Rows logged before this column exists stay NULL: what they stored isn't recoverable after the fact, and hashing whatever the page holds _now_ would assert precisely what the column exists to prove.

The blob path is **derived, not stored**: `raw/<content_sha>.<ext>`, where `<ext>` comes from the row's `content_type` (`content_types.extension_for`, or `web_cache.blob_path` from the fetcher). The row carries only the fact actually fetched — the type — and never bakes the directory name or a derivable extension into the data, so renaming the blob dir is a code-and-filesystem change that never touches a row.

## Lifecycle

```text
web_fetch.py   →  writes cache.sqlite + raw/ (localhost)
web_import.py  →  same, for files the fetcher can't retrieve
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

A client-rendered (JavaScript-only) site returns a skeleton document to the plain `urllib` GET — trafilatura extracts little or no text, so there's nothing to quote. When the extracted text comes back **thin** (under `--thin-chars`, default 200), the fetcher escalates to a **headless-Chromium render** (Playwright), executes the page's JavaScript, and stores _that_ DOM as the blob, marked `rendered`. The fast stdlib path stays the default; the browser fires only on the thin fallback.

```bash
uv run playwright install chromium    # one-time: download the browser binary (~150MB)
```

Flags: `--no-render` (pure stdlib, never render), `--render` (force a render even when the plain fetch isn't thin, for sites known to be JS-only — pair with `--force` to re-render a page that's already cached and fresh), `--thin-chars N` (tune the threshold). The browser is launched once per run, lazily — an all-stdlib batch never pays browser startup.

Two honest caveats about rendered blobs: the stored bytes are the **rendered DOM, not what the server sent** (hence the `rendered` flag, so a citation's provenance is clear), and their `content_sha` is **non-deterministic** (hydration, timestamps), so the unchanged-refetch dedup degrades — a `--force` on a JS page typically writes a _new_ blob alongside the old each time.

### PDF documents

PDFs (rulesheets, manufacturer flyers, press releases) are first-class evidence. A PDF is detected by its `application/pdf` content-type — or, when a server mislabels it (commonly `application/octet-stream`), by a `%PDF-` magic-byte sniff — then stored as the **raw bytes the server sent**, as a `<sha>.pdf` blob. [`pypdf`](https://pypdf.readthedocs.io/) pulls the readable text (for FTS + quoting) and title, and `last_updated` from the PDF's own `/ModDate` (falling back to `/CreationDate`), kept as conservative as the HTML date — a real date the document states, else null. No flags and no extra setup: a PDF URL is fetched exactly like any other.

PDFs are the integrity opposite of rendered pages: the blob is the unmodified document, so `content_sha` is **deterministic** (dedup works perfectly) and a citation re-verifies against the exact bytes — a PDF never touches the `rendered` flag. An image-only/scanned PDF extracts to little or no text (there is no OCR); like a still-thin render, that prints a loud warning so a zero-quote document isn't silent.

### Images (OCR)

Images are evidence whose text is **printed**: a scanned flyer, a photographed manual page, a screenshot of a page that won't scrape. A JPEG or PNG is stored as the **raw bytes the server sent** (a `<sha>.jpg` / `<sha>.png` blob, deterministic and dedup-friendly like a PDF), and its text comes from **OCR** — [macOS Vision](https://developer.apple.com/documentation/vision) via pyobjc, which needs no system binary, no model download, and no network. No flags: an image URL is fetched exactly like any other, and a picture with no legible text prints a loud warning rather than silently caching a blank page.

OCR is deliberately a **deterministic** text layer, not a smart one, and that is the point. Vision reads the same pixels the same way every run. It garbles hard glyphs — stylized logo art on the Mecatronics _Space Shuttle_ flyer comes out as junk tokens like `SAUTTL` and `BUMON` — but it does not invent sentences, which is exactly what a language model asked to "read this flyer" will do. Since a cached page is quoted verbatim downstream, a fluent wrong transcription is far more dangerous than obviously-broken text: it sails through the verbatim gate while misrepresenting the source. So no model writes text into this cache. Two settings follow from measurement rather than taste: lines below a confidence floor (`web_ocr.MIN_CONFIDENCE`, 0.6) are dropped, which on that flyer removed precisely the art noise and kept every line of prose; and Vision's `usesLanguageCorrection` stays **on**, because turning it off degraded real prose (`Unrted States`, `NAO` losing its tilde) without making proper nouns any more faithful.

What OCR cannot do is tell you when it is wrong. On the same flyer it merged a fragment of the corner logo into the footer line (`TRANCE CEP 04548 - Fones: ...`) at full confidence. So OCR'd text is a **draft**: fine to index and search as-is, but an image being imported as citable evidence should have its text reviewed against the picture first (see [Manual import](#manual-import)), and a reviewer's correction must be justified by something visible in the document — the clean-type footer, a string repeated elsewhere on the page — never by what the text is expected to say.

The image's `title` and `last_updated` stay **null**: OCR'd words are not a document title, and an image's EXIF timestamp records when the photo or scan was taken, not the date the document states. Same rule as everywhere else here — no date beats a wrong one.

OCR is macOS-only (Vision ships with the OS; the `pyobjc-framework-Vision` dependency is marked `sys_platform == 'darwin'`). On another platform an image still caches its bytes, but the text must be supplied by hand; the fetcher warns and carries on rather than failing the batch. A host that can't OCR also never _erases_ text one that could already stored: since the cache is shared through R2, refetching an image from a Vision-less host would otherwise blank the row — and drop it from FTS — while logging an innocuous `changed=0`. "No backend on this host" and "no text in this document" are kept distinct, and for byte-identical content the earlier extraction is preserved.

### How the text was derived

Every page row carries a **`text_source`** label saying what turned the bytes into text: `html` (trafilatura), `pdf` (pypdf's text layer), `vtt` (a caption track), `ocr` (machine-read pixels), or `manual` (a human transcription — see [Manual import](#manual-import)).

It exists because these are not equally trustworthy, and the difference is invisible once the text is in the column. A PDF's own text layer is what the document literally contains; an OCR pass is a guess about pixels; auto-captions are a speech model's guess about audio. Weighing a quote means knowing which one you're reading, so the answer is stored rather than inferred from `content_type` by a consumer who has to know that `image/jpeg` implies OCR:

```sql
SELECT url, title FROM web_pages WHERE text_source = 'ocr';  -- read the picture before quoting
```

Note `text_source` and `rendered` answer different questions and are independent: `rendered` says where the **bytes** came from (a headless browser, not the server), while `text_source` says what turned those bytes into **text**. A rendered page is still `text_source = 'html'`. Rows cached before the column existed are NULL — we know how they were extracted, but back-filling a guess into an evidence column is the kind of after-the-fact assertion this store exists not to make.

### Video transcripts (YouTube)

A YouTube URL is evidence whose text is **spoken**, so it gets its own transport: `web_video.py` uses [yt-dlp](https://github.com/yt-dlp/yt-dlp) to pull the video's metadata and best caption track, and the fetcher stores the raw `.vtt` as the blob with the parsed transcript as the page text — searchable and quotable like any cached page. No new flags: `web_fetch.py <any youtube url>` routes there automatically, and every URL shape (`watch?v=`, `youtu.be/`, `/shorts/`, `/live/`, `/embed/`) collapses to the one canonical `https://www.youtube.com/watch?v=<id>` cache key. The page's `title` is the video title and `last_updated` its upload date.

Track choice: **manual subtitles beat auto-captions** (a human wrote them), English is preferred among manual tracks, and among auto-captions the **original spoken language** (`xx-orig`/`xx`) beats YouTube's machine translations — every other language there is one lossy step further from the evidence. The `fetches` log records which track was taken. The transcript keeps one spoken line per row with the rolling-caption repetition deduped and inline timing tags stripped; timestamps stay in the `.vtt` blob for anyone hunting a citation's `locator:` moment.

A video with **no captions at all** (common for livestream archives) logs a loud warning and an audit row but no page — there is no transcript to quote. Check the video's description for the written source it usually links, and cite that instead.

### Adding a content type

Each document type is a self-contained **handler** under `scripts/web_scrape/content_types/` (one file per type), and the registry in `content_types/__init__.py` is the only place that lists them. A handler declares the content types it claims, the blob extension, whether it's render-eligible, and — for a type recognizable by its first bytes — a magic-byte `signature` (plus the `canonical_mime` to stamp when a server mislabels it). It must implement `extract` (→ `title`/`text`/`last_updated`); it overrides `decode` only if it's a text type (the base default treats the body as binary and hands `extract` the raw bytes) and `thin_warning` only to phrase its own no-text case. The registry validates each handler at import, so a missing extension or an inconsistent `canonical_mime` fails loudly up front rather than mid-fetch. `web_http` (the transport) and `web_fetch` (the orchestrator) branch only on the handler — never on a concrete type — so a new type (plain text, `.docx`) is a new file plus one line in the `HANDLERS` tuple, not an edit threaded through the pipeline.

## Manual import

Some sources simply won't be fetched. `ipdb.org` answers **HTTP 403 site-wide** to `web_fetch.py` — with and without `--render`, for image URLs and `machine.cgi` pages alike — while a person with a browser opens the same page fine. Others sit behind a login, a consent wall, or a Cloudflare challenge. `web_import.py` takes the file that person saved and files it as evidence like anything else: content-addressed blob, extracted text, FTS index, quotable and citable.

```bash
uv run python scripts/web_scrape/web_import.py flyer.jpg --url https://www.ipdb.org/images/4583/image-3.jpg --dry-run
uv run python scripts/web_scrape/web_import.py flyer.jpg --url https://www.ipdb.org/images/4583/image-3.jpg --text-file flyer.txt --query "mecatronics space shuttle flyer"
```

This is the **minority path**, for sources we can't download — not a routine alternative to fetching. Any type the cache understands can come in this way: an image (text via OCR), a PDF the fetcher gets a 403 on (pypdf), a page saved from the browser (trafilatura). See `--help` for `--title`, `--date`, and `--force`.

**An imported row never pretends to be a fetch.** `imported = 1` on both the page and its audit row, and `http_status` stays **NULL** — no request was made, and a plausible-looking `200` would make the fetch log lie about the one thing it exists to record. It's the same kind of marker as `rendered`: the stored bytes are real, but how they were obtained is not what the rest of the table implies, so the row says so out loud. Query fetched-only evidence with `WHERE imported IS DISTINCT FROM 1`.

**Store the bytes under the URL that serves them.** For the flyer above that's `https://www.ipdb.org/images/4583/image-3.jpg`, not the viewer page `https://www.ipdb.org/showpic.pl?id=4583&picno=6433` that displays it. The viewer URL serves HTML; filing JPEG bytes under it would make `content_type`, `content_sha` and the `.jpg` blob all describe a resource that URL does not return — a quiet falsehood that then rides along in every citation. The file type itself is taken from the **magic bytes**, not the filename, because a browser's "Save image as" cheerfully lands a JPEG under `.txt` or no suffix at all.

**A reviewed transcription outranks a later re-extraction.** A source that 403s today can become fetchable tomorrow, or land in a `--from-file` batch — and re-extracting the same bytes would trade a person's transcription for OCR output while logging it as `changed=0`. So a successful fetch never replaces `text_source = 'manual'` text when the bytes are unchanged; it updates the fetch metadata around it (`imported` flips to 0, because those bytes really did come off the wire this time, while `text_source` stays `manual` — the two answer different questions). When the bytes _did_ change the new extraction wins, since a transcription of the old version misdescribes the new one, but the supersession is announced loudly with the `text_sha` that finds the old text in the audit log. Changing a transcription stays a deliberate act through `web_import.py --force`, the one audited path for it.

**Text is mandatory.** An import with nothing to quote is refused, not stored: blank-text pages are excluded from flippatch's `evidence_pages` and can't be indexed by FTS, so the row would be evidence in name only. Supply a transcription with `--text-file` (recorded as `text_source = 'manual'`) or let the file's own handler extract it.

**`--text-source` says which machine read supplied text**, when the answer isn't "a person". The case that needs it is a **scanned PDF**: the PDF handler reads a text layer, a scan has none, and there is no OCR on that path — so the words have to come from OCR run outside this tool (render the pages, read them with the same Vision backend and confidence floor `web_ocr` uses), and `--text-file` alone would file a machine's reading as a transcription. `--text-source ocr` keeps `manual` meaning the one thing it exists to mean, and keeps the row out of the transcription-outranks-refetch protection above, which a machine draft hasn't earned. It accepts only labels the cache records — every handler's own plus `manual` — and only alongside `--text-file`, since without one the handler's extraction is stored under its own label and declaring a different one would just overwrite the truth.

The intended flow for an image, and the reason `--dry-run` exists:

1. **Draft** — run with `--dry-run` to see exactly what would be stored, including the full OCR text, and whether the URL is already cached. Nothing is written: a dry run opens the cache read-only, and on a fresh checkout doesn't create it at all.
2. **Review** — compare that draft against the picture. Correct only what the document itself contradicts: its clean-type footer, a string repeated elsewhere on the page. Never "correct" it toward what the text is expected to say — that substitutes an expectation for the evidence, which is the failure mode OCR was chosen to avoid.
3. **Import** — pass the reviewed text with `--text-file`.

An accurate transcription verifies downstream by the ordinary path, with no special-casing in flippatch: `make verify-quotes` resolves an `http(s)` cite through `web_cache.get(url)` and requires the cite's `quote` to be a verbatim substring of the stored text once smart quotes are straightened and whitespace runs collapsed. Keep the document's line structure in the transcription and the gate's whitespace collapsing handles the rest.

**The gate covers pending patches only.** It globs `patches/[0-9]*.yaml` non-recursively, so the patches under `patches/shipped/` are never re-checked — shipping retires a quote from verification, and shipped quotes are immutable. A change to how this cache extracts text therefore cannot break a shipped patch; it can only affect quotes still being authored.

## Querying

Python helpers (`scripts/web_scrape/web_cache.py`):

```python
import sys; sys.path.insert(0, "scripts/web_scrape")
import web_cache
web_cache.search("haggis closed")   # FTS5 BM25-ranked: url, title, snippet
web_cache.quote(url, "2024")         # sentences in the page containing a needle
web_cache.get(url)                   # full page record
```

`quote()` is the starting point for a patch's **`cite.quote`** — the verbatim span, not the `note:` (see [Data patches](#data-patches)). Confirm wording against the stored blob before shipping. `make explore` also materializes the cache into the `web_pages` / `web_fetches` DuckDB tables (via `03_raw_web.sql`) for joining against the catalog.

## Data patches

The cache is where the evidence in a [data patch](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatches.md) comes from (patches are authored in the [flippatch](https://github.com/deanmoses/flippatch) repo). A cached page becomes a cite in mapping form:

```yaml
cite:
  ref: https://www.tilt.it/flipper_pinball/ipdb/cea # the page URL
  quote: "Fly Man – ss – 1p" # verbatim, from web_cache.quote()
  locator: in the 1978 machine list # optional: where in the page it sits
```

- **`quote`** is the only field that must match the source word for word — it is what `make verify-quotes` checks. Take it from `web_cache.quote()` and confirm the wording against the stored blob before shipping.
- **`locator`** is freeform for a web page, and says where the excerpt lives so a reader can find it.
- **`note:`** is the edit summary — rationale beyond the evidence, uncertainty, why the value follows. It is never a verbatim excerpt, and a cite carrying a quote usually needs no note at all.

`cite:` also takes a list, and the policy for AI-authored patches is to corroborate a fact from as many separate sources as possible.

See DataPatches.md for the full cite grammar (a URL cite needs its website root seeded first; a known-scheme URL like `ipdb.org` cites as `scheme:id`), and [DataPatchAuthoring.md](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatchAuthoring.md) for the authoring rules on quoting.

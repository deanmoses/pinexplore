# Web Scrape Cache

The web scrape cache is a searchable, durable cache of web pages / text-extracted PDFs / OCR'ed images / video transcripts used as **evidence** for Flipcommons catalog data.

Flipcommons catalog data are written as curated [data patches](#cite). This web scrape cache is used to source that data from web. This cache:

- fetches a resource once and reuses it forever, avoiding rate-limiting and slow foreign sites
- extracts text from web sites, PDFs, video transcripts
- supports [automated quote verification](#cite)
- provides a **searchable corpus** of pinball evidence that grows over years
- captures **provenance** — when we fetched, the search intent that led there, and the page's own publish/modified date

## Basic usage

Cache a source once, then pull a verbatim quote from it whenever a claim needs evidence:

Cache a page:

```console
$ uv run python scripts/web_scrape/web_fetch.py \
    https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation
fetched [200] (new): https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation
    HAGGIS PINBALL IN LIQUIDATION ⬅️ page title
```

Search cached pages:

```console
$ uv run python scripts/web_scrape/web_cache.py search "haggis closed"
url: https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation
title: HAGGIS PINBALL IN LIQUIDATION
last_updated: 2024-07-18 ⬅️ the page's own stated date.  Not the fetch date.
snippet: … from Damian or [Haggis] Pinball. Today the company [closed] their social media … ⬅️ `[bracketed]` is a matched search term

url: https://www.pinballnews.com/site/2020/01/18/2019-review-of-the-year
title: 2019 REVIEW OF THE YEAR
last_updated: 2020-01-18
snippet: … [HAGGIS] PINBALL … may have its roots in Australia …
```

Pull the quotable span, labelled with the section it sits in (`--context 1` widens each hit to a line either side):

```console
$ uv run python scripts/web_scrape/web_cache.py quote \
    https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation \
    "creditors"
[Welcome to Pinball News – First & Free]
The appointment of Cathro & Partners as liquidators spells the end of Haggis Pinball, as any remaining assets are sold in an attempt to raise money for creditors of the company.
```

A **verbatim substring** of the span becomes the `quote` of a cite in a data patch (authored in flippatch), and the `[bracketed]` label becomes its `locator`:

```yaml
cite:
  ref: https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation
  quote: "any remaining assets are sold in an attempt to raise money for creditors of the company"
  locator: in the Welcome to Pinball News – First & Free section
```

In Flippatch, `make verify-quotes` then confirms the quote against the cached text before the patch ships. Everything below is detail on the three steps:

1. [Fetch](#fetch) (and [import](#import-when-fetching-fails) what won't fetch)
2. [Query](#query)
3. [Cite](#cite)

## Fetch

```bash
uv run python scripts/web_scrape/web_fetch.py <url>
```

- `--from-file urls.tsv` — batch fetch from a `url<TAB>query` TSV
- `--force` — refetch a URL that's already cached and fresh
- `--max-age N` — the freshness window in days: a URL fetched within it is skipped

Render flags are under [JavaScript-rendered pages](#javascript-rendered-pages).

Scrape behavior:

- **Polite** — descriptive User-Agent, per-domain rate limit, and an idempotent skip when the URL was fetched within the freshness window.
- **Normalized** — URLs are canonicalized (host lowercased, tracking params and fragment stripped, trailing slash dropped) so the same page dedups to one row; UTF-8 preserved, including non-ASCII in foreign-language quotes.
- **Extracted whole** — the entire document as block-level markdown (see [What the stored text looks like](#what-the-stored-text-looks-like)), plus a `last_updated` date extracted conservatively — a real date the page states, else null. We deliberately don't pad a weak year-only signal up to a fabricated `Jan 1`: for evidence, no date beats a wrong one.

### JavaScript-rendered pages

A client-rendered (JavaScript-only) site returns a skeleton document to the plain GET. When the extracted **body** comes back thin (under `--thin-chars`, default 200 — the frontmatter doesn't count, since JS-only pages ship rich `og:` tags), the fetcher escalates to a **headless-Chromium render** (Playwright), executes the page's JavaScript, and stores _that_ DOM as the blob, marked `rendered`.

```bash
uv run playwright install chromium    # one-time: download the browser binary (~150MB)
```

Flags: `--no-render` (pure stdlib, never render), `--render` (force a render for sites known to be JS-only — pair with `--force` if the page is already cached and fresh), `--thin-chars N`. Rendered blobs are the rendered DOM, not what the server sent — the `rendered` flag keeps a citation's provenance clear — and their `content_sha` is non-deterministic, so a `--force` on a JS page typically writes a new blob each time.

### PDFs

PDFs are fetched like any other URL: detected by content type or `%PDF-` magic bytes when a server mislabels them, stored as the raw bytes the server sent. The blob is the PDF file itself. Title and `last_updated` come from the document's own metadata — a real date it states, or null.

PDFs with a text layer are extracted to text — searchable and quotable like any page. Page boundaries are stored as lone `\f` lines, so `get` and `section` show them; `quote` drops them, so a span you lift never contains one.

`quote` hits name the PDF document page(s) on which they sit, and print the blob's path:

```console
$ uv run python scripts/web_scrape/web_cache.py quote <manual-url> "Bottom Pop Bumper"
blob: /…/pinexplore/ingest_sources/web/raw/9a83…0721.pdf ⬅️ stderr: a fact about the page
Bottom Pop Bumper
pdf document pages: 27
```

Facts about the **page** go to stderr — the blob path, and `pdf document pages: unavailable` on a row whose text predates the page markers — so stdout is the hit list and nothing else. Facts about a **hit** ride that hit on stdout, since they vary per hit and the two streams give no ordering guarantee to pair them by: a redirected `quote` holds each span with its `[section]` label and page line, not bare text.

Looking at the text is usually not enough. For example:

- **Printed page numbers**. To construct a citation you need the page number printed onto the sheet. The text isn't a reliable way to get that page number. The hit's `pdf document pages` is the PDF index of the sheet, not any page number printed on it.
- **Tables**. The text flattens a table into a column of cells with nothing tying rows to headers. You need to see the actual table.
- **Visual elements**. You might need to see a checkmark visually embedded in the page.

For these, look at the page visually. Claude Code can render a single page straight out of the blob: `Read(<blob path>, pages="27")` returns page 27 of a 5MB manual as an image without touching the rest.

A hit whose `--context` window spans multiple pages lists every page the shown text touches (`pdf document pages: 26, 27`).

**A miss on a PDF is not evidence of absence.** Only the document's own text layer is searchable, and a word drawn as artwork — a table cell, a diagram callout, a label baked into a figure — was never in it. Such a needle returns `no matches`, indistinguishable from a document that genuinely never says it. So `quote` prints the blob path on a miss too: the miss is when the go-look-at-the-page step matters most.

Captions and table titles are usually real text even when their contents aren't, which makes them the reliable way in. In the Galactic Tank Force manual, `Table 3-18 Coil Positions` is text and every coil name under it is artwork — quote the caption, take its page, render the sheet. `UPPER MAGNET` and `KNOCKER`, printed on that same page, cannot be found any other way.

A scanned or image-only PDF has no text layer at all: the blob caches, no text is extracted, and the fetch warns. We don't OCR PDFs yet. A row holding no text quotes as `no stored text, so no needle can match it` rather than a bare `no matches` — but it stops there, because the row cannot say whether the document is image-only or whether extraction was merely unavailable on the host that fetched it (a missing poppler, a document poppler couldn't read). Only the fetch's own warning drew that line. Read the blob to find out which.

### Images

Images — JPG, PNG — sometimes contain printed evidence: a scanned flyer, a photographed manual page, a screenshot of a page that won't scrape. We store the image's raw bytes, and its text comes from **OCR** via macOS Vision — no system binary, no model download, no network. A picture with no legible text prints a loud warning rather than silently caching a blank page.

OCR is deliberately deterministic, not smart: it garbles stylized lettering but never invents fluent sentences that would sail through the verbatim quote gate — and it cannot tell you when it _is_ wrong. So OCR'd text is a **draft**: fine to index and search, but review it against the picture before citing (see [Import](#import-when-fetching-fails)). The image's `title` and `last_updated` stay null. OCR is macOS-only; on another platform the bytes still cache with a warning and the text comes in by hand — and a host that can't OCR never blanks text a Mac already stored.

### Video transcripts (YouTube)

A YouTube URL routes automatically to the caption-track transport: yt-dlp pulls the video's metadata and best caption track, the raw `.vtt` becomes the blob and the parsed transcript the page text — searchable and quotable like any page. Every URL shape (`watch?v=`, `youtu.be/`, `/shorts/`, `/live/`, `/embed/`) collapses to the one canonical `watch?v=<id>` cache key; `title` is the video title, `last_updated` its upload date. Manual subtitles beat auto-captions, and among auto-captions the original spoken language beats YouTube's machine translations; the `fetches` log records which track was taken. Timestamps stay in the `.vtt` blob for a citation's `locator:` moment.

A video with **no captions at all** (common for livestream archives) logs a loud warning and no page — there is no transcript to quote. Check the video's description for the written source it usually links, and cite that instead.

### Import: when fetching fails

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

## Query

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
- The **body** is the `<body>` as markdown, block-level only. Page headings keep their **source** ATX levels (an `<h1>` is `#`) with nothing above them to outrank, so the document is well-formed markdown and `"in the Specifications section"` is a locator the text itself supports. Inline markup (`<a>`, `<b>`, `<em>`…) is deliberately not converted — its markers would land inside quotable spans — so the conversion never introduces `**` or `[text](url)` of its own. Table rows keep their pipes with empty cells preserved (`| Cavalier | 1979 | | … |` — the gap says "no month recorded"). Scripts, styles, SVG internals, HTML comments, JSON-LD and recognized cookie-consent widgets never reach the text; `<noscript>`, `<template>` and dropdown option text do.

The markdown's sections (`#`, `##`, …) are the page's own h1–h6 tags, and a section runs until the next heading at the same or higher level — so a parent section contains its subsections, and `outline()`'s char count for a parent includes its children's. Site chrome — header, footer, nav — often has no heading of its own, so like any other unheaded text it belongs to the section above it, or to no section at all when nothing precedes it. A locator naming a page's first or last section is the one worth a second look, since that is where chrome lands.

`metadata` and `body` are addressable through `section()` as well, but they are **frames this assembly adds**, not parts of the page: every HTML page has both, and a PDF, video transcript or OCR'd image has neither.

The `title` column is `og:title` → `<title>` → first `<h1>`, stored verbatim — no site-suffix stripping, because no separator heuristic can tell `… | Jersey Jack Pinball` from `Sirmo : Magic Screen`, where the separator joins two halves of one real title.

After an extraction change, run `web_backfill.py` to re-derive `text`/`title` for every cached HTML page and PDF from its stored blob, so the corpus reflects one extractor instead of splitting into before and after. It skips any row whose `text_source` isn't the one that handler produces — `manual` transcriptions and `ocr` text (including the OCR'd scanned PDFs, whose empty text layer is exactly why they were OCR'd) — and never blanks a non-empty row. Images and video transcripts are never swept: their text comes from a recognizer that moves under us, not a parser, so a bulk re-run would churn reviewed evidence with no extractor change to justify it.

### Determining what the cache already holds

The `have` command answers "which of these N sources am I already holding, and which still need fetching?":

```console
$ uv run python scripts/web_scrape/web_cache.py have --from-file sources.tsv
cached   https://americanpinball.com/houdini/  9324 chars  html  rendered
cached   https://www.kineticist.com/pinball-machines/eight-ball-fury-2024  5138 chars  html
         ↳ stored as https://www.kineticist.com/games/pinball/eight-ball-fury-2024 (redirected)
MISSING  https://turnerpinball.com/games/yukon-yeti/
2/3 cached
```

A URL counts as held if it is cached under its normalized form **or** as the `raw_url` of a page that redirected somewhere else — and that second case is why this is a helper rather than a loop you write inline. **8% of the current corpus lives under a different address than the one requested** (a path migration, a canonical redirect), so a hand-rolled `get()` loop calls those missing and sends the campaign off to refetch pages it already has. The alias is matched normalized, like every other lookup here, so a trailing slash or odd host casing in your source list still resolves. It is the **most recent** fetch, though, so a page refetched through its canonical address stops resolving under the old one — at worst one redundant polite refetch.

It reads the same `url<TAB>query` TSV `web_fetch.py --from-file` takes, so one source list drives both steps. To fill the gaps, hand that same list to the fetcher — it skips what it already holds, so there is no miss list to pass along:

```bash
uv run python scripts/web_scrape/web_fetch.py --from-file sources.tsv --max-age 99999
```

The fetcher skips on **freshness**, not presence — `--max-age` defaults to 30 days, so without a wide window it will also re-fetch anything older than that. `have` ignores age entirely, because a page cached two years ago is still evidence you hold.

A URL that doesn't parse is reported as `INVALID` rather than missing, because it was never looked up. One malformed entry never aborts the run — this is a bulk read over a hand-written list, and it must not cost the answer for the other sixty.

The tally goes to stderr and the exit status is non-zero when anything is missing or unparseable, so `have` also works as a precondition in a script. For a programmatic answer use `have()` in Python, which returns one `{"asked", "page", "stored_url", "error"}` record per URL in the order asked — the CLI prints for people, the functions return data, as with every other read here.

### The escalation ladder

The reads (`scripts/web_scrape/web_cache.py`) are an **escalation ladder** — each rung reads more of a page than the one before, so reach for the next rung only when the previous one wasn't enough. Whole-document text is long-tailed (the median page is ~6K chars, but a comment-heavy page can run 60x that), and the needle-driven reads cost the same however big the page is:

```bash
cache() { uv run python scripts/web_scrape/web_cache.py "$@"; }   # shorthand for this
                                           # block only — and a function, because zsh
                                           # won't word-split a CACHE="…" variable
cache search "haggis closed"               # 1. FTS5 BM25-ranked: url, title, snippet
cache quote <url> "2024"                   # 2. text containing a needle (matching
                                           #    ignores case, smart quotes, and line
                                           #    breaks; on a PDF each hit names its
                                           #    PDF page(s) and the blob path)
cache quote <url> "2024" --context 3       #    …each hit widened to ±3 lines
cache outline <url>                        # 3. heading tree + per-section char counts
cache outline <url> --min-chars 20         #    …hiding blocks too small to be content
cache outline <pdf-url>                    #    …one row per PDF sheet, and the
                                           #    blob path on stderr
cache section <url> "Specifications"       # 4. one section's block, not the page
cache section <pdf-url> "page 41"          #    …on a PDF, one sheet's text, and
                                           #    the blob path on stderr
cache get <url>                            # 5. full page record — the last resort
                                           #    (text on stdout, row fields and the
                                           #     blob path on stderr)
```

The same five reads are Python functions in `web_cache.py` (`search()`, `quote()`, `outline()`, `section()`, `get()`) — flippatch's quote gate imports them directly. `quote_hits()` is `quote()` with each hit's section and PDF pages attached (`{"text": …, "heading": …, "pdf_document_page_numbers": …}`); `quote()` stays the plain-span form the gate consumes. Matching collapses whitespace runs, straightens smart quotes, and ignores case — so a phrase spanning a stored line break is still found — while the returned text is always the stored lines verbatim, which is why every hit still verifies.

`search` spans **every cached type** — one index over web pages, PDFs, OCR'd images and video transcripts together. A non-web hit says what it is (`type:`) and how its text was derived (`text_source:`), so you know to weigh (and for `ocr`, review) before quoting; web pages are the unlabeled common case:

```console
$ uv run python scripts/web_scrape/web_cache.py search "mecatronics"
url: https://www.ipdb.org/images/4583/image-3.jpg
title: Mecatronics Space Shuttle flyer
type: image
text_source: manual
snippet: [MECATRONICS] SPACE SHUTTLE SPACE SHUTTLE United States “O MELHOR FLIPPER JAMAIS FABRICADO …

url: https://en.wikipedia.org/wiki/Taito_of_Brazil
title: Taito of Brazil - Wikipedia
last_updated: 2026-06-15
snippet: … made under the label '[Mecatronics]') - Speed Test …
```

`quote` is the starting point for a patch's **`cite.quote`** — the verbatim span, not the `note:` (see [Cite](#cite)). It labels each hit with the section the span sits in, so **one command produces a whole cite** — the `quote` and the `locator` — instead of quoting first and then hunting for where the words were:

```console
$ uv run python scripts/web_scrape/web_cache.py quote \
    https://en.wikipedia.org/wiki/Taito_of_Brazil "Mecatronics"
[Solid state electronics]
- Space Shuttle (nearly identical to Space Shuttle (Williams Electronics, 1984) made under the label 'Mecatronics')
```

The label is a name `section()` accepts, so it doubles as the way to pull the span's surroundings. `metadata` means the hit is in the frontmatter — an `og:description` rather than the page's own prose, which is usually a signal to keep reading for the body's wording. A hit above the first heading has no label, because there is no section to name — and on a page whose markup has no headings at all, nothing is labelled. A match that itself crosses a section boundary is likewise unlabeled: no single name is true of it.

The label names the **match**, never the widened window around it, so **a hit never leaves the section it names** and any span you lift out of one can carry that hit's locator — `--context` changes how much you see, never where the evidence is said to live. The cost is that a large `--context` gives you less than ±N lines near a section edge; `section()` shows the whole block when that's what you want. The label is also only as good as the page's own markup: a site whose tab labels are real `<h2>`s yields locators like `$7,995` — faithful to the document, and no more wrong than the outline it comes from.

`outline()` tells you where a long page's weight sits ("intro 2K, machine list 4K, 41 comments 32K") for a couple hundred chars; `section()` then pulls just the block you need. If a heading matches more than once, `section()` returns every matching block — ambiguity surfaces rather than silently picking one — and `outline()` correspondingly collapses that name to one row carrying `x2` and the summed size.

That collapse is what keeps the map usable on page-builder sites, where the meaningful labels are styled `<div>`s and the real `<h2>`s are UI chrome. A live product page came back with **100 rows, 38 after collapsing** — `RETIRED` twelve times, edition panels repeated once per responsive variant. Repeats collapse only when they sit in the same place in the tree (same ancestors, same level), so a name that recurs under different parents stays several rows. `--min-chars N` trims further by hiding blocks too small to be content (38 → 29 on that page), and says on stderr how many it withheld.

`section` matches a heading **exactly**, and separates the three reasons a name comes back empty instead of letting all three read as "not on this page": a heading that merely contains the name (`did you mean: 100th Anniversary`), a name that is only body text (`not a heading; appears as text in section(s): Additional Features` — the page-builder shape again), or genuine absence, which stays silent.

### Extracting everything a page knows

The ladder is for needle-driven reads — you have a claim and want its span. The inverse task also comes up: building a whole catalog entry from one page ("every gameplay feature, every credit on the new Godzilla's page"), where the page is the input and the target schema is the sieve. No needle helper enumerates that; read the whole stored text against the schema — `get()` for a typical page, or `outline()` + `section()` to walk a long one piece by piece.

**On a PDF that walk is by sheet.** `section(url, "page 41")` returns that sheet's text. A sheet with no extracted text gets no block in `section`.

For a long page, do the read in a subagent so only results enter the main session's context: its prompt is the schema ("read this page's text; for every gameplay feature, credit, spec and date it states, return the verbatim span and the section it sits in"), its return is field → span pairs. The same applies to whole-page questions ("do any of the 41 replies dispute the production count?") — don't pull 38K chars into the main session to extract two sentences. Whole-document extraction is what makes either read reliable: credits live in footers and specs in tables, and both survive with structure intact. Each extracted span then becomes its own cite.

### Weighing a quote: text_source

Every page row carries a **`text_source`** label saying what turned the bytes into text: `html` (the markdown conversion), `pdf` (the document's own text layer), `vtt` (a caption track), `ocr` (machine-read pixels), or `manual` (a human transcription). These are not equally trustworthy — a PDF's text layer is what the document contains, OCR is a guess about pixels, captions a guess about audio — so weigh a quote by its label:

```sql
SELECT url, title FROM web_pages WHERE text_source = 'ocr';  -- read the picture before quoting
```

`text_source` is independent of `rendered` (which says where the **bytes** came from): a rendered page is still `text_source = 'html'`. Rows cached before the column existed are NULL.

## Cite

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

**When the fact is only in pixels — a quote-less cite.** A checkmark in a PDF feature matrix is vector art; there is no text to quote. The honest cite is the analogue of a locator-only book cite: `ref` the page URL, `locator` the PDF document page (`printed page 17, PDF document page 27`), `note` the visual observation — `quote:` is optional in the cite grammar. **Do not quote the feature's row label instead**: it is verbatim and it is on the page, but it establishes the row, never the edition column — a true string doing work its context doesn't support, which is the column-splice forgery in a new costume. flippatch's `RULE_QUOTE_SUPPORTS_CLAIM` would rightly reject it; the point is not to author it. (The authoring-side rules live in flipcommons' [DataPatchAuthoring.md](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatchAuthoring.md); this paragraph is the reading-side statement of the same policy.)

Every quote is machine-checked. flippatch's `make verify-quotes` gate is **fast, deterministic and offline** — it requires the cite's `quote` to be a verbatim substring of `pages.text` once smart quotes are straightened and whitespace runs collapsed, and it never does a live fetch. A quote needs to verify for one brief window: from the moment a session authors the patch to the moment it's committed. After that the quote is shipped and immutable — the gate globs pending `patches/[0-9]*.yaml` only, so a change to how this cache extracts text can never break a shipped patch.

**If a quote doesn't verify, the presumption is that the quote is wrong** — changing cached text to match a claim is a deliberate human act, never a side effect of making a check pass.

See DataPatches.md for the full cite grammar (a URL cite needs its website root seeded first; a known-scheme URL like `ipdb.org` cites as `scheme:id`), and [DataPatchAuthoring.md](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatchAuthoring.md) for the authoring rules on quoting.

## Sync

The cache is shared through Cloudflare R2 and never committed to git (`ingest_sources/` is gitignored). After fetching or importing, `make push` uploads the new evidence — skip it and your fetches stay stranded on one machine. On a fresh checkout or another machine, `make pull` restores the cache and `make explore` rebuilds the DuckDB tables from it. Both ride the same manifest as the other ingest sources.

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
                             have() / search() / quote() / quote_hits() /
                             outline() / section() / get()
  web_http.py                transport: GET, content-type gate, wire-safe URLs
  web_video.py               transport: YouTube caption tracks via yt-dlp
  content_types/             one handler per document type (the registry)
  web_ocr.py                 OCR backend for images (macOS Vision)
  web_pdftext.py             PDF text backend (poppler pdftotext, reading order)
  web_render.py              headless-render fallback for JS-only pages
  web_fetch.py               CLI + per-URL orchestration (writes sqlite + raw/)
  web_import.py              CLI: file a hand-obtained file as evidence
  web_backfill.py            CLI: re-extract all cached HTML + PDFs from stored blobs

sql/
  03_raw_web.sql             ATTACHes the sqlite, materializes web_pages/web_fetches
```

Two tables plus an FTS index (schema and invariants documented in [`web_cache.py`](../scripts/web_scrape/web_cache.py)): **`pages`** is current state per normalized URL — the extracted `title`/`text`/`last_updated`, plus provenance flags `rendered` (see [JS-rendered pages](#javascript-rendered-pages)), `text_source` (see [Weighing a quote](#weighing-a-quote-text_source)) and `imported` (see [Import](#import-when-fetching-fails)). **`fetches`** is the append-only audit log: one row per fetch, with the `search_query` that drove it, the `content_sha` it saw, and a `changed` flag. Blobs are content-addressed, so every distinct version of a page stays on disk.

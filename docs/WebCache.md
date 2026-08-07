# Web Cache

The web cache is a searchable, durable cache of web pages / text-extracted PDFs / OCR'ed images / video transcripts used as **evidence** for Flipcommons catalog data.

Flipcommons catalog data are written as curated [data patches](#citing--quoting). This web cache is used to source that data from the web. This cache:

- fetches a document once and stores it forever, avoiding rate-limiting and slow foreign sites
- extracts text from web sites, PDFs, video transcripts
- provides a **searchable corpus** of pinball evidence that grows over years
- captures **provenance** — when we fetched and the document's own publish/modified date
- supports [automated quote verification](#the-verify-gate)

## How to use

Every command runs as `uv run python scripts/web_scrape/<script>`.

```bash
# --- get documents in ---
web_fetch.py <url>                                          # fetch & cache; PDFs, images, YouTube all route automatically
web_fetch.py <url> --force                                  # …refetch even though it's cached and fresh
web_fetch.py --from-file urls.tsv                           # batch, from url<TAB>query rows (query optional, logged)
web_fetch.py --from-file urls.tsv --max-age 99999           # …fetch only what's missing (plain refetches >30d)
web_cache.py have --from-file urls.tsv                      # which of these do we already hold?
web_cache.py links <url>                                    # what documents does a cached page point at?
web_cache.py links <url> --ext pdf                          # …just the PDFs
web_cache.py links <url> --ext pdf --limit 0 | cut -f1 | web_cache.py have --from-file -  # …which does cache already hold
web_import.py <file> --url <url>                            # hand-saved copy, for a site that refuses the fetcher
web_pdfocr.py                                               # OCR cached PDFs' sheet images into the searchable ocr tier (macOS)
web_pdfocr.py --url <url> [--force]                         # …one document; --force re-reads one already OCR'd

# --- narrow by query ---
web_cache.py search "haggis closed"                         # relevance-ranked: url, title, match count, sections, snippet
web_cache.py search '"upper magnet" knocker'                # …a double-quoted run is one phrase, not two loose words
web_cache.py search "coil" --url <url>                      # …that document's matching sections, in order, each counted
web_cache.py search "coil" --url <url> --section "page 41"  # …that section's matches; --surrounding-words sizes context
web_cache.py search "coil" --url <pdf-url> --pages 40-50    # …--pages is an inclusive range of PDF sheets

# --- navigate without a query ---
web_cache.py outline <url>                                  # heading tree + sizes; use when you have no search term
web_cache.py outline <url> --min-chars 20                   # …hiding blocks too small to be content
web_cache.py outline <pdf-url>                              # …one row per PDF sheet; blob path on stderr
web_cache.py section <url> "Specifications"                 # one section's block, not the whole document
web_cache.py section <pdf-url> "page 41"                    # …on a PDF, one sheet's text

# --- lift a span for a citation ---
web_cache.py quote <url> "2024"                             # text containing a needle; on a PDF each hit names its page(s)
web_cache.py quote <url> "2024" --context 3                 # …each hit widened to ±3 lines
web_cache.py quote <url> "<the whole quote>"                # …whole quote as the needle: a hit is the verification

# --- view one PDF page as an image (Claude Code built-in) ---
Read(<blob path>, pages="27")                               # blob path: quote/outline/section print it (PDFs, images)

# --- last resort ---
web_cache.py get <url>                                      # full document; text on stdout, row fields + blob on stderr
```

The reads are needle-driven: each costs the same however big the document is. That matters because a comment-heavy web page can hold 360K chars of text and a large PDF 670K. `get` is last resort, not the start.

If you are trying to suck all the information out of a document — such as you're trying to fill in every piece of information about a model or manufacturer or person record — `get` the stored text in a subagent so only the results reach your context; see [Extracting everything a document knows](#extracting-everything-a-document-knows).

## A worked example

Cache a document, then mine it for evidence.

### Fetch the document

```console
$ uv run python scripts/web_scrape/web_fetch.py https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation
fetched [200] (new): https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation
    HAGGIS PINBALL IN LIQUIDATION ⬅️ document title
```

### Search cached documents

```console
$ uv run python scripts/web_scrape/web_cache.py search "haggis closed"
url: https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation
title: HAGGIS PINBALL IN LIQUIDATION
last_updated: 2024-07-18 ⬅️ the document's own stated date.  Not the fetch date.
matches: 25 in 2 sections ⬅️ how hard it matched, and how spread out
snippet: … from Damian or [Haggis] Pinball. Today the company [closed] their social media … ⬅️ `[bracketed]` is a matched search term

url: https://www.pinballnews.com/site/2020/01/18/2019-review-of-the-year
title: 2019 REVIEW OF THE YEAR
last_updated: 2020-01-18
matches: 15 in 1 section
snippet: … [HAGGIS] PINBALL … may have its roots in Australia …
```

### Pull the quotable span

Spans are labelled with the section it sits in (`--context 1` widens each hit to a line either side):

```console
$ uv run python scripts/web_scrape/web_cache.py quote https://www.pinballnews.com/site/2024/07/18/haggis-pinball-in-liquidation "creditors"
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

In Flippatch, `make verify-quotes` then confirms the quote against the cached text before the patch ships.

## Getting documents in

Filling the cache: what to run, and what each document type does on the way in.

### `fetch`: fetch and cache a document

```bash
uv run python scripts/web_scrape/web_fetch.py <url>
```

- `--from-file urls.tsv` — batch fetch from a `url<TAB>query` TSV
- `--force` — refetch a URL that's already cached and fresh
- `--max-age N` — the freshness window in days: a URL fetched within it is skipped

Render flags are under [JavaScript-rendered pages](#javascript-rendered-pages).

Scrape behavior:

- **Polite** — descriptive User-Agent, per-domain rate limit, and an idempotent skip when the URL was fetched within the freshness window.
- **Normalized** — URLs are canonicalized (host lowercased, tracking params and fragment stripped, trailing slash dropped) so the same document dedups to one row; UTF-8 preserved, including non-ASCII in foreign-language quotes.
- **Extracted whole** — the entire document as block-level markdown (see [What the stored text looks like](#what-the-stored-text-looks-like)), plus a `last_updated` date extracted conservatively — a real date the document states, else null. We deliberately don't pad a weak year-only signal up to a fabricated `Jan 1`: for evidence, no date beats a wrong one.

#### JavaScript-rendered pages

A client-rendered (JavaScript-only) site returns a skeleton document to the plain GET. When the extracted **body** comes back thin (under `--thin-chars`, default 200 — the frontmatter doesn't count, since JS-only pages ship rich `og:` tags), the fetcher escalates to a **headless-Chromium render** (Playwright), executes the page's JavaScript, and stores _that_ DOM as the blob, marked `rendered`.

```bash
uv run playwright install chromium    # one-time: download the browser binary (~150MB)
```

Flags: `--no-render` (pure stdlib, never render), `--render` (force a render for sites known to be JS-only — pair with `--force` if the page is already cached and fresh), `--thin-chars N`. Rendered blobs are the rendered DOM, not what the server sent — the `rendered` flag keeps a citation's provenance clear — and their `content_sha` is non-deterministic, so a `--force` on a JS page typically writes a new blob each time.

### `have`: determine what documents the cache already holds

The `have` command answers "which of these N sources am I already holding, and which still need fetching?":

```console
$ uv run python scripts/web_scrape/web_cache.py have --from-file sources.tsv
cached   https://americanpinball.com/houdini/  9324 chars  html  rendered
cached   https://www.kineticist.com/pinball-machines/eight-ball-fury-2024  5138 chars  html
         ↳ stored as https://www.kineticist.com/games/pinball/eight-ball-fury-2024 (redirected)
MISSING  https://turnerpinball.com/games/yukon-yeti/
2/3 cached
```

A URL counts as held if it is cached under its normalized form **or** as the `raw_url` of a document that redirected somewhere else — a hand-rolled `get()` loop misses that second case and refetches documents the cache already holds. The alias is matched normalized, like every other lookup here, so a trailing slash or odd host casing in your source list still resolves. It is the **most recent** fetch, though, so a document refetched through its canonical address stops resolving under the old one — at worst one redundant polite refetch.

It reads the same `url<TAB>query` TSV `web_fetch.py --from-file` takes, so one source list drives both steps. To fill the gaps, hand that same list to the fetcher — it skips what it already holds, so there is no miss list to pass along:

```bash
uv run python scripts/web_scrape/web_fetch.py --from-file sources.tsv --max-age 99999
```

The fetcher skips on **freshness**, not presence — `--max-age` defaults to 30 days, so without a wide window it will also re-fetch anything older than that. `have` ignores age entirely, because a document cached two years ago is still evidence you hold.

A URL that doesn't parse is reported as `INVALID` rather than missing, because it was never looked up. One malformed entry never aborts the run.

The tally goes to stderr and the exit status is non-zero when anything is missing or unparseable, so `have` also works as a precondition in a script. For a programmatic answer use `have()` in Python, which returns one `{"asked", "page", "stored_url", "error"}` record per URL in the order asked — the CLI prints for people, the functions return data, as with every other read here.

### `links`: a document's outbound links

```console
$ uv run python scripts/web_scrape/web_cache.py links https://www.sternpinball.com/manuals --ext pdf
159 unique outbound links
by extension: pdf:132  (none):27
132 shown after filtering
https://wp.sternpinball.com/…/ACDC_Pro_web.pdf    AC/DC Pro Manual Download File
…
showing 100 of 132 (--limit 0 for all; --ext/--host narrow better than truncation)
```

### Import: when fetching fails

Some sources won't be fetched — `ipdb.org` answers HTTP 403 site-wide to the fetcher, others sit behind a login or a Cloudflare challenge — while a person with a browser gets the same document fine. `web_import.py` takes the file that person saved and files it as evidence like anything else: content-addressed blob, extracted text, FTS-indexed, quotable and citable. This is the minority path, not a routine alternative to fetching; any type the cache understands can come in this way. See `--help` for `--title`, `--date`, and `--force`.

```bash
uv run python scripts/web_scrape/web_import.py flyer.jpg --url https://www.ipdb.org/images/4583/image-3.jpg --dry-run
uv run python scripts/web_scrape/web_import.py flyer.jpg --url https://www.ipdb.org/images/4583/image-3.jpg --text-file flyer.txt --query "mecatronics space shuttle flyer"
```

The rules that bind an import:

- **An imported row never pretends to be a fetch.** `imported = 1` on the document row and its audit row, and `http_status` stays NULL — no request was made. Query fetched-only evidence with `WHERE imported IS DISTINCT FROM 1`.
- **Store the bytes under the URL that serves them** — the image URL, not the viewer page that displays it. The file type is taken from the magic bytes, not the filename.
- **Words are mandatory — citable or findable.** A document nothing can find or quote is refused. Citable text comes from `--text-file` (recorded as `text_source = 'manual'` — a person is answerable for it) or the file's own handler (a PDF's text layer). An image is findable without a transcription: its OCR lands in `ocr_text`, searchable but never quotable. A scanned PDF imports with no text at all — `web_pdfocr.py` reads its sheets afterwards.
- **A reviewed transcription outranks a later re-extraction.** A refetch never replaces `manual` text while the bytes are unchanged; when the bytes did change, the new extraction wins loudly. Changing a transcription is a deliberate act through `web_import.py --force`.
- **Machine-read text is never imported as citable.** OCR run outside this tool used to come in under a `text_source = 'ocr'` label; that label is retired — machine readings belong in `ocr_text`, and the OCR pass produces them from the blob itself.

To cite an image, open the blob and read the words off the picture (`--dry-run` previews the import, including its OCR draft, writing nothing). A hand-typed transcription via `--text-file` is what makes an image quotable; keep the document's line structure — the quote gate's whitespace collapsing handles the rest.

## Finding & reading

### Search scopes

`search` narrows in three steps, and each step returns the units at that level with a match count. A term alone ranks documents; `--url` lists that document's matching sections; `--section` (or `--pages`, an inclusive sheet range) shows the matches themselves with `--surrounding-words` of context around each. The counts are the point — a term appearing 95 times across 35 sheets and a term appearing once no longer look alike, and they tell you how much to trust the snippet beside them:

```console
$ uv run python scripts/web_scrape/web_cache.py search "coil" --url <sonic-manual-url>
page 8                    1 match
page 9                    2 matches
page 10                   4 matches
…

$ uv run python scripts/web_scrape/web_cache.py search "coil" --url <sonic-manual-url> --pages 24-26 --surrounding-words 8
[page 25]
The Switch History screen displays the 24 most recent inactive-to-active switch transitions.
Coils - test virtually any coil, magnet, motor or light in the game. A screen will be displayed, listing
```

The section names are the ones `outline` prints and `section` resolves, so an address you read here is an address the other reads take: sheets on a paginated PDF, headings elsewhere, `metadata` in an assembled document's frontmatter. Unheaded text — above the first heading, or a document with no headings at all — lists as `(no heading)`, which `--section` accepts but `section` does not, since there is no heading for it to resolve. A window labelled `section boundary` is a match that runs past its section's end: returned whole rather than truncated, and deliberately unaddressable, because no single section name is true of it. Sections are listed in **document order, never ranked** — sheet numbers stay monotonic and every matching section is shown. Where there is only one section to choose, `--url` skips the list and returns the matches.

A **match count is matched phrases plus matched loose words**: `'"camel toes" bananas'` reads two phrase hits and five `bananas` as seven. Overlapping phrases merge into one, so `'"a b" "b c"'` over `a b c` counts one.

Match windows are **stored lines verbatim and unmarked**, so any part of one can be lifted into a cite's `quote` — unlike the global snippet, which elides with `…` and brackets its matches. `--surrounding-words` sizes a window in words rather than lines (a PDF sheet's lines are short and irregular, so ±3 lines means something different on every document) but whole lines still come out, keeping table rows intact. N is a cap on lines either side too, which only ever bites on blank runs: a blank line holds no words, so without it two matches a page apart would merge into one window of mostly whitespace. A window's padding never leaves the section it is filed under, so a window that sits inside one carries its locator. The exception is a match that itself runs past the section's end: that comes back whole and labelled `section boundary` rather than truncated, since a span missing the words it was found for would be worse than one without a locator. Overlapping windows merge and say how many matches they absorbed. `--limit` caps how many are shown and reports what it withheld.

**Search reads two tiers.** `text` is the citable layer (a document's own words); `ocr` is machine-read sheet ink from [the OCR pass](#pdfs) — findable, verified by rendering the sheet, never quoted. Every scope keeps the tiers apart: a document row carries each tier's own counts (`95 in 35 sections (text) · 12 in 4 (ocr)` — the asymmetry says whether OCR found anything the text layer missed), section lists and match windows interleave both in sheet order with each row labelled, and a `snippet (ocr)` label means the snippet is machine-read. The tier says where the words came from, not which is more accurate: on manuals whose text layer is mojibake (seven Stern manuals store headings as a cipher), the `ocr` tier is the only readable one. When any un-OCR'd PDFs remain, `search` says so on stderr rather than implying completeness.

Two documents can both match with no text match at all, since the index covers url and title as well: `url/title match, no text layer` is a document whose bytes are cached but unreadable — a scanned PDF the OCR pass hasn't reached — while `url/title match, 0 text matches` is an ordinary document that simply doesn't say the word.

### Search syntax

Units of a term AND together, and a **double-quoted run is one phrase**: `'"upper magnet" knocker'` asks for the phrase and the loose word, where `upper magnet` asks only that both words appear somewhere in the document. On this corpus that is 1 hit against 28 — worth reaching for whenever you know one exact caption and are guessing at the rest. The shell has to be told to keep the double quotes, hence the surrounding single ones. Every unit is sent as a quoted phrase, so FTS5 operator syntax in a term (`AND`, `OR`, `NEAR(…)`, `*`) is searched for literally rather than obeyed, and no term can raise a query error; an unbalanced quote runs to the end of the term, and the CLI shows the expression it ran.

`search` spans **every cached type** — web pages, PDFs, OCR'd images and video transcripts together. A non-web hit says what it is (`type:`) and how its citable text was derived (`text_source:`), so you know to weigh before quoting; web pages are the unlabeled common case:

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

### `outline` and `section`

`outline()` tells you where a long document's weight sits ("intro 2K, machine list 4K, 41 comments 32K") for a couple hundred chars; `section()` then pulls just the block you need. If a heading matches more than once, `section()` returns every matching block — ambiguity surfaces rather than silently picking one — and `outline()` correspondingly collapses that name to one row carrying `x2` and the summed size.

That collapse is what keeps the map usable on page-builder sites, where the meaningful labels are styled `<div>`s and the real `<h2>`s are UI chrome. A live product page came back with **100 rows, 38 after collapsing** — `RETIRED` twelve times, edition panels repeated once per responsive variant. Repeats collapse only when they sit in the same place in the tree (same ancestors, same level), so a name that recurs under different parents stays several rows. `--min-chars N` trims further by hiding blocks too small to be content (38 → 29 on that page), and says on stderr how many it withheld.

`section` matches a heading **exactly**, and separates the three reasons a name comes back empty instead of letting all three read as "not in this document": a heading that merely contains the name (`did you mean: 100th Anniversary`), a name that is only body text (`not a heading; appears as text in section(s): Additional Features` — the page-builder shape again), or genuine absence, which stays silent.

### What the stored text looks like

`pages.text` for an HTML page is the **whole document** — footers, nav, comments, forum replies, a manufacturer index kept in a `<select>` dropdown, etc.

The text is YAML-style frontmatter followed by the document as markdown:

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
- The **body** is the `<body>` as markdown, block-level only. The document's headings keep their **source** ATX levels (an `<h1>` is `#`) with nothing above them to outrank, so the document is well-formed markdown and `"in the Specifications section"` is a locator the text itself supports. Inline markup (`<a>`, `<b>`, `<em>`…) is deliberately not converted — its markers would land inside quotable spans — so the conversion never introduces `**` or `[text](url)` of its own. Table rows keep their pipes with empty cells preserved (`| Cavalier | 1979 | | … |` — the gap says "no month recorded"). Scripts, styles, SVG internals, HTML comments, JSON-LD and recognized cookie-consent widgets never reach the text; `<noscript>`, `<template>` and dropdown option text do.

The markdown's sections (`#`, `##`, …) are the document's own h1–h6 tags, and a section runs until the next heading at the same or higher level — so a parent section contains its subsections, and `outline()`'s char count for a parent includes its children's. Site chrome — header, footer, nav — often has no heading of its own, so like any other unheaded text it belongs to the section above it, or to no section at all when nothing precedes it. A locator naming a document's first or last section is the one worth a second look, since that is where chrome lands.

`metadata` and `body` are addressable through `section()` as well, but they are **frames this assembly adds**, not parts of the document: every HTML page has both, and a PDF, video transcript or OCR'd image has neither.

The `title` column is `og:title` → `<title>` → first `<h1>`, stored verbatim — no site-suffix stripping, because no separator heuristic can tell `… | Jersey Jack Pinball` from `Sirmo : Magic Screen`, where the separator joins two halves of one real title.

### Extracting everything a document knows

The ladder is for needle-driven reads — you have a claim and want its span. The inverse task also comes up: building a whole catalog entry from one document ("every gameplay feature, every credit on the new Godzilla's page"), where the document is the input and the target schema is the sieve. No needle helper enumerates that; read the whole stored text against the schema — `get()` for a typical document, or `outline()` + `section()` to walk a long one piece by piece.

**On a PDF that walk is by sheet.** `section(url, "page 41")` returns that sheet's text. A sheet with no extracted text gets no block in `section`.

For a long document, do the read in a subagent so only results enter the main session's context: its prompt is the schema ("read this document's text; for every gameplay feature, credit, spec and date it states, return the verbatim span and the section it sits in"), its return is field → span pairs. The same applies to whole-document questions ("do any of the 41 replies dispute the production count?") — don't pull 38K chars into the main session to extract two sentences. Whole-document extraction is what makes either read reliable: credits live in footers and specs in tables, and both survive with structure intact. Each extracted span then becomes its own cite.

## Citing & quoting

The cache is where the evidence in a [data patch](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatches.md) comes from (patches are authored in the [flippatch](https://github.com/deanmoses/flippatch) repo). A cached document becomes a cite in mapping form:

```yaml
cite:
  ref: https://www.tilt.it/flipper_pinball/ipdb/cea # the document URL
  quote: "Fly Man – ss – 1p" # verbatim, from web_cache.quote()
  locator: in the 1978 machine list # optional: where in the document it sits
```

- **`quote`** is the only field that must match the source word for word — it is what `make verify-quotes` checks. Take it from `web_cache.quote()`.
- **`locator`** is freeform for a web page, and says where the excerpt lives so a reader can find it.
- **`note:`** is the edit summary — rationale beyond the evidence, uncertainty, why the value follows. It is never a verbatim excerpt, and a cite carrying a quote usually needs no note at all.

`cite:` also takes a list, and the policy for AI-authored patches is to corroborate a fact from as many separate sources as possible.

### From a hit to a locator

`quote` is the starting point for a patch's **`cite.quote`** — the verbatim span, not the `note:` above. It labels each hit with the section the span sits in, so **one command produces a whole cite** — the `quote` and the `locator` — instead of quoting first and then hunting for where the words were:

Matching is forgiving but the return is not: finding a hit collapses whitespace runs, straightens smart quotes and ignores case, so a phrase spanning a stored line break is still found — while the text handed back is always the stored lines verbatim. That asymmetry is why every hit you lift still verifies.

```console
$ uv run python scripts/web_scrape/web_cache.py quote https://en.wikipedia.org/wiki/Taito_of_Brazil "Mecatronics"
[Solid state electronics]
- Space Shuttle (nearly identical to Space Shuttle (Williams Electronics, 1984) made under the label 'Mecatronics')
```

The label is a name `section()` accepts, so it doubles as the way to pull the span's surroundings. `metadata` means the hit is in the frontmatter — an `og:description` rather than the document's own prose, which is usually a signal to keep reading for the body's wording. A hit above the first heading has no label, because there is no section to name — and on a document whose markup has no headings at all, nothing is labelled. A match that itself crosses a section boundary is likewise unlabeled: no single name is true of it.

The label names the **match**, never the widened window around it, so **a hit never leaves the section it names** and any span you lift out of one can carry that hit's locator — `--context` changes how much you see, never where the evidence is said to live. The cost is that a large `--context` gives you less than ±N lines near a section edge; `section()` shows the whole block when that's what you want. The label is also only as good as the document's own markup: a site whose tab labels are real `<h2>`s yields locators like `$7,995` — faithful to the document, and no more wrong than the outline it comes from.

### Two ways to pull a span

`quote` and a scoped `search` both return matched text from one document, labelled with the section it sits in, verbatim enough to lift into a cite. They differ in what you hand them and what rides along:

|              | `quote <url> "<needle>"`                           | `search "<term>" --url <url> --section "<name>"`   |
| ------------ | -------------------------------------------------- | -------------------------------------------------- |
| takes        | one literal needle                                 | AND'd units; a double-quoted run is one phrase     |
| widens by    | `--context N` lines                                | `--surrounding-words N`                            |
| also reports | the PDF page(s) a hit sits on, and the blob path   | per-window match counts, and `--limit` withholding |
| available as | `quote()`, the form flippatch's quote gate imports | `search_matches()`                                 |

### Weighing a quote: text_source

Every document row carries a **`text_source`** label saying what turned the bytes into its _citable_ text: `html` (the markdown conversion), `pdf` (the document's own text layer), `vtt` (a caption track), or `manual` (a human transcription). These are not equally trustworthy — a PDF's text layer is what the document contains, captions a guess about audio — so weigh a quote by its label. Machine-read pixels carry no label because they are never citable text at all: they live in the separate `ocr_text` column, and a row holding only OCR (an image, a fully scanned PDF) has `text_source` NULL.

`text_source` is independent of `rendered` (which says where the **bytes** came from): a rendered page is still `text_source = 'html'`. Rows cached before the column existed are NULL too.

### Quote-less cites: when the fact is only in pixels

A checkmark in a PDF feature matrix is vector art; there is no text to quote. The honest cite is the analogue of a locator-only book cite: `ref` the document URL, `locator` the PDF document page (`printed page 17, PDF document page 27`), `note` the visual observation — `quote:` is optional in the cite grammar. **Do not quote the feature's row label instead**: it is verbatim and it is in the document, but it establishes the row, never the edition column — a true string doing work its context doesn't support, which is the column-splice forgery in a new costume. flippatch's `RULE_QUOTE_SUPPORTS_CLAIM` would rightly reject it; the point is not to author it. (The authoring-side rules live in flipcommons' [DataPatchAuthoring.md](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatchAuthoring.md); this paragraph is the reading-side statement of the same policy.)

### The verify gate

Every quote is machine-checked. flippatch's `make verify-quotes` gate is **fast, deterministic and offline** — it requires the cite's `quote` to be a verbatim substring of `pages.text` once smart quotes are straightened and whitespace runs collapsed, and it never does a live fetch. A quote needs to verify for one brief window: from the moment a session authors the patch to the moment it's committed. After that the quote is shipped and immutable — the gate globs pending `patches/[0-9]*.yaml` only, so a change to how this cache extracts text can never break a shipped patch.

**If a quote doesn't verify, the presumption is that the quote is wrong** — changing cached text to match a claim is a deliberate human act, never a side effect of making a check pass.

See DataPatches.md for the full cite grammar (a URL cite needs its website root seeded first; a known-scheme URL like `ipdb.org` cites as `scheme:id`), and [DataPatchAuthoring.md](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatchAuthoring.md) for the authoring rules on quoting.

## Document types

What each non-HTML type gives you once cached, and how it can mislead.

### PDFs

PDFs are fetched like any other URL: detected by content type or `%PDF-` magic bytes when a server mislabels them, stored as the raw bytes the server sent. The blob is the PDF file itself. Title and `last_updated` come from the document's own metadata — a real date it states, or null.

PDFs with a text layer are extracted to text — searchable and quotable like any document. Page boundaries are stored as lone `\f` lines, so `get` and `section` show them; `quote` drops them, so a span you lift never contains one.

`quote` hits name the PDF document page(s) on which they sit, and print the blob's path:

```console
$ uv run python scripts/web_scrape/web_cache.py quote <manual-url> "Bottom Pop Bumper"
blob: /…/pinexplore/ingest_sources/web/raw/9a83…0721.pdf ⬅️ stderr: a fact about the document
Bottom Pop Bumper
pdf document pages: 27
```

Facts about the **document** go to stderr — the blob path, and `pdf document pages: unavailable` on a row whose text predates the page markers — so stdout is the hit list and nothing else. Facts about a **hit** ride that hit on stdout, since they vary per hit and the two streams give no ordering guarantee to pair them by: a redirected `quote` holds each span with its `[section]` label and page line, not bare text.

Looking at the text is usually not enough. For example:

- **Printed page numbers**. To construct a citation you need the page number printed onto the sheet. The text isn't a reliable way to get that page number. The hit's `pdf document pages` is the PDF index of the sheet, not any page number printed on it.
- **Tables**. The text flattens a table into a column of cells with nothing tying rows to headers. You need to see the actual table.
- **Visual elements**. You might need to see a checkmark visually embedded in the page.

For these, look at the page visually. Claude Code can render a single page straight out of the blob: `Read(<blob path>, pages="27")` returns page 27 of a 5MB manual as an image without touching the rest.

A hit whose `--context` window spans multiple pages lists every page the shown text touches (`pdf document pages: 26, 27`).

**A word drawn as artwork is found through the OCR tier.** A table cell, a diagram callout, a label baked into a figure — none of it is in the text layer, and before the OCR pass such a needle returned `no matches`, indistinguishable from a document that never says it. Now `web_pdfocr.py` rasterizes every sheet (Quartz) and reads it (macOS Vision) into `pages.ocr_text`, so `search` finds the ink and names the sheet: in the Galactic Tank Force manual, `UPPER MAGNET` lives only inside a diagram image, and a scoped search returns `page 41 (ocr)` where the text tier returns nothing. The division of labor: **OCR provides findability, not citability** — measured line-exactness against ground truth is ~88%, and a wrong-but-plausible reading (`1/16"` for `11/16"`) would survive quote verification. So an `(ocr)` hit's payoff step is `Read(<blob>, pages="41")`: render the sheet, read the ink, and cite what you read — a quote-less cite (`ref`, a `locator` naming the sheet, a `note` recording what was seen). `quote` never answers from the OCR tier. `page N` counts sheets from the front of the file in both tiers — the two columns are asserted to agree at write time — but the folio printed on the sheet is different ink.

A fully scanned PDF has no text layer at all: the blob caches, no text is extracted, and the fetch warns — but once OCR'd it gets a page map (`outline`/`section` answer from the OCR tier, labelled `(ocr)`) and every scope finds its words. `quote` on such a row says the matches are OCR and to render rather than quote. Until the pass runs, the row cannot say whether the document is image-only or whether extraction was merely unavailable on the host that fetched it (a missing poppler, a document poppler couldn't read); `search` prints a coverage line while any PDF remains un-OCR'd. OCR is macOS-only and separate from fetching: a refetch on any host keeps the stored OCR while the bytes are unchanged, and clears it when they changed so the next pass re-reads them.

### Images

Images — JPG, PNG — sometimes contain printed evidence: a scanned flyer, a photographed manual page, a screenshot of a page that won't scrape. We store the image's raw bytes, and OCR (macOS Vision — no system binary, no model download, no network) reads them at fetch time into `ocr_text`: findable by every search scope, labelled `(ocr)`, never citable. To cite an image, open the blob and read the words off the picture — the same render-and-cite step a PDF sheet gets, except the blob already is the picture. A picture with no legible text prints a loud warning rather than silently caching a blank document.

The only way an image acquires a citable text layer is a human transcription through `web_import.py` (`text_source = 'manual'`), where a person is answerable for the words. The image's `title` and `last_updated` stay null unless the importer supplies them. OCR is macOS-only; on another platform the bytes still cache with a warning — and a host that can't OCR never blanks a reading a Mac already stored.

### Video transcripts (YouTube)

A YouTube URL routes automatically to the caption-track transport: yt-dlp pulls the video's metadata and best caption track, the raw `.vtt` becomes the blob and the parsed transcript the document text — searchable and quotable like any document. Every URL shape (`watch?v=`, `youtu.be/`, `/shorts/`, `/live/`, `/embed/`) collapses to the one canonical `watch?v=<id>` cache key; `title` is the video title, `last_updated` its upload date. Manual subtitles beat auto-captions, and among auto-captions the original spoken language beats YouTube's machine translations; the `fetches` log records which track was taken. Timestamps stay in the `.vtt` blob for a citation's `locator:` moment.

A video with **no captions at all** (common for livestream archives) logs a loud warning and stores no document — there is no transcript to quote. Check the video's description for the written source it usually links, and cite that instead.

## Using from Python

Every capability is also a function in `web_cache.py`.

| CLI                      | Python                                                                                            |
| ------------------------ | ------------------------------------------------------------------------------------------------- |
| `search "<term>"`        | `search()`                                                                                        |
| `search --url`           | `search_sections()`                                                                               |
| `search --url --section` | `search_matches()`                                                                                |
| `quote`                  | `quote()` — plain spans; `quote_hits()` adds each hit's `heading` and `pdf_document_page_numbers` |

The rest have the same name in both.

The CLI prints; functions return structured data. The OCR tier rides that data rather than living only in the CLI: a `SearchHit` carries each tier's own counts (`matches`/`sections` and `ocr_matches`/`ocr_sections`) plus `snippet_tier`; `SectionHit`, `MatchHit` and `OutlineEntry` each carry a scalar `tier`; and `section()` returns `{text, tier}` blocks, so a caller always knows whether it is holding citable text or machine-read ink.

## Architecture

The **SQLite database is the system-of-record**; `make explore` materializes it into the `web_pages` / `web_fetches` DuckDB tables (via `03_raw_web.sql`) so web evidence can be joined against the IPDB/OPDB/pindata tables.

```text
ingest_sources/web/          ← durable (R2-backed, gitignored), NOT in git
  cache.sqlite                 system-of-record: pages + fetches + pages_fts (FTS5)
  raw/<sha256(raw)>.<ext>      raw document blobs, content-addressed
                               (kept for re-extraction; citations verify
                                against pages.text, never blobs)

scripts/web_scrape/
  web_cache.py               store: schema, URL normalization, upsert, and
                             the reads (see Using from Python)
  web_http.py                transport: GET, content-type gate, wire-safe URLs
  web_video.py               transport: YouTube caption tracks via yt-dlp
  content_types/             one handler per document type (the registry)
  web_ocr.py                 OCR backend for images (macOS Vision)
  web_pdftext.py             PDF text backend (poppler pdftotext, reading order)
  web_pdfocr.py              CLI + backend: OCR PDF sheets into ocr_text
                             (Quartz raster + Vision, macOS-only)
  web_render.py              headless-render fallback for JS-only pages
  web_fetch.py               CLI + per-URL orchestration (writes sqlite + raw/)
  web_import.py              CLI: file a hand-obtained file as evidence

sql/
  03_raw_web.sql             ATTACHes the sqlite, materializes web_pages/web_fetches
```

Two tables plus two FTS indexes (schema and invariants documented in [`web_cache.py`](../scripts/web_scrape/web_cache.py)): **`pages`** is current state per normalized URL — the extracted `title`/`text`/`last_updated`, the machine-read `ocr_text` tier, plus provenance flags `rendered` (see [JS-rendered pages](#javascript-rendered-pages)), `text_source` (see [Weighing a quote](#weighing-a-quote-text_source)) and `imported` (see [Import](#import-when-fetching-fails)). The OCR tier has its own FTS table (`ocr_fts`) rather than a column on `pages_fts`, so each tier ranks in its own bm25 space and OCR'ing a document can never depress its text-tier rank. **`fetches`** is the append-only audit log: one row per fetch, with the `search_query` that drove it, the `content_sha` it saw, and a `changed` flag. Blobs are content-addressed, so every distinct version of a document stays on disk.

### Sync

The cache is shared between dev machines through Cloudflare R2. Moving evidence between machines — publishing what you fetched, restoring the corpus on a fresh checkout — is **a human's job, never the AI**.

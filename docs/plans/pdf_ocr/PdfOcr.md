# PDF OCR

This is a statement of product direction, not a detailed technical plan.

As of Aug 5 2026, 78% of our 58 cached PDFs are searchable. However:

- **Dark docs**. 13 (22%) of docs are fully dark.
- **Dark sheets**. 1,242 of 4,489 sheets (28%) yield no text — including the mixed cases like AFM, where 157 of 163 sheets are images inside a document that technically "has text".
- **Dark content within sheets**. Only 10% of sheets are pure text; 62% carry both text and images. Critical info is often in those images, like model names as section and table headers.

This dark content is absent from `search`. `search` is how consumers figure out whether relevant information exists and where it lives. Meaning a huge chunk of our PDF content is effectively missing.

We want to use OCR to light up the dark content.

The division of labor: OCR provides findability, not citability. AI sessions use `search` to locate the sheet, then Claude Code's `Read(blob, pages=N)` to render that sheet. Then cite what they read off the image.

## The flow

The flow is the same as today.

### Step 1 — search

Use `search` to find relevant documents. OCR results show up alongside existing results:

```terminal
$ web_cache.py search "UPPER MAGNET"

url: https://www.spookypinball.com/.../ACNC-Manual.pdf
type: pdf
snippet: … 1 [Upper] [Magnet] Pattern Different patterns used for the [upper] [magnet] Original …

url: http://s4.american-pinball.com/.../Galactic-Tank-Force-Game-Manual.pdf
type: pdf
snippet (ocr): … Table 3-18 Coil Positions … [UPPER] [MAGNET] … KNOCKER …
```

`snippet (ocr)` ⬅️ machine-read from the page image — not citable

### Step 2 — match

Find matches within a specific document. The command is currently called `quote`; we're generalizing it to `match`. Same command you'd run on any hit, now also looking in the OCR column, and telling you the sheet:

```terminal
$ web_cache.py match <gtf-url> "coil"

blob: /…/raw/9a83…0721.pdf                  ⬅️ stderr

Check alignment VUK coil
pdf document pages: 7

(ocr)                                       ⬅️ machine-read from the page image — not citable
Table 3-18 Coil Positions
UPPER MAGNET     KNOCKER
pdf document pages: 41
```

We also rename the Python `quote()` / `quote_hits()` to `match()` / `match_hits()` and change all the various docs in both repos including `AGENTS.src.md`.

Note that this does not affect flippatch's `make verify-quotes` gate because it does NOT use `quote()`; it goes through `get()` and reads the text key by name.

The rename is not negotiable; do not ask to keep `quote`.

### Step 3 — render

Claude Code AI session renders page 41 and reads it. `Read("/…/raw/9a83…0721.pdf", pages="41")`. It reads UPPER MAGNET off the actual table, and that is what goes in the patch's `quote:`.

## Architecture

We will OCR every sheet. OCR content must be stored separately from the PDF's text layer that's what quotes are verified against, and also duplicated content would garble spans.

- `pages.text`: the citable text layer provided by the source (be it PDF, HTML or other)
- `pages.ocr_text`: the OCR text we generate (be it for a PDF, image or other)

We retire `text_source = 'ocr'`. Not the field, but the `ocr` value.

### Pagination

#### Page separator

We'd separate sheets with `\f`, just like `pages.text`.

#### Pages in dark documents

A fully dark document has page markers in `ocr_text` and none in `text`, making the two columns disagree about whether the document is paginated. `outline`/`section` need a rule for which column defines `page N`. The rule: `text` is primary. It comes from `text` when it contains markers, and only otherwise `ocr_text`.

Incidentally, this gives those 13 dark documents a page map they don't have today.

#### Pages in non-dark documents

When both columns are paginated they must agree on sheet count. That's a cheap assertion at write time and the only thing standing between a rasterization/extraction mismatch and page 41 meaning two different sheets in the two columns.

### Search

Search would include `pages.ocr_text`. Search must not allow ranking and snippets to cross tiers.

A search hit returns a `snippet` from `pages.ocr_text`. This help the consumer determine whether that hit is actually useful or not. We also should somehow let them know that they can't rely on it to be citable... something like a `tier: "text" | "ocr"`.

When the same document has hits on both tiers, don't dedup; return both

### Match

When the same sheet has hits on both tiers, dedup; don't return the OCR one.

### Staleness

When content is re-fetched, we re-OCR. However, OCR is macOS-only. A row can be refetched on a host that can't OCR — new bytes, text re-extracted. Probably the way to do this is that `ocr_text` clears when `content_sha` changes, then on Macs re-OCR.

## Proving this out

The premise -- "we will OCR every sheet" -- needs to be proved out before we commit to OCR'ing our thousands of existing sheets.

Some of the things we want to prove out first (not exhaustive):

- Does Vision read a table baked into a diagram at high enough quality to understand it?
- Is Vision is fast enough?

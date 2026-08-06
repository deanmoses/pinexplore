# PDF OCR

We will OCR the PDFs in web cache to make them searchable. Here's why. As of Aug 5 2026, 78% of the 58 cached PDFs were searchable because of their text layers. The problems:

- **Dark docs**. 13 (22%) of docs are fully dark.
- **Dark sheets**. 1,242 of 4,489 sheets (28%) yield no text — including the mixed cases like AFM, where 157 of 163 sheets are images inside a document that technically "has text".
- **Dark content within sheets**. Only 10% of sheets are pure text; 62% carry both text and images. Critical info is often in those images, like model names as section and table headers.
- **Corrupt text**. Text that _is_ extracted but is unsearchable gibberish. Seven Stern manuals store their headings in subset fonts with no ToUnicode map, so the text layer holds a Caesar-shifted cipher — Monopoly sheet 32 extracts as `*R\x03 7R\x03 &RL O\x03 0HQX` where the page reads "Go To Coil Menu". That is ~360 sheets across those seven, landing on headings specifically.

This dark content is absent from `search`. `search` is how consumers figure out whether relevant information exists and where it lives. Meaning a huge chunk of our PDF content is effectively missing.

We want to use OCR to light up the dark content.

The division of labor: OCR provides findability, not citability. AI sessions use `search` to locate the sheet, then Claude Code's `Read(blob, pages=N)` to render that sheet. Then cite what they read off the image.

## The flow

### Step 1 — global search

A Flippatch AI session uses `search` to find relevant documents. OCR results show up alongside existing results:

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

### Step 2 — search specific document(s) (optional)

Once the Flippatch AI session has found a set of documents it cares about, it could uses `search` to search them deeper, or with different terms.

`search` would take a list of documents to restrict the search to. It would return more results for each document.

It may even be able to restrict the search to specific pages or page ranges per document?

### Step 3 — render

The Flippatch Claude Code AI session renders page 41 and reads it. It reads UPPER MAGNET off the actual table, and that is what goes in the patch's `quote:`.

#### Enhanced render - not for v1

Handing the PDF straight to Claude Code's `Read` tool — `Read("/…/raw/9a83…0721.pdf", pages="41")` — works, but at roughly 850x1100 it is the weakest link in the chain. Measured on an Iron Man parts sheet, that path renders a bushing spec as `Bushing, 16" a ID X 281" oOD X 187`; the same sheet rendered at scale 4.0 reads `Bushing, .16" ø ID X .281" ø OD X .187"`. Decimal points and `ø` glyphs are exactly the characters a parts citation turns on, so it is worth the better render.

So `web_cache.py` should expose an **on-demand render**: give it a url and a page, it rasterizes that sheet to a PNG in a temp location and prints the path, which the session then `Read`s. Same Quartz path the OCR uses, ~35ms per sheet.

Render at **scale 2.0** by default — the same scale the OCR pass uses, so there is one setting and one code path. At 1224x1584 the image passes to the model without downsampling and costs ~2,600 tokens. That is roughly double what `Read`-on-PDF costs, and it is the right place to spend: `search` is the cheap net over thousands of sheets, and the render lands on the one or two sheets actually being cited.

Going higher buys less than it looks. The ceiling is not the rasterizer but the vision input, which caps near 1550px on the long edge, so scale 4.0 is downsampled back to 1550x2000 and costs ~4,100 tokens for it. On both sheets tested — an Iron Man parts page and an unseen AFM assembly page — scale 2.0 already resolves the details that matter (`Rivet, .089 ⌀D, 11/32" L, Roll`, `Bushing, .16" ø ID X .281" ø OD X .187"`). So make scale a parameter and step up to 4.0 only when a specific glyph is genuinely contested, not by default.

Render on demand rather than storing the images. Storing every sheet would be ~13GB against an R2-backed cache to save 35ms of work, and the blobs we already keep can regenerate any sheet at any scale whenever it is actually wanted.

ENHANCED RENDER IS NOT FOR V1.

### Step 4 — confirm against the text layer (conditional)

If step 2 labelled the hit `(ocr)`, skip this: the words are only ink, and the cite is quote-less — `ref`, a `locator` naming the sheet, and a `note` recording what was seen.

However, if the hit was not labeled `(ocr)`, the words are in the text layer, and you will need to quote it. A reading of the ink can differ from the stored text — decimal points, `ø`, hyphenation across a line break. Use `section <url> "page 41"` to get that sheet's stored text verbatim.

### `quote` is not needed for PDFs

For PDFs, `search` and `section` replace everything that `quote` was once used for. `quote` is still needed for HTML documents, though, because we don't provide a FTS way to search within a HTML document, and `quote`'s needle-driven read beats dumping a 60K HTML document.

## Architecture

We will OCR every sheet. OCR content must be stored separately from the PDF's text layer that's what quotes are verified against, and also duplicated content would garble spans.

- `pages.text`: the citable text layer provided by the source (be it PDF, HTML or other)
- `pages.ocr_text`: the OCR text we generate (be it for a PDF, image or other)

We retire `text_source = 'ocr'`. Not the field, but the `ocr` value.

### `ocr_text` shall not be citable

It is tempting to allow quote verification against OCR text -- OCR reads the corpus faithfully in aggregate (97.9% of the text layer's vocabulary). However, aggregate fidelity is the wrong test. Citation needs the exact character sequence, and measured at line grain against sheets where both columns exist, only **88.2% of corresponding OCR lines are character-exact**. The other 11.8% look like this:

```text
text: 11/16" Single Groove Post (Clear)
ocr :  1/16" Single Groove Post (Clear)

text: (holds the Display ROM Loc: ROM0)
ocr : (holds the Display ROM Loc: ROMO)
```

Neither reads as damaged. One is a different valid spec, the other a zero silently become a letter. A gate that accepted either column would verify `1/16" Single Groove Post` as verbatim-correct and land a wrong measurement in the catalog — and nothing downstream can separate the 88% from the 12% without looking at the page, which is what step 3 already does.

Reading order compounds it. Vision emits text in spatial order, and adjacency between corresponding lines survives only 68–86% of the time, so a quote spanning two lines can splice content that is not contiguous on the sheet.

Quote verification therefore happens against `text` only.

Note this cuts the other way on the mojibake sheets, where `text` is a cipher and `ocr_text` is the only readable version — but the answer there is still render-and-cite, not cite-the-OCR.

### Pagination

#### Page separator

We'd separate sheets with `\f`, just like `pages.text`.

`page N` counts sheets from the front of the file, not the folio printed on the page. GTF sheet 41 prints "31". `search` and the render step both address by sheet so they agree with each other, but anyone cross-checking against the printed number will be off by the front matter.

#### Pages in dark documents

A fully dark document has page markers in `ocr_text` and none in `text`, making the two columns disagree about whether the document is paginated. `outline`/`section` need a rule for which column defines `page N`. The rule: `text` is primary. It comes from `text` when it contains markers, and only otherwise `ocr_text`.

Incidentally, this gives those 13 dark documents a page map they don't have today.

#### Pages in non-dark documents

When both columns are paginated they must agree on sheet count. That's a cheap assertion at write time and the only thing standing between a rasterization/extraction mismatch and page 41 meaning two different sheets in the two columns.

### Search

Search would include `pages.ocr_text`. Search must not allow ranking and snippets to cross tiers.

A search hit returns a `snippet` from `pages.ocr_text`. This help the consumer determine whether that hit is actually useful or not. We also should somehow let them know that they can't rely on it to be citable... something like a `tier: "text" | "ocr"`.

The tier says where the text came from, not which one is more accurate. On the ~360 mojibake sheets the relationship inverts: the `text` tier is a cipher and the `ocr` tier is the faithful reading. So the label must stay descriptive — `ocr` means "machine-read, verify by rendering the sheet" — and must not editorialize into "lower quality", which would send consumers away from the only tier that can find those headings. Step 3 renders the sheet and cites from the image either way, so citability is unaffected.

When the same document has hits on both tiers, don't dedup; return both

#### Detailed search design

Today `search` returns one row per document with one snippet, and has no concept of a sheet. A term appearing 99 times and a term appearing once look identical, and FTS5 picks the window by its own rule: searching `coil`, the top hit is the 175-sheet Sonic manual and the snippet is a table-of-contents line. That is survivable today because `quote` resolves it on the next rung. It stops being survivable with OCR, whose whole value is _which sheet do I render_.

So index at **segment** grain rather than document grain: a derived, rebuildable table holding each document's segments, with a separate FTS index per tier so BM25 never ranks the two against each other. `pages` stays the system-of-record; the segment table is a cache, never cited against.

Two segmentation rules to start:

- **PDF → one segment per sheet**, split on the `\f` markers both columns already carry, addressed as `page N`.
- **Everything else → one segment for the whole document.** A legitimate case, not a placeholder — search behaves identically across types, and a finer rule can be added later without touching the schema.

HTML is excluded for now. Its unit would have to be the leaf block (heading to next heading of any level), since `section()`'s blocks nest and indexing those would enter the same text two or three times. Leaf blocks partition correctly but make poor index units — median 174 chars, 35% under 100 — and evening them out needs a size-merging knob. Only 3 HTML pages in the corpus exceed 15K chars with no headings, so revisit on evidence.

Segment-grain BM25 is what makes "the best sheets" definable: the same ranking function, finer grain. Return each document's best segments, capped at 5, one snippet each, labeled with address and tier:

```terminal
$ web_cache.py search "coil"

url: https://marketing.jerseyjackpinball.com/sonic/Sonic_Manual_10_July_2026.pdf
type: pdf
page 60: … Flipper [coil] resistance should read 4.2 ohms across the …
page 102: … replace the [coil] stop before it damages the plunger …
page 141 (ocr): … Table 6-2 [COIL] POSITIONS … UPPER MAGNET … KNOCKER …
+ 94 more matches on 31 other pages
```

**Best segments, never the first** — document order returns the parts list and the table of contents, which is the failure this fixes. The cap is on segments, not matches; a sheet matching repeatedly still contributes one snippet. Tier labeling rides the segment, so one document can show both tiers.

This also keeps `search` FTS end-to-end. The alternative — document-level FTS plus the literal matcher for labeled snippets — breaks down because FTS ANDs tokens anywhere while the matcher needs a contiguous phrase, so a large share of multi-word hits yield zero spans. `quote` stays the literal-span tool, which is its job.

### When OCR happens

Here's the ways that OCR can get kicked off:

1. We show a coverage line on `search` when the corpus has un-OCR'd PDFs, so a thin result set says so rather than implying completeness.
2. We provide a manual command that consumers can run in ⬆️ that case. Default is to run against all outstanding docs, but there's also a way to run against 1 doc, which I think we'll use for testing more than anything else.
3. `fetch` spawns detached process, if that is a simple thing to build

#### Failed OCRs

There are two failure mechanisms here, and they need different answers.

**Document-level: Vision permanently chokes on a document.** It stays in the gap forever and gets retried on every subsequent fetch. Either mark the attempt on the row (an ocr_attempted_at or a failure flag, so the gap query can skip what has already failed and a --retry-failed flag can bring it back) or accept perpetual retry.

For v1 we accept perpetual retry. YAGNI.

**Sheet-level: a single sheet fails inside a document that otherwise succeeds.** This is the one that actually happens. `Exception thrown when attempting to allocate memory` and `Too many symbols in JBIG2 symbol dictionary` both fire mid-document and are non-fatal, so the document is written as a success with a hole in it. JBIG2 appears in 13 of the 58 PDFs, including fully dark ones OCR exists to rescue. Retry does not address this at all — there is nothing in the gap to retry, because the document completed.

Probing the outcome of every sheet individually — raster refused / Vision errored / nothing legible — found **no swallowed failures**. Elvis (185 sheets), Lord of the Rings (211) and the Centaur schematics (15) all came back clean despite logging those errors; the errors are noisy but fully recovered. The only empty sheets in the sample are Time Machine's p10, p34 and p78, which are the three genuinely blank pages `web_pdftext.py` already documents. So the measured 0.4% blank rate is real blankness, not hidden loss.

That makes per-sheet outcome recording cheap insurance rather than a correctness fix. Worth doing — it costs a few lines and nothing at query time, and it is what lets the coverage line on `search` vouch for its own number if a future document does fail mid-run — but it is not a blocker.

### Staleness

When content is re-fetched, we re-OCR. However, OCR is macOS-only. A row can be refetched on a host that can't OCR — new bytes, text re-extracted. Probably the way to do this is that `ocr_text` clears when `content_sha` changes, then (only on Macs) re-OCR.

## Document-scoped `search`

We will provide an option on `search` to scope it to a single document. This will be better than `quote` for many scenarios:

- **Tokenized vs literal**. FTS ANDs tokens anywhere in a segment; `quote` needs a contiguous phrase. That's the divergence I measured earlier — for "upper magnet" and "haggis closed", a large share of FTS hits yield zero literal spans. So scoped search finds relevant sheets that `quote` structurally cannot, which today is a real hole: if you don't already know the exact wording, `quote` gives you nothing.
- **Ranked vs exhaustive**. Scoped search would rank sheets and cap. `quote` returns every span in document order — 97 of them for coil on the Sonic manual. That's correct for citation work (you need to see them all, and document order keeps page numbers monotonic) and poor for "where should I look."
- **Snippet vs verbatim**. `quote` returns stored lines verbatim, which is why every hit verifies. FTS snippets are constructed with elisions and bracket markers — never citable, by design.

Document-scoped `search` would return more results (all results?) than the limited # per-document that global `search` does.

### Page-scoped `search`

We've determined that outline/section is almost useless on a PDF. You mostly get Page 1...N. To actually find content in a document, you have to search. The scenario I'm thinking of is, say you find a promising table at page 45 that tells you you're in the right area, a comparison of models. Now you want to search for terms you hope to find around model comparison, in pages 40-50.

Something like `--pages 40-50`. It would return results from both the text and OCR tiers.

Cheap to build on the segment table — the filter is a `WHERE` on an indexed integer column, and prototyping it against the real index found no performance or ranking problem. Four things it needs:

- **`--pages` requires a document scope.** A sheet range across documents is meaningless; make it a validation error rather than something silently applied to every hit.
- **Filtering happens after ranking, and that is correct.** BM25's term statistics come from the whole index, so a term that is rare corpus-wide stays rare inside the range. Ordering among the surviving sheets is unaffected. Don't "fix" this by building an index per range.
- **Out-of-range is not an error.** `--pages 40-500` on a 58-sheet document returns what exists.
- **The sheet-vs-folio distinction bites hardest here**, because this is the one rung where a human types a page number rather than copying one out of search output. See [Page separator](#page-separator).

## Implementation notes

Traps and mechanics that cost real time to discover. None are visible from the API surface.

### Page markers

**Splitting on `\f` yields one more element than there are sheets.** poppler terminates _every_ page including the last, so `text.count("\f")` is the sheet count while `text.split("\f")` leaves a trailing empty string to discard. Getting this backwards turns every paginated document into an off-by-one and makes the sheet-count assertion fire on the whole corpus. Emit `ocr_text` the same way — a marker after every sheet, including the final one — so both columns split identically.

**A blank sheet still contributes its marker.** Dropping it shifts every later sheet's ordinal, which is the one thing the markers promise. `web_pdftext._normalize` already does this deliberately; match it.

### Segment addressing

**Store the sheet ordinal as an integer column, not just the rendered address.** `page 41` does not compare numerically, and [page-scoped search](#page-scoped-search) needs a range filter.

**Never label an unpaginated text layer `page 1`.** A PDF whose `text` holds no form feeds is not a one-page document — it is a document with no page information. Emitting a single segment addressed `page 1` silently mis-addresses every hit in it: the cached Jurassic Park manual has zero markers across 103 real sheets, so a hit on sheet 60 would report as page 1 and `--pages 1-5` would return the whole document. Emit one whole-document segment with a null ordinal instead, exactly as for a non-PDF. The pagination rule in [Pages in dark documents](#pages-in-dark-documents) decides which column supplies the ordinals; this is what the other column must do when it has none.

**`max(page)` is not the sheet count.** Empty sheets contribute no segment, so a document whose last sheet is blank tops out below its true length — TAG's text tier ends at 59 of 60. Read sheet counts from the marker count, never from the segment table.

### Rasterizing with Quartz

**Fill the bitmap context with white before drawing.** A fresh `CGBitmapContextCreate` is transparent, and Vision reads dark-on-transparent as nothing at all — the sheet comes back silently empty rather than erroring, which looks exactly like an image-only page with no legible text.

**Use `CGPDFPageGetDrawingTransform` against the crop box, and swap width/height for 90/270 rotation.** Landscape schematics are stored rotated; without this they render off-canvas or clipped. The transform also handles a crop-box origin that is not at zero.

**Hoist `CGPDFDocumentCreateWithProvider` out of the page loop.** Creating it per sheet reparses the whole blob every time, and the AFM manual is 65MB.

**Hand Vision the `CGImage` directly** via `VNImageRequestHandler.initWithCGImage_options_`. Encoding to PNG and back costs ~15x the render time and gains nothing.

**Don't build a thread pool.** Vision already saturates the machine internally; a pool measured 8% at best and adds failure modes for nothing.

### Failure signals

**CoreGraphics logs decoder errors to stderr that are non-fatal.** `Exception thrown when attempting to allocate memory` and `Too many symbols in JBIG2 symbol dictionary` both appear mid-document and are fully recovered — every sheet in the documents that logged them came back complete. Do not wire stderr noise to a failure path; JBIG2 is in 13 of 58 PDFs and treating these as errors would blank documents that read fine.

**Distinguish "Vision errored" from "Vision found nothing".** `performRequests_error_` returning false is a failure; returning true with no observations is a real finding about the sheet. Collapsing both into empty text is what makes a failure indistinguishable from a genuinely blank page.

**Don't filter short numeric lines as junk.** Part numbers, pin designators and callout numbers (`500-6307-10`, `SW1`, `Pin#4`) are exactly what catalog corrections search for, and they dominate the lines that carry no alphabetic word. The `MIN_CONFIDENCE = 0.6` floor `web_ocr` already applies is the right and only filter.

### Tooling

`Quartz` needs adding to the `ignore_missing_imports` override in `pyproject.toml`; the list currently has `Vision` and `Foundation` but not `Quartz`, and `mypy .` runs over the whole repo in pre-commit.

## Proven out

The premise — "we will OCR every sheet" — was spiked against the real corpus before committing to it. Measurements below are from 25 of the 58 cached PDFs (2,255 sheets), enough to settle every open question. The premise holds: build it.

### Vision reads the diagram-baked tables

The motivating example works as written. GTF sheet 41's text layer holds the caption "Table 3-18 Coil Positions" and the neighboring _fuse_ table, but `UPPER MAGNET` and `KNOCKER` live inside the diagram image and are absent from it entirely. OCR reads them, scoped `search` returns `page 41 (ocr)` where the text tier returns nothing, and `Read(blob, pages="41")` then renders the sheet well enough to read `UPPER MAGNET → J11-Pin3, Q12` off the actual table. The OCR text for that same sheet is spatially scrambled and could not have supported that quote — which is the division of labor working exactly as intended, not a defect.

Fidelity is high where it can be checked: against GTF's 58 sheets of ground truth, OCR recovers 97.9% of the text layer's vocabulary. The misses are table-of-contents dot leaders and URL tokenization, not content. Schematics are the floor case — the Centaur sheets drop 37% of lines below the confidence floor and yield fragmented reference designators — still a net gain over nothing, but that is the shape of the worst result.

### Vision is fast enough

~563ms/sheet, projecting to **~42 minutes** for all 4,489 sheets, single-threaded. A thread pool buys nothing (8% at best): Vision already parallelizes internally, spending 222s of CPU per 98s of wall clock. Output is byte-identical across worker counts, so it is deterministic and thread-safe.

Volume roughly doubles the corpus: ~9.0M OCR chars against the 6.2M chars of existing PDF text layer. The segment index costs 27MB over 25 documents (~50MB projected) and answers in 21–24ms, so neither storage nor latency is a concern.

### Settled implementation choices

- **Rasterize with Quartz, in-process.** `CGPDFDocument` → `CGImage` handed straight to Vision is 15× faster than shelling out to `pdftoppm` (19ms vs 283ms per sheet) and skips the PNG encode/decode round trip. It costs no new dependency — `pyobjc-framework-Vision` already brings Quartz — and OCR is macOS-only regardless, so a Darwin-only rasterizer gives up nothing.
- **Render at scale 2.0 (144dpi).** Line yield plateaus there; scale 1.0 loses a third of the recognized lines and scale 4.0 gains none.
- **Keep `usesLanguageCorrection` on**, as `web_ocr` already has it. A/B on parts pages dense with alphanumeric part numbers gives identical recovery either way, and correction _on_ drops marginally fewer lines to the confidence floor. No reason to diverge from the image path.
- **The write-time sheet-count assertion is safe.** Quartz and poppler agree on the page count of all 58 PDFs exactly. The lone corpus-wide mismatch is the legacy `text_source = 'ocr'` Jurassic Park row, which carries no page markers at all — the case this plan retires.

### What the spike changed

**There is a third category of dark content: corrupt text, not missing text.** Seven Stern manuals carry a broken font encoding on their _headings_ — subset fonts with no ToUnicode map, so the text layer stores a Caesar-shifted cipher. Monopoly sheet 32 reads `*R\x03 7R\x03 &RL O\x03 0HQX` where the page says "Go To Coil Menu". OCR reads it correctly.

| Manual            | Sheets with mojibake |
| ----------------- | -------------------- |
| Lord of the Rings | 69/211               |
| Elvis             | 63/185               |
| Austin Powers     | 52/169               |
| Pirates           | 52/207               |
| Indiana Jones     | 51/203               |
| Monopoly          | 50/172               |
| Family Guy        | 24/170               |

That is ~360 sheets, and it lands on headings specifically — the thing consumers search for. It also inverts an assumption running through this document: on these sheets the citable tier is garbage and the machine-read tier is accurate. The architecture survives, because step 3 renders the sheet and cites from the image either way. But "OCR is machine-read, therefore lesser" is not universally true, and the tier labeling should not imply it is.

**OCR failure is per-sheet and silent, not per-document.** [Failed OCRs](#failed-ocrs) anticipates Vision permanently choking on a whole document and concludes perpetual retry is adequate. Observed behavior is different: `Exception thrown when attempting to allocate memory` and `Too many symbols in JBIG2 symbol dictionary` both fire mid-document and are non-fatal, so the document completes "successfully" with a hole in it. JBIG2 appears in 13 of the 58 PDFs, including fully dark ones OCR exists to rescue. The current blank rate is 9 of 2,255 sheets (0.4%) — small, but indistinguishable from sheets that are genuinely blank. Separating the outcomes at write time (raster refused / Vision errored / nothing legible) is a few lines and worth having, and it is a different mechanism from the document-level retry question.

**`page N` means the PDF sheet, not the printed folio.** GTF sheet 41 prints "31". The flow is sound because every rung addresses by sheet, but it needed saying — it is now stated under [Page separator](#page-separator).

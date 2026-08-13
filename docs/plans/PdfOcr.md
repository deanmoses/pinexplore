# PDF OCR

We will OCR the PDFs in web cache to make them searchable. Here's why. As of Aug 5 2026, 78% of the 58 cached PDFs were searchable because of their text layers. The problems:

- **Dark docs**. 13 (22%) of docs are fully dark.
- **Dark sheets**. 1,242 of 4,489 sheets (28%) yield no text — including the mixed cases like AFM, where 157 of 163 sheets are images inside a document that technically "has text".
- **Dark content within sheets**. Only 10% of sheets are pure text; 62% carry both text and images. Critical info is often in those images, like model names as section and table headers.
- **Corrupt text**. Text that _is_ extracted but is unsearchable gibberish. Seven Stern manuals store their headings in subset fonts with no ToUnicode map, so the text layer holds a Caesar-shifted cipher — Monopoly sheet 32 extracts as `*R\x03 7R\x03 &RL O\x03 0HQX` where the page reads "Go To Coil Menu". That is ~360 sheets across those seven, landing on headings specifically.

This dark content is absent from `search`. `search` is how consumers figure out whether relevant information exists and where it lives. Meaning a huge chunk of our PDF content is effectively missing.

We want to use OCR to light up the dark content.

The division of labor: OCR provides findability, not citability. AI sessions use `search` to locate the sheet, then quote the text layer where it has the words. Where it doesn't — the ink-only sheets OCR exists to surface — they render that sheet and cite what they read off the image.

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

### Step 2 — search within a document (optional)

Once the Flippatch AI session has a document it cares about, it scopes `search` to that document, and then to a section, to find which sheets carry the term. Those scopes are [SearchScopes.md](../SearchScopes.md) and are being built separately; this plan only adds the OCR tier to what they already return.

### Step 3 — render

Often you'll need to render the sheet, such as:

- reading the printed page number
- an `(ocr)` hit, where the words exist only as ink
- mojibake in the text layer, where `text` is a cipher


#### Rasterize with `pdftoppm`

```bash
pdftoppm -f 10 -l 10 -r 144 -png -singlefile <blob> /tmp/sheet-b21ecaf7-p10
```

A quarter-second on the 105MB Sonic blob for a ~370KB PNG. 144dpi is the same resolution the OCR pass rasterizes at, and `-r 288` is there for a contested glyph. poppler is already a hard requirement of this repo (`pdftotext` extracts the text layer), so unlike the OCR pass this step carries no macOS constraint.

Important flags:

- **`-singlefile`**: without it poppler pads the page suffix to the width of the document's total page count, so identical flags write `gtf-09.png` on a 58-sheet manual and `sonic-010.png` on Sonic — a hand-assembled path is wrong on every 100+ page document, and the sheet count is precisely the thing the cache knows and the session does not. With it, the output path is the prefix plus `.png`.
- **a flat `/tmp` prefix**, so nothing has to `mkdir` before rendering. Out of range is loud rather than silent: `-f 999` against a 58-sheet document exits 99 with `the first page (999) can not be after the last page (58)`, so there is no empty-file failure mode to guard.

**What ships is the handoff line.** `_render_handoff_line` prints the command and the path it will produce, so the session runs the one and reads the other instead of parsing `pdftoppm`'s output:

```terminal
blob: /Users/…/raw/b21ecaf7….pdf  (105.4MB — over Read's 100MB cap)
sheet: pdftoppm -f 10 -l 10 -r 144 -png -singlefile <blob> /tmp/sheet-b21ecaf7-p10
       → Read /tmp/sheet-b21ecaf7-p10.png
```

The blob line stays, and gains its size. `Read` refuses a PDF over 100MB, so a session pointed at Sonic's blob gets a refusal and nothing that explains it; with the size printed, the line says when it is worth following. It is also the whole of what there is to say about an image row, whose blob already is the picture.

144dpi is the default because the ceiling is the vision input, not the rasterizer. At 1224x1584 the image passes to the model without downsampling, for roughly 2,600 tokens; 288dpi is downsampled back to about 1550x2000 and costs ~4,100 tokens for it. 144 already resolves the glyphs a numeric citation turns on — `Rivet, .089 ⌀D, 11/32" L, Roll`, `Bushing, .16" ø ID X .281" ø OD X .187"` — on both sheets tested, an Iron Man parts page and an unseen AFM assembly page. So 288 is for a specific contested glyph, not a default.

The renders are not stored: `/tmp` is the right lifetime, since the OS reaps it, re-rendering is free, and a leftover file is by construction the same bytes at the same dpi. The OCR pass keeps its own in-process Quartz rasterizer — across every sheet in the corpus, shelling out per sheet is 15x slower, and at one sheet a quarter-second is free.

### Step 4 — quoting

**Quote the text layer when it has the words.** A hit not labelled `(ocr)` is already the source's own characters: `section <url> "page 41"` prints that sheet verbatim, and what it prints goes in `quote:`.

**Quote the render when it doesn't**, like an `(ocr)` hit,  mojibake in the text layer.

Extraction reads a sheet in reading order, so a table arrives as a column of unattached cells and words drawn as artwork never reach the text layer at all — a correct quote is routinely not a substring of what was extracted. Checking the OCR tier instead does not rescue it: measured against text layers across this corpus, an exact match rejects about a quarter of correct spans and an ordered-word match about a seventh, so no threshold makes the check honest and a fuzzy one would trade false rejections for false confidence.

What that does **not** relax is what counts as a quote. The test is whether the evidence is _text_, not whether extraction caught it: outlined flyer type and a manual with no text layer are words on a sheet and are quotable once read, while a checkmark in a feature-matrix column is a mark and never becomes text by being looked at — that stays a quote-less cite (`ref` + `locator` + `note`). Quoting a feature's row label to establish an edition column is the forgery both rules exist to stop.

The discriminator is the cache row's `content_type` — a fact about the document, not something the patch records about its own quote — which the gate reads through `web_cache.get`. That is already exposed, so this plan owes the change nothing: it is the same `content_type` `_render_handoff_line` keys on. Two consequences worth naming. A document the cache does not hold is a failure and never a skip, since a PDF nobody could read is not a PDF quote. And the skip is narrow by ref scheme: `http(s)` refs that resolve to a cached PDF go ungated, while IPDB rows, OPDB pages, caption transcripts and HTML pages stay fully gated — including an HTML page served at a `.pdf` path, because the row's type is consulted and the URL's spelling is not.

### `quote` is not needed for PDFs

For PDFs, `search` and `section` replace everything that `quote` was once used for — and the search scopes cover HTML too, so that is not a reason to keep it either. It is kept anyway for now, but the reason has narrowed: its matching is the quote gate's matching, so a `quote` hit verifies by construction where an FTS hit can match tokens scattered across a sheet — and that argument now holds only for the refs the gate still checks, which are every kind except a PDF. On a PDF, `quote` is a convenience for finding a phrase, not a preview of a verdict. See [SearchScopes.md](../SearchScopes.md); the decision waits on how Flippatch sessions actually use the scopes.

## Architecture

We will OCR every sheet. OCR content must be stored separately from the PDF's text layer that's what quotes are verified against, and also duplicated content would garble spans.

- `pages.text`: the citable text layer provided by the source (be it PDF, HTML or other)
- `pages.ocr_text`: the OCR text we generate (be it for a PDF, image or other)

We retire `text_source = 'ocr'` — not the field, but the `ocr` value. See [Retiring `text_source = 'ocr'`](#retiring-text_source--ocr) below, which depends on the argument in the next section.

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

### Retiring `text_source = 'ocr'`

The value labels `text` as machine-read, and after this change no such row exists: machine-read words live in `ocr_text` wherever they came from, and `text_source` goes back to answering one question — how the _citable_ layer was derived.

#### Images

Images are the value's original owner (`_ImageHandler.text_source = 'ocr'`), and OCR is an image's entire text. The finding that makes an OCR'd sheet non-citable makes an OCR'd flyer non-citable too — 88.2% line-exactness is a property of the recognizer, not of what it was pointed at — so images move with the PDFs. An image's OCR is written to `ocr_text`, its `text` is NULL, and its `text_source` is NULL: nothing derived a citable layer, and stamping one would assert otherwise.

The division of labor survives the move intact, and is shorter than the PDF's — `search` finds the flyer on the OCR tier, and the render step is just opening the blob, which is already a picture. `_render_handoff_line` prints that blob path today via a `text_source == 'ocr'` test whose own docstring says the rule is "a property of the content type, not of the extraction method", so it keys on the content type being an image instead and the code stops disagreeing with its comment.

What it costs: `quote()` never answers for an image again, and the reviewed-draft workflow in [WebCache.md](../../WebCache.md)'s Images section — OCR it, check it against the picture, then cite it — becomes read-the-picture-and-cite, which is the instruction PDFs already get. A hand-typed transcription is untouched: it arrives through `web_import.py` as `manual`, lands in `text`, and stays citable because a person is answerable for it. That is now the only way an image acquires a citable text layer, and it is the right one.

Three mechanical consequences. `ExtractedMeta` gains an `ocr_text` field so a handler can return machine-read words without claiming a text layer — the image handler returns `text=None, ocr_text=…`, and images keep OCRing inline at fetch time, since one image is one Vision call with no batch to defer. The registry's `assert handler.text_source` becomes conditional, since a handler that writes only `ocr_text` declares none; `web_import.py` builds `TEXT_SOURCES` from that same attribute and must drop the empty one rather than offer it as an importable label. And the thin check must learn the new shape: `_thin_probe` measures `body_text ?? text`, so an image row whose words now live in `ocr_text` would read as thin on every fetch and fire "OCR found little/no text in image" even when Vision read a full flyer — the probe falls back to `ocr_text` when `text` is None and the handler produced one, so the warning keeps meaning what it says.

#### The Jurassic Park row

One row in the corpus carries the value: the IPDB Jurassic Park manual, imported with 108KB of OCR text produced outside this tool and no page markers across its 103 real sheets. The migration moves it where it belongs:

```sql
UPDATE pages SET ocr_text = text, text = NULL, text_source = NULL WHERE text_source = 'ocr';
```

Moving rather than clearing, so a host that cannot OCR loses nothing — the imported reading stands as that row's OCR tier until a Mac replaces it. It NULLs a text column on the system-of-record, so it takes the `_backup_before_destructive_migration` copy the column drops take, and it is idempotent by construction: once it has run, nothing matches.

Then re-OCR it in the ordinary pass, which is worth doing on its own merits rather than for tidiness. The imported text has no sheet boundaries, so today its matches file under the extractor's misparsed parts-list headings (`6-32 x /4 Phil.M.S.`) — the document [SearchScopes.md](../SearchScopes.md) names as the visible cost of following `outline()`'s rule. A Vision pass gives it `\f` markers, the pagination rule then addresses it by sheet like every other manual, and the misparsed headings stop being an address anyone can reach. It is also the corpus's only PDF whose text layer carries no markers, so re-reading it removes the one exception to the write-time sheet-count assertion and lets that assertion be unconditional.

### Pagination

#### Page separator

We'd separate sheets with `\f`, just like `pages.text`.

`page N` counts sheets from the front of the file, not the folio printed on the page. GTF sheet 41 prints "31". `search` and the render step both address by sheet so they agree with each other, but anyone cross-checking against the printed number will be off by the front matter.

#### Pages in dark documents

A fully dark document has page markers in `ocr_text` and none in `text`, making the two columns disagree about whether the document is paginated. `outline`/`section` need a rule for which column defines `page N`. The rule: `text` is primary. It comes from `text` when it contains markers, and only otherwise `ocr_text`.

Incidentally, this gives those 13 dark documents a page map they don't have today.

Which means `outline` and `section` start answering for documents that had nothing to say, and what they hand back is ink. **They label it `(ocr)`** — in the CLI output, and as a tier on the Python return, the same word and the same meaning `search` uses: machine-read, verify by rendering the sheet. A function whose contract is verbatim citable text must not quietly start returning something else, and the tier is the whole of the difference, so it rides the text rather than living only in the rung that found it. `_doc_of` takes the tier as a parameter to make this possible — it reads `rec["text"]` unconditionally today.

#### Pages in non-dark documents

When both columns are paginated they must agree on sheet count. That's a cheap assertion at write time and the only thing standing between a rasterization/extraction mismatch and page 41 meaning two different sheets in the two columns.

### Search

`search` gains the OCR tier: it indexes `pages.ocr_text` alongside `pages.text`, and a hit says which tier it came from. Ranking, counts and snippets must not cross tiers — and that separation belongs in the computation, not in the output.

Counts are the clearest case. 62% of sheets carry both text and images, so a term printed once in a header lands once in `text` and once in `ocr_text` — one physical occurrence, counted twice — and the count is the entire triage signal at the global and document scopes. The inflation would be worst on the documents whose text layers were already fine. Snippets are the same argument one step on: the architecture rests on quote verification happening against `text` alone, so a snippet whose provenance is ambiguous trains a reader out of the one habit keeping a machine reading out of the catalog. `snippet()` takes a column index, so per-tier is the natural implementation there anyway.

#### Two indexes, not two weighted columns

The obvious build — one `pages_fts` gaining an `ocr_text` column, tiers told apart by column filters and `bm25()` weights — does not separate ranking, and fails quietly. FTS5 normalizes bm25 by the row's **total** token count across every column, so a zeroed weight suppresses a column's term contribution but not its contribution to the length denominator:

```text
rowid 3  -7.482778   4 matches, no ocr_text
rowid 1  -7.108671   3 matches, no ocr_text
rowid 2  -6.023062   3 matches, ocr_text populated  ← text column identical to rowid 1
```

Rows 1 and 2 hold byte-identical text columns and match identically; row 2 ranks lower for no reason but having been OCR'd, under `bm25(t, 1.0, 0.0)`. The penalty is about the size of the gap between three matches and four — so OCR'ing a document would cost it roughly one match's worth of rank on the text tier, across the 78% of PDFs that never needed OCR, with nothing in the output to show for it.

So the OCR tier gets its own external-content FTS table over the same `pages` rowid, with its own sync triggers. Two indexes, two independent bm25 spaces, no shared denominator.

#### One row per document

Both tiers are computed independently and presented together: one row per document, ranked by its better tier, carrying each tier's own count and the snippet from whichever tier is worth showing.

```terminal
95 matches in 35 sections (text) · 12 in 4 (ocr)   …/Sonic_Manual_10_July_2026.pdf
18 matches in 6 sections (ocr)                     …/Galactic-Tank-Force-Game-Manual.pdf
```

Returning a document twice — once per tier — would keep the computation honest and then discard the result at the point of use. `search` is the triage rung and its unit is the document: two rows makes `--limit 20` mean twenty rows rather than twenty candidates, and it scatters one document's two answers across two ranks, where the _asymmetry between them_ is the thing worth reading. `95 (text) · 12 (ocr)` says the OCR found little the text layer did not, and that is a decision; the same two numbers thirty places apart say nothing anyone can act on. A dark document loses nothing: it has no text tier, so it prints one count labeled `(ocr)`, which is the row it would have had either way.

The merge costs an over-fetch — run each tier at roughly twice the limit, merge, take the top N. A document in neither tier's top-N cannot be in the merged top-N, so stopping there loses nothing.

How the scopes themselves work — counts, sections, `highlight()` markers — is [SearchScopes.md](../SearchScopes.md), which already reserves a `tier` field for this. This plan supplies the second tier and its semantics, not the retrieval design. One shape changes there: `SearchHit.tier` is a scalar, which only makes sense if a document appears once per tier, so the document row carries a count per tier instead. `SectionHit.tier` stays scalar and is already right — a section belongs to one tier, and a document's section list interleaves both in sheet order rather than splitting in two.

The tier says where the text came from, not which one is more accurate. On the ~360 mojibake sheets the relationship inverts: the `text` tier is a cipher and the `ocr` tier is the faithful reading. So the label must stay descriptive — `ocr` means "machine-read, verify by rendering the sheet" — and must not editorialize into "lower quality", which would send consumers away from the only tier that can find those headings. Step 3 renders the sheet and cites from the image either way, so citability is unaffected.

That inversion is not rare — ~360 sheets across seven manuals, landing on headings specifically, which is what consumers search for:

| Manual            | Sheets with mojibake |
| ----------------- | -------------------- |
| Lord of the Rings | 69/211               |
| Elvis             | 63/185               |
| Austin Powers     | 52/169               |
| Pirates           | 52/207               |
| Indiana Jones     | 51/203               |
| Monopoly          | 50/172               |
| Family Guy        | 24/170               |

### When OCR happens

Here's the ways that OCR can get kicked off:

1. We show a coverage line on `search` when the corpus has un-OCR'd PDFs, so a thin result set says so rather than implying completeness.
2. We provide a manual command that consumers can run in ⬆️ that case. Default is to run against all outstanding docs, but there's also a way to run against 1 doc, which I think we'll use for testing more than anything else.

A detached OCR process spawned by `fetch` was considered and rejected for v1: two runs racing on the same SQLite, partial writes, and no failure surface are exactly the machinery the retry section below declines to build, and the coverage line plus the manual command already close the loop. It can be added later without rework.

#### Failed OCRs

There are two failure mechanisms here, and they need different answers.

**Document-level: Vision permanently chokes on a document.** It stays in the gap forever and gets retried on every subsequent fetch. Either mark the attempt on the row (an ocr_attempted_at or a failure flag, so the gap query can skip what has already failed and a --retry-failed flag can bring it back) or accept perpetual retry.

For v1 we accept perpetual retry. YAGNI.

**Sheet-level: a single sheet fails inside a document that otherwise succeeds.** This is the one that actually happens. `Exception thrown when attempting to allocate memory` and `Too many symbols in JBIG2 symbol dictionary` both fire mid-document and are non-fatal, so the document is written as a success with a hole in it. JBIG2 appears in 13 of the 58 PDFs, including fully dark ones OCR exists to rescue. Retry does not address this at all — there is nothing in the gap to retry, because the document completed.

Probing the outcome of every sheet individually — raster refused / Vision errored / nothing legible — found **no swallowed failures**. Elvis (185 sheets), Lord of the Rings (211) and the Centaur schematics (15) all came back clean despite logging those errors; the errors are noisy but fully recovered. The only empty sheets in the sample are Time Machine's p10, p34 and p78, which are the three genuinely blank pages `web_pdftext.py` already documents. So the measured 0.4% blank rate is real blankness, not hidden loss.

That makes per-sheet outcome recording insurance against a failure measured at zero, so **v1 does not record it**. The coverage line never needed it — un-OCR'd is `ocr_text IS NULL`, a question about the document — and a tally of sheets attempted against sheets that yielded text would today read "all clean" for all 58 documents.

What v1 does keep is the distinction _in flight_. The three outcomes — raster refused, Vision errored, nothing legible — all leave the same empty run between two form feeds, so a sheet that failed and a sheet that is blank are indistinguishable once written. `web_ocr` already tells the last two apart for a single image, and the sheet loop must preserve that (see [Failure signals](#failure-signals)) and warn on stderr when a sheet errors. That way a mid-run failure is visible while the run is happening; what you give up is asking the row about it afterwards.

### Staleness

When content is re-fetched, we re-OCR. However, OCR is macOS-only. A row can be refetched on a host that can't OCR — new bytes, text re-extracted. The mechanism is in `upsert_page`, whose `ON CONFLICT DO UPDATE` rewrites every column: it gains an `ocr_text` parameter. The image path supplies it inline. When a caller passes none (the PDF path — the fetch never OCRs a PDF), the SQL keeps the stored value if `content_sha` is unchanged and NULLs it when the sha changes — so a refetch on a host that can't OCR never strands stale OCR against new bytes, and an unchanged refetch never discards a Mac's work. The `unavailable`/`keep_manual` preserve path in `web_fetch` carries `ocr_text` through for the same reason. A cleared row rejoins the `ocr_text IS NULL` gap, and the next OCR pass on a Mac re-reads it.

## Implementation notes

Traps and mechanics that cost real time to discover. None are visible from the API surface.

### Page markers

**Splitting on `\f` yields one more element than there are sheets.** poppler terminates _every_ page including the last, so `text.count("\f")` is the sheet count while `text.split("\f")` leaves a trailing empty string to discard. Getting this backwards turns every paginated document into an off-by-one and makes the sheet-count assertion fire on the whole corpus. Emit `ocr_text` the same way — a marker after every sheet, including the final one — so both columns split identically.

**A blank sheet still contributes its marker.** Dropping it shifts every later sheet's ordinal, which is the one thing the markers promise. `web_pdftext._normalize` already does this deliberately; match it.

### Attributing a match to a sheet

**Never treat an unpaginated text layer as one page.** A PDF whose `text` holds no form feeds is not a one-sheet document — it is a document with no page information. Reporting its hits as `page 1` mis-addresses every one of them — the Jurassic Park manual carried zero markers across 103 real sheets until [its re-OCR](#the-jurassic-park-row), and an import can hand over an unpaginated transcription again at any time. Report those hits as whole-document, with no sheet address. Which column supplies the addresses is decided in [Pages in dark documents](#pages-in-dark-documents); this is what the other column must do when it has none.

**The highest sheet carrying text is not the sheet count.** Empty sheets carry no text, so a document whose last sheet is blank appears to stop short — TAG's text tier ends at 59 of 60. Read sheet counts from the marker count, never from the sheets that happened to produce content.

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

Volume roughly doubles the corpus: ~9.0M OCR chars against the 6.2M chars of existing PDF text layer. Neither storage nor latency is a concern.

### Settled implementation choices

- **Rasterize with Quartz, in-process.** `CGPDFDocument` → `CGImage` handed straight to Vision is 15× faster than shelling out to `pdftoppm` (19ms vs 283ms per sheet) and skips the PNG encode/decode round trip. It costs no new dependency — `pyobjc-framework-Vision` already brings Quartz — and OCR is macOS-only regardless, so a Darwin-only rasterizer gives up nothing.
- **Render at scale 2.0 (144dpi).** Line yield plateaus there; scale 1.0 loses a third of the recognized lines and scale 4.0 gains none.
- **Keep `usesLanguageCorrection` on**, as `web_ocr` already has it. A/B on parts pages dense with alphanumeric part numbers gives identical recovery either way, and correction _on_ drops marginally fewer lines to the confidence floor. No reason to diverge from the image path.
- **The write-time sheet-count assertion is safe.** Quartz and poppler agree on the page count of all 58 PDFs exactly. The lone corpus-wide mismatch is the legacy `text_source = 'ocr'` Jurassic Park row, which carries no page markers at all — the case this plan retires.

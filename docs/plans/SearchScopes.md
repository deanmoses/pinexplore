# Search scopes: global, document, section

## The problem

`search` returns **one row per document with one snippet**, and has no notion of where in the document the match sits. A term appearing 99 times and a term appearing once look identical, and FTS5 picks the displayed window by its own rule rather than by usefulness.

Searching `coil` today, the top hit is the 175-sheet Sonic manual — 95 matches spread over 35 sheets — and the snippet is a table-of-contents line:

```console
$ web_cache.py search "coil"
url: …/Sonic_Manual_10_July_2026.pdf
snippet: … 26 PRICING SETTINGS 27 [COIL] SETTINGS 28 [COIL] SETTINGS …
```

Hits 2 and 3 (Scooby-Doo and Looney Tunes, built from the same manual template) come back with byte-identical snippets, so nothing in the output distinguishes them.

The other half of the gap is that the only way to find occurrences _within_ a document is `quote`, which needs the contiguous phrase — so it answers nothing when you don't already know the wording.

A third symptom, smaller but live: a query matching one of the 13 text-less documents crashes the CLI outright, because `snippet()` over their NULL text column returns NULL and `_cmd_search` calls `.split()` on it. `search '"time machine manual"'` is a traceback today. The scopes work below has to give those documents a row of their own anyway, which is where that gets fixed.

## Three scopes

Each scope narrows by one level and returns the units at that level with a match count.

| Scope        | Input        | Returns                                                                                                    |
| ------------ | ------------ | ---------------------------------------------------------------------------------------------------------- |
| **Global**   | a query      | matching documents, BM25-ranked, each with its match count, how many sections it matched in, and a snippet |
| **Document** | + a document | that document's matching sections, in document order, each with its match count                            |
| **Section**  | + a section  | each individual match, in document order, with `surrounding_words=N` of context                            |

```console
$ web_cache.py search "coil"
95 matches in 35 sections  …/Sonic_Manual_10_July_2026.pdf
35 matches in 10 sections  …/Scooby-Doo-Maintenance-Manual.pdf
15 matches in  1 section   …/Gottlieb Far Out pinball schematic

$ web_cache.py search "coil" --url <sonic-url>
page 8    3 matches
page 9    5 matches
page 60   2 matches
…

$ web_cache.py search "coil" --url <sonic-url> --section "page 60" --surrounding-words 200
$ web_cache.py search "coil" --url <sonic-url> --pages 40-50
```

**The global scope keeps its snippet.** The ladder's discipline is about where the expensive per-match work happens, not about withholding what a single query already pays for: the snippet costs what it costs today, and for the common one- or two-match HTML hit it still answers the question in one command instead of three. What it lacked was context, and the count supplies exactly that — `95 matches in 35 sections` in front of a table-of-contents line tells the reader the line is not representative, which is the actual defect in today's output. Dropping it would also strand [PdfOcr.md](pdf_ocr/PdfOcr.md), whose OCR tier leans on a global-scope snippet to say whether a machine-read hit is worth rendering.

The scopes live on `search` rather than as a `--match` flag on `outline` and `section`, even though the document scope is close to "outline with a count column". A scope is a rung of the search ladder — you arrive at it holding a query, not a heading — and it must list sections `outline --min-chars` hides, since a nav fragment carrying the only match still needs to surface.

FTS5 query syntax is already in place — a double-quoted run is one phrase, units AND together (`'"upper magnet" "coil positions" knocker'`). All three scopes route through the same `_fts_query` wrapper, so its semantics define counts at every level.

### A sheet range is the section scope, entered differently

`outline`/`section` is almost useless on a PDF — you mostly get `page 1`…`page N`. To actually find content in a document you have to search. The scenario: a promising table on page 45 tells you you're in the right area — a comparison of models — and now you want the terms you hope surround it, across pages 40-50.

`--pages 40-50` is that read. It is **not a fourth rung**: it enters the section scope the same way `--section` does, and returns the same thing — each match in document order with `surrounding_words` of context, sheet by sheet. The only difference is the address, which is why it is `--pages` rather than `--section "page 40-50"`: every `--section` value is a name the document scope printed, and a range is not. It is the one address a human types instead of copying out of output.

- **`--pages` requires `--url`.** A sheet range across documents is meaningless; a validation error, not something quietly applied to every hit.
- **`--pages` and `--section` are mutually exclusive.** Two ways of naming the same thing, and composing them means nothing.
- **Out-of-range is not an error.** `--pages 40-500` on a 58-sheet document returns what exists. So does a range that catches no matching sheet — that is an answer, and the document scope is where you see which sheets have any.
- **Unpaginated is an error**, and says so the way `_section_miss_hint` already does: `no page markers in this document; --pages names PDF sheets`. Silently ranging over a document with no sheets would report hits under numbers that mean nothing.
- **The sheet-vs-folio distinction bites hardest here**, because this is the rung where the number is typed rather than copied. `page N` counts sheets from the front of the file; GTF sheet 41 prints "31". See [Page separator](pdf_ocr/PdfOcr.md#page-separator).

A range interacts with nothing, because nothing inside a document is ranked: it is a filter over document order, so sheets stay monotonic and there is no question of what narrowing does to term statistics or to the order of what survives. That is the same thing [no ranking of sections](#deliberately-not-doing) buys everywhere else, showing up here as an absence of rules.

## Addressing a section

`--section` takes a name the document scope printed, matched case-insensitively, the same way `section()` matches a heading. A heading name can open more than one block, and `section()` returns all of them rather than picking; `--section` inherits that unchanged: every block bearing the name, in document order, with its matches. The document scope correspondingly holds one row per **name**, summing a repeated name's blocks, so what it prints and what `--section` returns describe the same thing.

Two addresses have no heading behind them, and both need to be reachable:

- **`(no heading)`** — matches above the first heading, and every match in a document that has no headings at all. Rare in HTML (one match in 92 on a `stern` query) but universal on an OCR'd image or a caption track, where it is the document's only section. It prints under that literal name and `--section "(no heading)"` takes it back. It is deliberately not `body`: on an assembled page `section(url, "body")` returns the whole body including every headed section, which would answer a three-match question with 60K of text.
- **A single-section document** skips the hop entirely. When a document has exactly one matching section, `--url` alone prints the matches rather than a one-row list restating the count the global scope already gave.

## Sizing, clipping, and limits

`surrounding_words` is measured in **words, not lines** — a PDF sheet's lines are short and irregular, so a line count means something different on every document. That is a deliberate divergence from `quote --context`, which widens by ±N lines and stays as it is: `quote` is anchored on a phrase the caller already has and clips to a section, where lines are the unit that keeps a table row intact. Name the new flag `--surrounding-words` rather than `--context` so the two are not read as the same knob. It defaults to **30**.

Words size the window; **whole lines are what comes out**. A window that begins or ends mid-line breaks a table row apart and makes a worse `cite.quote` than the same span with its structure intact — and structure is the surviving evidence that two cells were two cells. So the window grows outward line by line until it has gathered at least N words either side.

Two rules come straight from `quote`, for the same reason:

- **Clip the padding to the section, never the match.** A window's padding stops at the section, so a window inside one can carry its locator. A match that itself straddles a boundary is returned whole and unlabelled instead — truncating it to the boundary would hand back a span missing the words it was found for, which is the one thing this read must not do. That is `quote`'s split of a name that must be true from an extent that must be complete.
- **Merge overlapping windows.** Two matches a few words apart are one window, not the same text printed twice.

And two limits, because the section scope is the rung where a document can hand back everything at once — Elvis holds 277 `coil` matches, and single sheets hold dozens:

- **`--limit` caps windows shown** (default 20, `--limit 0` for all), with a line saying how many were withheld. Silent truncation would read as "that's all of them".
- The count in the section list is the **match** count, not the window count; a window absorbing three matches says so.

**Window text is verbatim, unmarked.** No `[brackets]` around the matched terms, unlike the global snippet: these windows are stored lines exactly, which makes any span lifted from one citable, and `[` already appears in 171 cached documents so a bracket would be ambiguous anyway. The global snippet keeps its brackets — it carries `…` elisions and was never citable.

## What a match count means

FTS5 exposes no count function (`matchinfo()` was FTS4), so counts come from `highlight()`, which marks every matched phrase and every matched loose token. For `"camel toes" bananas`, two phrase hits plus five `bananas` reads as seven. That needs stating in the CLI help, because at the global and document scopes the count **is** the entire triage signal.

One documented undercount: FTS5 merges **overlapping** phrase matches into a single marked region, so `'"a b" "b c"'` against `a b c` counts one, not two. Adjacent-but-separate matches are unaffected (`coil coil coil` counts three). It belongs in the same help text, next to the phrases-plus-tokens sentence.

## Mechanism

`highlight()` returns the stored column with markers _inserted_ — not elided or reflowed like `snippet()`. So a window sliced around a marker, markers stripped, is byte-identical to the stored text, and splitting the highlighted string on `\f` attributes every match to its sheet.

This needs **no schema change and no derived index**. Counting is a full-column read per hit, which measures fine:

```text
'coil':    20 docs, 2093 matches,  67ms
'magnet':  20 docs,  148 matches,  28ms
'pinball': 20 docs, 2686 matches,  14ms
```

The cost lands in the right place: the global scope reads whole columns for at most `limit` documents, and the expensive per-match work happens only after you have scoped to something.

### The markers have to be characters the corpus cannot contain

Byte-identity holds only for a marker absent from the text being marked, and both counting and slicing are built on it: a literal marker character inflates the count, and stripping it corrupts the returned evidence. The obvious candidates are contaminated, measured across the 478 cached documents:

| Marker pair                               | Documents containing it |
| ----------------------------------------- | ----------------------: |
| `[` / `]` — what `search`'s snippet uses  |               171 / 170 |
| `\x02` / `\x03`                           |                   4 / 9 |
| `U+E000` / `U+E001` (Unicode private use) |                   0 / 0 |

Control characters are not the safe choice they look like. The Elvis manual's text layer is cid-mangled and uses `\x03` as its space character, so marking with `\x03` and stripping it afterwards deletes every space on the sheet — 7 of the top 20 `coil` hits fail to reproduce their stored text that way. The private-use pair roundtrips exactly on every hit tested.

So: mark with `U+E000` / `U+E001`, and **assert per document that the stripped highlight equals the stored text**. Both strings are already in hand at that point, so the check costs nothing.

A failed assertion **degrades that one document**, and does not raise: its row reports `count unavailable` and the search returns its other nineteen hits. A marker collision in one document is not a reason to withhold the answer about the rest, and the label is as loud as an exception where it matters — on the document that actually has the problem.

Write the markers as escapes (`"\ue000"`) in code and as `U+E000` in prose — a literal private-use character is invisible in an editor and does not reliably survive a copy-paste. **Assert at import that each marker constant is one character long.** This is not paranoia: a prototype of this design pasted the literals, they arrived as empty strings, and `line.count("")` reported 117,184 matches on a 95-match document without erroring anywhere.

### A match maps to a section by line index

Markers are inserted, and contain no newline — so a line's index in the highlighted string is its index in the stored text. Splitting the highlighted text on `\n` therefore hands every match a line index that the sectioning rule below already knows how to name, with no offset arithmetic and nothing to keep in sync. That also means a match inside an assembled page's frontmatter names `metadata`, which is an address `section()` already accepts.

## What a section is

**The rule is `outline()`'s rule, unchanged**, so all three scopes share one address vocabulary and every _heading or sheet_ `search` prints is a name `section()` accepts. The two synthetic labels are the exceptions: `(no heading)` is a `--section` address only, and `section boundary` is not an address at all. In order:

1. **Paginated** — a document whose text carries `\f` markers is divided into sheets, addressed `page N`. Every PDF extracted since the page markers landed.
2. **Otherwise, headed** — the enclosing heading block, via the existing `_enclosing_section`, with `metadata` for the frontmatter and `(no heading)` above the first heading.
3. **Otherwise** — one whole-document section, named `(no heading)`. A caption track, an OCR'd image, an HTML page whose markup has no headings at all. A legitimate answer, not a placeholder.

Following `outline()` rather than branching on content type is a deliberate choice with a visible cost. The IPDB Jurassic Park manual is a PDF whose text predates the page markers, so it falls to rule 2 and its 70 `coil` matches file under the extractor's misparsed ATX headings — `6-32 x /4 Phil.M.S.`, `Indicate Manufacturer`. Those are poor names. They are also exactly what `outline()` prints for that document today and exactly what `section()` will return them for, and a rung of the ladder that invented better names would be handing out addresses the next rung cannot resolve. The fix for that document is page markers, not a second sectioning rule.

## A document can match with no text matches

The index is `fts5(url, title, text, …)`, so a document can match on its address or its title while contributing no text match at all. There are **two** such cases and they are not the same fact:

- **No text layer.** `highlight()` over the text column returns NULL. These are the 13 documents with no extracted text — the dark PDFs — findable today by any token in their URL. Label them `url/title match, no text layer`. Not "title match": their `title` is NULL too, so what matched was the address. This is both the true answer and the useful one — it says the cache holds the bytes and cannot yet read them, and it is the exact slot the OCR tier fills later.
- **Text, but the match isn't in it.** `highlight()` returns the column and it holds no marker. This is not an edge case: `uploads` puts 22 such documents in 29 hits, `wp` 15 in 33, `htm` 7 in 15 — every URL-shaped token does it. Label them `url/title match, 0 text matches`.

Rendering either as `0 matches in 0 sections` would read as a bug in the counter. Hiding them would be a regression against today's `search`, which finds them.

**Ranking is left alone.** A zero-text-match document can outrank one with fifty real matches, because BM25 scores the url and title columns too. That looks wrong and isn't worth a second sort key: the count column now sits right next to the row saying precisely what it is, which is the signal the ordering was being asked to carry.

## The Python API

The CLI is the shell-friendly face; the functions are what flippatch and multi-step sessions import, and WebCache.md documents them as such. Only `quote()` has an external consumer today, so the search-side shapes are free.

- **`SearchHit` grows** `matches: int | None` (None = count unavailable), `sections: int | None`, `has_text: bool`, and `tier: str`. `snippet` stays, and becomes `str | None` — the dark rows have none, which is the crash in "The problem" above.
- **`search_sections(url, term)` → `list[SectionHit]`** — `{section: str | None, matches: int, tier: str}`, document order.
- **`search_matches(url, term, *, section=None, pages=None, surrounding_words=30)` → `list[MatchHit]`** — `{text: str, section: str | None, straddles: bool, matches: int, tier: str}`, document order, **every** window. `section` is None when no single one is true of the window, and `straddles` says which of the two reasons applies — an unheaded region, or a match running past its section's end — because a paginated document has no unheaded region and calling a cross-sheet match `(no heading)` would be a false locator. `section` and `pages` are the two addresses and at most one may be given; `pages` is an inclusive `(first, last)` sheet range. The limit is a display concern: computing all the windows for a 277-match document costs ~55KB, and only printing them is expensive, so the library returns them all and the CLI truncates. That also lets the CLI say how many it withheld without a second count.

**Carry `tier` on all three from day one**, valued `text` for everything until OCR exists, so adding the OCR tier later is additive rather than a change to every scope's output. It rides the section as well as the document because a single document will be able to hit on both tiers.

## Deliberately not doing

**No ranking of sections within a document.** Document order is correct here: page numbers stay monotonic, and every matching section is returned rather than a chosen subset. Ranking would need BM25 at section grain, which means a derived segment table — and on HTML it would misbehave badly, since leaf blocks run to a median of 174 chars with 35% under 100, so a nav fragment would outrank a real paragraph. Without ranking, that pathology never arises and HTML gets the same treatment as PDF.

**No segment table.** It exists only to make section ranking possible; nothing else needs it.

**Not deleting `quote`.** The three scopes appear to subsume it — a stripped `highlight()` window is the same verbatim text `quote` returns — but its matching is the gate's matching (contiguous literal substring after smart-quote straightening and whitespace collapsing), so a `quote` hit verifies by construction where an FTS hit can match tokens scattered across a sheet. Keep it until flippatch sessions show how they actually use the new scopes.

## Relationship to PDF OCR

Ship this **before** [PdfOcr.md](pdf_ocr/PdfOcr.md), even though that work motivated it.

OCR's value proposition is _which sheet do I render_ — and that answer comes from these scopes, not from OCR. Shipping search first makes OCR a purely additive data change (one column, one more FTS index, one tier label), and means a disappointing result can be attributed to the OCR or to the retrieval rather than to both at once. It also opens the observation window that decides `quote`'s fate.

The 13 fully dark PDFs have no text for any scope to search until OCR lands. They are not invisible — they match on url and title now and will keep doing so, carrying the `url/title match, no text layer` label above. Worth saying plainly so this release is not read as covering them.

## Sequencing note

`pages` (the table, meaning one URL) and `page N` (a PDF sheet) collide, and this is the code that uses both senses most heavily. The [documents-not-pages rename](DocumentsNotPages.md) would resolve it, but **this does not wait on it** — the rename is a 26-file mechanical sweep plus a destructive migration, and blocking a feature on it buys nothing the feature needs.

So the collision gets handled locally instead: in the new code the URL sense is never called `page`. It is `rec`/`doc` in variables and "document" in output and help text, exactly as `_doc_of` and `_Doc` already do, leaving `page` to mean a sheet everywhere the reader can see. The only unavoidable uses of the old sense are the `pages` / `pages_fts` identifiers in SQL. If the rename does happen later, that convention is what keeps its diff here to those lines.

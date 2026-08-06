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

## Three scopes

Each scope narrows by one level and returns the units at that level with a match count. Snippets are not shown until you have committed to a section, which is where the tokens are worth spending.

| Scope        | Input        | Returns                                                                                        |
| ------------ | ------------ | ---------------------------------------------------------------------------------------------- |
| **Global**   | a query      | matching documents, BM25-ranked, each with its match count and how many sections it matched in |
| **Document** | + a document | that document's matching sections, in document order, each with its match count                |
| **Section**  | + a section  | each individual match, in document order, with `surrounding_words=N` of context                |

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
```

`surrounding_words` is measured in **words, not lines** — a PDF sheet's lines are short and irregular, so a line count means something different on every document. That is a deliberate divergence from `quote --context`, which widens by ±N lines and stays as it is: `quote` is anchored on a phrase the caller already has and clips to a section, where lines are the unit that keeps a table row intact. Name the new flag `--surrounding-words` rather than `--context` so the two are not read as the same knob.

The scopes live on `search` rather than as a `--match` flag on `outline` and `section`, even though the document scope is close to "outline with a count column". A scope is a rung of the search ladder — you arrive at it holding a query, not a heading — and it must list sections `outline --min-chars` hides, since a nav fragment carrying the only match still needs to surface.

A heading name can open more than one block, and `section()` returns all of them rather than picking. `--section` inherits that unchanged: every block bearing the name, in document order, with its matches.

FTS5 query syntax is already in place — a double-quoted run is one phrase, units AND together (`'"upper magnet" "coil positions" knocker'`). All three scopes route through the same `_fts_query` wrapper, so its semantics define counts at every level.

## What a match count means

FTS5 exposes no count function (`matchinfo()` was FTS4), so counts come from `highlight()`, which marks every matched phrase and every matched loose token. For `"camel toes" bananas`, two phrase hits plus five `bananas` reads as seven. That needs stating in the CLI help, because at the global and document scopes the count **is** the entire triage signal.

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

So: mark with `U+E000` / `U+E001`, and **assert per document that the stripped highlight equals the stored text**. Both strings are already in hand at that point, so the check costs nothing and turns a silent miscount into a loud failure. Write the markers as escapes (`"\ue000"`) in code and as `U+E000` in prose — a literal private-use character is invisible in an editor and does not reliably survive a copy-paste.

### A match maps to an HTML section by line index

Markers are inserted, and contain no newline — so a line's index in the highlighted string is its index in the stored text. Splitting the highlighted text on `\n` therefore hands every match a line index that `_enclosing_section` already knows how to name, with no offset arithmetic and nothing to keep in sync. That also means a match inside an assembled page's frontmatter names `metadata`, which is an address `section()` already accepts.

## What a section is

The same unit `section()` already addresses, so all three scopes share one address vocabulary:

- **PDF** — one sheet, addressed `page N`, from the `\f` markers already in the text.
- **HTML** — the enclosing heading block, via the existing `_enclosing_section`.
- **Neither** — a PDF whose text layer carries no markers, or a page with no headings, is one whole-document section. A legitimate answer, not a placeholder.

## A document can match with zero text matches

The index is `fts5(url, title, text, …)`, so a document can match on its address or its title while `highlight()` over the text column returns **NULL**. This is not hypothetical: it is exactly the 13 documents with no extracted text — the dark PDFs — which are findable today by any token in their URL. `Data_East_Time_Machine_Manual` is one.

Rendering those as `0 matches in 0 sections` would read as a bug in the counter. Label them instead — `title match, no text layer` — which is both the true answer and the useful one: it says the cache holds the bytes and cannot yet read them, and it is the exact slot the OCR tier fills later. Hiding them would be a regression against today's `search`, which finds them.

## Deliberately not doing

**No ranking of sections within a document.** Document order is correct here: page numbers stay monotonic, and every matching section is returned rather than a chosen subset. Ranking would need BM25 at section grain, which means a derived segment table — and on HTML it would misbehave badly, since leaf blocks run to a median of 174 chars with 35% under 100, so a nav fragment would outrank a real paragraph. Without ranking, that pathology never arises and HTML gets the same treatment as PDF.

**No segment table.** It exists only to make section ranking possible; nothing else needs it.

**Not deleting `quote`.** The three scopes appear to subsume it — a stripped `highlight()` window is the same verbatim text `quote` returns — but its matching is the gate's matching (contiguous literal substring after smart-quote straightening and whitespace collapsing), so a `quote` hit verifies by construction where an FTS hit can match tokens scattered across a sheet. Keep it until flippatch sessions show how they actually use the new scopes.

## Relationship to PDF OCR

Ship this **before** [PdfOcr.md](pdf_ocr/PdfOcr.md), even though that work motivated it.

OCR's value proposition is _which sheet do I render_ — and that answer comes from these scopes, not from OCR. Shipping search first makes OCR a purely additive data change (one column, one more FTS index, one tier label), and means a disappointing result can be attributed to the OCR or to the retrieval rather than to both at once. It also opens the observation window that decides `quote`'s fate.

One thing to anticipate: **carry a `tier` field on results from day one**, valued `text` for everything until OCR exists, so adding the OCR tier later is additive rather than a change to every scope's output.

The 13 fully dark PDFs have no text for any scope to search until OCR lands. They are not invisible — they match on url and title now and will keep doing so, carrying the `title match, no text layer` label above. Worth saying plainly so this release is not read as covering them.

## Sequencing note

`pages` (the table, meaning one URL) and `page N` (a PDF sheet) collide, and this is the code that uses both senses most heavily. The [documents-not-pages rename](DocumentsNotPages.md) would resolve it, but **this does not wait on it** — the rename is a 26-file mechanical sweep plus a destructive migration, and blocking a feature on it buys nothing the feature needs.

So the collision gets handled locally instead: in the new code the URL sense is never called `page`. It is `rec`/`doc` in variables and "document" in output and help text, exactly as `_doc_of` and `_Doc` already do, leaving `page` to mean a sheet everywhere the reader can see. The only unavoidable uses of the old sense are the `pages` / `pages_fts` identifiers in SQL. If the rename does happen later, that convention is what keeps its diff here to those lines.

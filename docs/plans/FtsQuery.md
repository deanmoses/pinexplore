# Search query syntax: stop mangling phrases

**Implemented** in `d98288e`. Kept for the reasoning behind `_fts_units`, the injection guard, and what was deliberately left out.

## The problem

`search` currently accepts plain words and ANDs them. FTS5 itself supports far more — a quoted unit is a **phrase**, and units separated by whitespace are ANDed together, so this is valid FTS5 and means exactly what it looks like:

```text
"upper magnet" "coil positions" knocker
```

Phrase, phrase, and a loose word. The difference from ANDing tokens is not marginal:

```console
$ # against the real cache
"upper magnet"     (phrase)  ->  1 doc
"upper" "magnet"   (AND)     ->  28 docs
```

`_fts_query` ([`scripts/web_scrape/web_cache.py:523`](../../../scripts/web_scrape/web_cache.py)) destroys that syntax. It splits on whitespace and re-quotes every token, so the query above becomes:

```text
"""upper" "magnet""" """coil" "positions""" "knocker"
```

That does not raise. It parses, silently degrades to an AND of every token, and returns 11 documents — a wrong answer with no signal that the phrases were discarded. Silent wrongness is the failure mode this codebase works hardest to avoid.

Reproduce before doing anything else:

```bash
uv run python -c "
import sys; sys.path.insert(0,'scripts/web_scrape')
from web_cache import _fts_query
print(_fts_query('\"upper magnet\" \"coil positions\" knocker'))"
```

## Why it is written that way

The current behavior is not an oversight — read the docstring. Every token is wrapped in quotes so that FTS5 operator characters in user input cannot break the query. A bare `AND`, `*`, `NEAR`, or a stray unbalanced `"` would otherwise produce an `sqlite3.OperationalError` that a caller has to interpret. **Any fix must keep that guard.** Do not solve this by passing the term through raw.

## Proposed fix

Make the split **quote-aware**: break the term on whitespace _outside_ double quotes, then emit each resulting unit as one quoted FTS5 phrase.

- `"upper magnet" knocker` → two units → `"upper magnet" "knocker"`
- `upper magnet` → two units → `"upper" "magnet"` (today's behavior, unchanged)

This gives per-term control rather than a whole-query mode flag, which matters because the real case is a session that knows one exact caption and is guessing at the rest. It also preserves the injection guard: every unit still goes out as a quoted phrase, so operator characters inside one stay inert.

**Consume the quote characters rather than escaping them — but replace each with a space, not with nothing.** Once the split is quote-aware, an interior `""` has no defined meaning: it could be an escaped quote or an artifact of where the split landed, and neither reading is better. Consuming it makes the guard total rather than merely careful, since no quote character reaches the parser at all.

The substitution is what keeps that from changing a term's meaning, and it is easy to get wrong. `unicode61` reads `"` as a separator, so today's `a"b` → `"a""b"` is the FTS5 string `a"b`, which tokenizes to the **two** tokens `a b`. Delete the quote and it becomes the one token `ab` — a genuinely different query, matching a different document:

```console
"a""b"  ->  alpha a b beta
"ab"    ->  gamma ab delta      ← what deleting the quote would ask for
"a b"   ->  alpha a b beta      ← what replacing it with a space asks for
```

So substitute a space and whitespace-normalize each unit afterwards, which leaves no trace of the substitution in the emitted query.

**NUL gets the same substitution**, and is the only other character that needs it: FTS5 scans its query as a C string, so a NUL truncates it mid-token and raises `unterminated string`. `unicode61` reads NUL as a separator too, so a space stands in for it losslessly. Sweeping `0x00–0x1f`, `0x7f` and the Unicode space/BOM oddballs turns up nothing else — a lone surrogate raises, but that is `sqlite3` refusing to encode a bind parameter (`con.execute("SELECT ?", ("\ud800",))` fails the same way), not the FTS parser, and not this function's to catch.

A unit that strips to nothing (`""`, `"---"`) is dropped. That is safe either way — `""` matches 0 documents alone and is a no-op when ANDed (`"" "magnet"` = 35 = `"magnet"`) — but dropping it says so explicitly. If nothing survives, the query is empty, which FTS5 rejects with `syntax error near ""`; return no hits instead.

**An unbalanced quote runs to the end of the string.** The guard exists so that no input produces an error a caller has to interpret, and rejecting reintroduces exactly that path. `search 'the "upper magnet'` almost always means the phrase — the usual cause is shell quoting, not intent. Because silently reinterpreting input is the failure this whole change is about, the library stays total and the **CLI** prints one stderr line showing the expression that actually ran: `unbalanced quote; searched "the" "upper magnet"`.

Show the whole expression rather than naming one phrase as the open quote's. The quote need not own a whole unit — `foo"bar` is a single unit that is half bare word — and need not own any: in `foo "` the trailing phrase is empty and drops out, so the last unit is a bare word that was never quoted at all. Reporting it as a phrase would make the honesty line itself misreport.

## Scope

- `_fts_query` and `search()`'s docstring, which currently states "AND across whitespace tokens" ([`web_cache.py:538`](../../../scripts/web_scrape/web_cache.py)).
- `--help` for the `search` subcommand needs a line showing the syntax **and** noting the shell must be told to keep the quotes: `search '"upper magnet" knocker'`.
- Tests in `tests/test_web_cache.py` — phrase preserved, bare words unchanged, operator characters still inert, unbalanced quotes behave as decided.
- `docs/WebCache.md` — the search section and the escalation-ladder block.
- flippatch's `docs/AGENTS.src.md:149` mentions `web_cache.search()`; check whether it needs the syntax note. Do not edit generated `CLAUDE.md` / `AGENTS.md` in either repo.

## Out of scope

`OR`, `NEAR(...)`, prefix `*`, and column filters are real FTS5 features, and `NEAR` is arguably the sweet spot for manuals. Ship phrases alone anyway.

The reason to defer `NEAR` is not that sessions can't handle a malformed-query error — they can. It is that `NEAR`'s value is proximity _within a document_, and [PdfOcr](../pdf_ocr/PdfOcr.md) re-grains search to one segment per PDF sheet. At sheet grain a hit is already proximate, so `NEAR(coil magnet, 10)` and a sheet-scoped AND largely converge. Building it now means tuning a distance parameter against a grain about to change. Revisit once segment search lands and the value can be measured.

`OR` and column filters can't coexist with the guard at all: any passthrough mode means arbitrary parser errors reach the caller, which is a cost worth paying only for a feature someone has asked for. Prefix `*` is the one to keep the door open on — it is unit-local, so a trailing `*` outside quotes could be honored without touching the guard — but not in this change.

## Notes

**Match counts belong to [SearchScopes](../SearchScopes.md), not here.** Nothing in `web_cache.py` calls FTS5's `highlight()`, and `search` reports no per-document occurrence count, so there is no counter for this change to alter. Phrase support is a precondition for counting accurately — under AND, a multi-word query's count sums every token independently — but the count itself is defined by the three scopes, which route through `_fts_query` and inherit its unit semantics.

**No tokenizer change.** `pages_fts` is `fts5(url, title, text, content='pages', ...)` with no `tokenize=` clause, so it is stock `unicode61`: case-insensitive, no stemming. This change leaves that alone.

# Documents, not pages

## The problem

`page` means two unrelated things in the web cache, and they sit next to each other constantly.

- **A row in `pages`** — one cached URL. One HTML page, one PDF, one image, one video transcript.
- **A sheet inside a PDF** — `page N`, `page_starts`, `pdf document pages: 41`, `Read(blob, pages="41")`.

So `pages.text` on a PDF holds many `page N`s, and a phrase like "the page's page count" is not a joke, it is what the code makes you write. It is survivable while PDFs are a corner of the system, and it stops being survivable now that sheets are a first-class unit: [SearchScopes.md](SearchScopes.md) makes sections (sheets, on a PDF) a scope of their own, and [PdfOcr.md](pdf_ocr/PdfOcr.md) adds a second per-sheet text column.

Rename the URL sense to **document**. The sheet sense keeps `page`, which is what every PDF reader, `pdftotext`, and Claude Code's `Read(pages=)` already call it.

## What changes

| Now                                  | After                                            |
| ------------------------------------ | ------------------------------------------------ |
| `pages` (SQLite table)               | `documents`                                      |
| `pages_fts`                          | `documents_fts`                                  |
| `pages_ai` / `pages_ad` / `pages_au` | `documents_ai` / `documents_ad` / `documents_au` |
| `PageRow`                            | `DocumentRow`                                    |
| `upsert_page()`                      | `upsert_document()`                              |
| `_require_page()`                    | `_require_document()`                            |
| `web_pages` (DuckDB)                 | `web_documents`                                  |

## What does not change

Everything naming a sheet: `page_starts`, `_page_block`, `_PAGE_NAME`, the `page N` address `section()` takes, `pdf_document_page_numbers`, and the `pdf document pages:` output line. After the rename these are the _only_ uses of the word, which is the point.

`fetches` keeps its name — a fetch is a fetch.

## The migration is not a rename

`ALTER TABLE pages RENAME TO documents` appears to work and quietly breaks the FTS index. Tested against the real schema:

```text
ALTER TABLE pages RENAME TO documents        -> OK
  triggers      rewritten to fire ON "documents", but still named pages_ai/ad/au
  pages_fts     still declares content='pages'  <- stale, and nothing warns
  highlight()   FAILED: SQL logic error
  snippet()     FAILED: SQL logic error
  rebuild       FAILED: no such table: main.pages
```

SQLite rewrites trigger bodies on rename but does **not** update an external-content FTS table's `content=` pointer. A bare `MATCH` still answers from the index's own shadow tables, so a smoke test passes — while `highlight()` and `snippet()`, which read column values back from the content table, fail with an opaque `SQL logic error`. `highlight()` is what all three search scopes are built on, so this is the failure that matters.

The migration therefore has to be:

1. Back up first — this drops objects, so it is destructive by this codebase's own definition. Use the existing `_backup_before_destructive_migration()`, same as the `html_file` and `text_sha` drops.
2. Drop the three triggers.
3. Drop `pages_fts` (its shadow tables go with it).
4. `ALTER TABLE pages RENAME TO documents`.
5. Create `documents_fts` with `content='documents'`, and the three triggers under their new names.
6. `INSERT INTO documents_fts(documents_fts) VALUES('rebuild')`.

It belongs in `init_schema()` beside the existing column migrations, guarded on the old table's presence so it is a no-op on a fresh or already-migrated database. The rebuild is over 478 rows and 10.5M chars, so it costs seconds, once.

## Blast radius

Measured across `*.py`, `*.sql` and `*.md`:

```text
pages (URL sense)  173      web_pages  14      PageRow         15
upsert_page         16      pages_fts  13      _require_page    5
```

26 files. The bulk is `scripts/web_scrape/web_cache.py`; the rest is a line or two each across the other `web_scrape` modules, `scripts/rebuild_explore.py`, `sql/03_raw_web.sql`, and the tests.

Docs carry it too: `docs/WebCache.md`, `docs/AGENTS.src.md` (which regenerates `CLAUDE.md` and `AGENTS.md` — never edit those directly), and the three plan docs. **flippatch has zero references to `web_pages`** and reaches the cache only through `get()`, so nothing there breaks; its prose mentions of `pages.text` want updating for consistency, not correctness.

The one judgement call in the sweep: `\bpages\b` has 173 hits and not all are the table. Prose like "JS-only pages" and "cached pages" means web pages in the ordinary sense and reads fine either way — rename identifiers mechanically, read prose individually.

## Sequencing

**Land this before [SearchScopes.md](SearchScopes.md).** That work is the heaviest user of both senses of the word, and it introduces "section" as the name for a sheet — writing it in the old vocabulary means writing it twice. Doing the rename first also keeps the search diff free of rename noise, so it can be reviewed for what it actually does.

It makes a good standalone commit: mechanical, no behavior change, and one real migration with a test that proves `highlight()` still works afterwards.

## Verification

- `make test` green, plus a new case that migrates a pre-rename database and asserts `highlight()`, `snippet()` and `rebuild` all work — the three things a bare `ALTER TABLE` breaks silently.
- Open the real `cache.sqlite`, confirm the migration runs once, then re-open and confirm it is a no-op.
- `make explore` produces `web_documents`, and `SELECT count(*)` matches the SQLite row count.
- `grep -rn '\bpages\b' scripts/ sql/` returns only the sheet sense and ordinary prose.
- `make push` after migrating, so the shared cache carries the new schema. A stale checkout pushing an unmigrated cache afterwards would undo it.

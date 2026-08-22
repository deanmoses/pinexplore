# Development Guide

START_IGNORE

This is the source file for generating [`CLAUDE.md`](../CLAUDE.md) and [`AGENTS.md`](../AGENTS.md).
Do not edit those files directly - edit this file instead.

Regenerate with: make agent-docs

Markers:

- START_CLAUDE / END_CLAUDE - content appears only in [`CLAUDE.md`](../CLAUDE.md)
- START_AGENTS / END_AGENTS - content appears only in [`AGENTS.md`](../AGENTS.md)
- START_IGNORE / END_IGNORE - content stripped from both (like this block)

END_IGNORE

This file provides guidance to AI programming agents when working with code in this repository.

## Project Overview

Pinexplore is an exploration and validation tool used in support of sister project Flipcommons pinball catalog.

It has two jobs:

- **Web cache**. It builds and maintains a **web evidence cache** — a durable, searchable corpus of fetched web assets related to pinball (manufacturer sites, PDFs, video transcripts, foreign-language press), captured once and reused as attributed evidence for catalog corrections. See [WebCache.md](docs/WebCache.md).
- **Analytics**. It builds and maintains a read-only **DuckDB database** from dumps of external sources of pinball information (IPDB, OPDB, Fandom) as well as web cache. See [Explore.md](docs/Explore.md).

These support a multi-repo pinball catalog system:

- **[Flipcommons](https://github.com/deanmoses/flipcommons)**: the live website and production database. The local dev database is a near-copy of prod, and is considered the source of truth as far as this system is concerned.
- **[Flippatch](https://github.com/deanmoses/flippatch)**: it's job is to research and build [data patches](https://github.com/deanmoses/flipcommons/blob/main/docs/DataPatches.md) to update the Flipcommons database. This is how new AI-researched and AI-authored information gets into Flipcommons.

Pinexplore reads the external systems and nothing else — it holds no catalog data, never modifies the catalog, doesn't join to the catalog, doesn't reference Flippatch or Flipcommons. Its job is to get each source into a shape worth comparing; the comparison itself runs in Flippatch, beside the live records. Flippatch is the system that joins between Flipcommons and Pinexplore data. Pinexplore's only consumer is Flippatch.

## Requirements

- Python 3
- [uv](https://docs.astral.sh/uv/) (manages the venv and `duckdb` Python package)
- Node.js (for `npx prettier` and `npx markdownlint-cli2` in pre-commit hooks)
- [poppler](https://poppler.freedesktop.org/) (`brew install poppler`) — `pdftotext` reads the web cache's PDF evidence. Without it PDFs still cache, but extract no text until poppler is installed and `web_backfill.py` is re-run.

## Querying DuckDB

Use the Python `duckdb` package, not the DuckDB CLI binary — the package is the only required dependency. Don't use MotherDuck; this database is a local file, not on MotherDuck.

```python
import duckdb
con = duckdb.connect("explore.duckdb", read_only=True)
con.execute("FROM ipdb.models LIMIT 5").show()
```

Both `explore.duckdb` and `ingest_sources/` are gitignored build artifacts that won't exist in a fresh checkout. If they're missing, ask the user to run `make all` — pulling from R2 is human-only, see [Rules](#rules).

## Development Commands

```bash
make explore      # Rebuild explore.duckdb from SQL layers
make check        # lint + typecheck + sql-check + test
make test         # Run the test suite (pytest)
make agent-docs   # Regenerate CLAUDE.md and AGENTS.md
make clean        # Remove DuckDB build artifacts
```

`make pull`, `make push` and `make all` reach Cloudflare R2 and are human-only — see [Explore.md](docs/Explore.md).

## Tests

`make test` runs `pytest` over `tests/`, currently covering the web evidence cache (`scripts/web_scrape/`) and running fully offline — a tmp SQLite and a stubbed `_http_get`, no network. The SQL layers are exercised by the build's own integrity checks (`make explore`), not pytest.

## Project Structure

```text
sql/              DuckDB SQL layers
scripts/          Shell and Python utilities
docs/             Documentation source files
ingest_sources/   External data dumps ingested into DuckDB (gitignored, pulled from R2)
explore.duckdb    Build artifact (gitignored)
```

## DuckDB

### Schemas

Every relation lives in a schema naming the layer it belongs to: `<source>_raw`, `<source>_stg`, `<source>_ref`, and bare `<source>` for the published mart, plus `glossary`, `web_cache`, `ingest` and `checks`. [Explore.md](docs/Explore.md) has the table and which ones to read.

The rules no single file shows you:

- **Only the unsuffixed mart is a contract.** Flippatch reads it and nothing beneath, so changing a mart column is a cross-repo change and changing a staging one is not.
- **The direction is one-way.** A mart may read staging; staging must never read a mart.
- **`main` is deliberately empty.** A build that leaves anything there fails.
- **Marts select their staging views with `*`**, so a field a dump gains upstream surfaces and fails the build rather than disappearing silently. Don't replace a star with a column list. The reasoning, and what it does _not_ catch, is on the views in `sql/09_mart.sql`.
- **IPDB's word for a machine is `model`** everywhere past its raw layer. OPDB keeps `machines`, because an OPDB row can be a title or a model.

### SQL Layers

Files in `sql/` load in numeric order during `make explore`, and each states its own purpose in its first line — that is where to look, rather than a list here that goes stale the first time a layer is added or renamed. A new layer goes in the body; 80/90 are the closing gate and stay at the end.

The build **fails** if integrity checks don't pass, printing every violation as it aborts. `checks.violations` is a real table and its rows survive the abort, so a failed build can be reopened and queried.

## Web Evidence Cache

`scripts/web_scrape/web_fetch.py` fetches web pages into a durable, searchable cache used as attributed evidence for catalog corrections, falling back to archive.org's newest capture when a live fetch fails (a blocking host like `ipdb.org`, a dead site). The system-of-record is a SQLite database with raw blobs under `ingest_sources/web/`; `make explore` materializes it into `web_cache.pages` / `web_cache.fetches` via the local-only `03_raw_web.sql` layer, so those tables are absent in `--remote` mode.

Query it with `scripts/web_scrape/web_cache.py`. Its `search`, `quote`, `outline`, `section` and `get` are an **escalation ladder** — prefer the earlier, needle-driven rungs over whole-page reads. Read [WebCache.md](docs/WebCache.md) before a research session rather than guessing at the CLI: search scopes and syntax, OCR'd PDF sheets, citation locators, and the document library (thousands of known-but-unfetched documents) are all covered there.

### IPDB machine pages as structured data

`scripts/web_scrape/parse_ipdb.py` parses a cached IPDB machine page into the fields the xantari dump never had — `Project Date`, a `Production` status, `Concept by`, `Specialty`, `Easter Eggs`. `scripts/web_scrape/extract_ipdb_to_jsonl.py` runs it over every cached page into `ingest_sources/ipdb_archive/models.jsonl`, which this build folds in as `ipdb_raw.archive_models` and patch-authoring sessions in **flippatch** read directly.

It is derived: re-run it after a fetch campaign and the diff is what the campaign found. The emitted shape answers to `read_json_auto` and the constraints are not optional — they are in the module docstring, which is required reading before changing the output.

## Cloudflare R2

The web cache database + raw cache files and the DuckDB ingest source files are moved between developer machines via Cloudflare R2. `make pull` downloads them, and `scripts/rebuild_explore.py --remote` reads them in place. Both are human-only — see [Rules](#rules) and [Explore.md](docs/Explore.md).

START_CLAUDE

## Tool Usage

Use Context7 (`mcp__context7__resolve-library-id` and `mcp__context7__query-docs`) to look up current documentation when:

- Working with DuckDB SQL syntax or functions
- Answering questions about library APIs or best practices

GitHub access:

- Use the GitHub MCP server for read-only operations (listing/viewing issues, PRs, commits, files)
- Use the `gh` CLI for writes or auth-required actions (creating/updating/commenting/merging)

END_CLAUDE

START_AGENTS

## Environment Setup (Codex Cloud)

**Setup command**: `bash scripts/bootstrap`

After setup, use the standard commands:

```bash
make explore      # Rebuild DuckDB from SQL layers
make agent-docs   # Regenerate agent docs
```

**Notes:**

- Internet is disabled during task execution — all dependencies must be installed during setup
- Use the `gh` CLI for GitHub operations

END_AGENTS

## Pre-commit Hooks

Pre-commit hooks auto-regenerate `CLAUDE.md` and `AGENTS.md` when `docs/AGENTS.src.md` changes, and block direct edits to those generated files. Hooks also run Prettier and markdownlint on Markdown files, and detect secrets. Do not edit `CLAUDE.md` or `AGENTS.md` directly — edit `docs/AGENTS.src.md` instead.

## Rules

- Never run `make pull`, `make push`, `make all` or `rebuild_explore.py --remote` — these reach Cloudflare R2 and are human-only. Ask the user instead.
- Don't silence linter warnings — fix the underlying issue
- Never hardcode secrets — use environment variables via `.env`
- When writing or editing Markdown, never hard-wrap prose. Write each paragraph and list item as a single long line and let the viewer soft-wrap it. Hard line breaks inserted to fit ~80 columns produce choppy short lines in a narrow viewport. Exempt from this rule: tables, code blocks and the existing line structure of generated files.

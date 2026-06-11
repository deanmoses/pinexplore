# DuckDB Explore Database

The best way to explore the pindata catalog is via DuckDB.

The project contains a read-only DuckDB database for validating pindata catalog data, comparing it against
external sources (OPDB, IPDB, Fandom), and finding gaps.

DuckDB is purely an audit and exploration tool; pindata is the source of truth.

## Using it

```bash
make explore   # rebuild from SQL layers
```

Query via the Python `duckdb` package (installed by uv):

```python
import duckdb
con = duckdb.connect("explore.duckdb", read_only=True)
con.execute("FROM machines LIMIT 5").show()
```

**Do NOT use MotherDuck or the DuckDB CLI.** The Python `duckdb` package is the only
required dependency. AI agents should query through Python, not the CLI.

The database is a build artifact (gitignored). Rebuild whenever the pindata catalog
or source dumps change. The build **fails** if integrity checks don't pass —
query `SELECT * FROM _violations` for details.

## SQL layers

Files in `sql/` load in numeric order:

| File                    | Purpose                                              |
| ----------------------- | ---------------------------------------------------- |
| `01_reference.sql`      | Hand-maintained reference tables, macros, exceptions |
| `02_raw.sql`            | Turn pindata & external JSON into tables             |
| `03_raw_web.sql`        | Web evidence cache → raw source tables (local-only)  |
| `04_staging.sql`        | Per-source normalization (no cross-source joins)     |
| `05_error_checks.sql`   | Integrity checks. Hard violations abort the build    |
| `06_warning_checks.sql` | Soft checks that warn but don't abort                |
| `07_compare.sql`        | Cross-source comparison: do sources agree?           |
| `08_gaps.sql`           | Gap analysis: what's missing from pindata?           |
| `09_quality.sql`        | Slug quality, media audit, backfill proposals        |
| `10_popularity.sql`     | Title popularity composite scoring                   |
| `11_history.sql`        | Industry history: decade-level trends                |
| `90_print_warnings.sql` | Print accumulated warnings (always runs last)        |

The web cache layer (`03_raw_web.sql`) is local-only and skipped when its SQLite is
absent — see [WebCache.md](WebCache.md).

## Remote data (Cloudflare R2)

Ingest source files are stored in Cloudflare R2 for access by cloud-based tools.

```bash
make pull   # download R2 → local ingest_sources/
make push   # upload local ingest_sources/ → R2 (requires credentials)
```

### Rebuilding from R2

```bash
uv run python scripts/rebuild_explore.py --remote   # reads JSON from R2 instead of local files
```

## Related scripts

- `scripts/rebuild_explore.py` — build `explore.duckdb` from the SQL layers
- `scripts/cloud_store/{pull,push}_ingest_sources.py` — sync ingest sources with R2
- `scripts/web_scrape/web_fetch.py` + `web_cache.py` — fetch and query the web evidence cache (see [WebCache.md](WebCache.md))
- `scripts/glossary/parse_*_glossary.py` — parse saved glossary HTML dumps into JSON
- `scripts/apply_descriptions.py` — apply curated manufacturer descriptions to the pindata catalog

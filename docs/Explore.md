# DuckDB Explore Database

The best way to explore the data in Pinbase is via DuckDB.

The project contains a read-only DuckDB database for validating pinbase data, comparing it against
external sources (OPDB, IPDB, Fandom), and finding gaps.

DuckDB is purely an audit and exploration tool; Pinbase markdown is the source of truth.

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

The database is a build artifact (gitignored). Rebuild whenever pinbase markdown
or source dumps change. The build **fails** if integrity checks don't pass —
query `SELECT * FROM _violations` for details.

## SQL layers

Files in `sql/` load in numeric order:

| File               | Purpose                                              |
| ------------------ | ---------------------------------------------------- |
| `01_reference.sql` | Hand-maintained reference tables, macros, exceptions |
| `02_raw.sql`       | Turn pinbase & external JSON into tables             |
| `03_staging.sql`   | Per-source normalization (no cross-source joins)     |
| `04_checks.sql`    | Integrity checks. Hard violations abort the build    |
| `05_compare.sql`   | Cross-source comparison: do sources agree?           |
| `06_gaps.sql`      | Gap analysis: what's missing from pinbase?           |
| `07_quality.sql`   | Slug quality, media audit, backfill proposals        |

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

- `scripts/apply_markdown_updates.py` — applies backfills to markdown files
- `scripts/generate_missing_ipdb_data.py` — creates markdown for missing IPDB entities

# Writing Scripts to Edit Pindata from Pinexplore

This guide explains how to write a high-quality, safe script that edits Pindata catalog
records using insights discovered in Pinexplore.

## Overview

- **Pindata** is the source of truth — catalog records live there as Markdown files
- **Pinexplore** is read-only audit and exploration; it never modifies Pindata directly
- When Pinexplore analysis reveals a data change that needs to be made, write a one-off
  Python script and run it against the Pindata repo

## Where to write the script

Write one-off editing scripts to the **system temp directory** (e.g. `/tmp/my_task.py`).
Never commit them to either repo — they are ephemeral tools, not persistent code.

## Using Pindata library functions

Pindata ships Python utilities in `lib/` and `scripts/`. Add both to `sys.path` so your
script can import them directly without installing anything:

```python
import sys
from pathlib import Path

# Run this script from the pindata project root (cd ../pindata first)
PINDATA = Path.cwd()
sys.path.insert(0, str(PINDATA / "lib"))
sys.path.insert(0, str(PINDATA / "scripts"))
```

### Finding records

You can find the slug of a record in DuckDB and that will give you the path to the file in pindata.

If you need to iterate over all records, you can iterators from [`lib/catalog_loader.py`](https://github.com/deanmoses/pindata/blob/main/lib/catalog_loader.py):

```python
from lib.catalog_loader import iter_taxonomy, iter_all

# Iterate a single taxonomy directory
for record in iter_taxonomy("gameplay_features", catalog_dir=PINDATA / "catalog"):
    print(record.slug, record.frontmatter)

# Iterate every entity type (PINDATA = Path.cwd() when run from pindata root)
for record in iter_all(catalog_dir=PINDATA / "catalog"):
    ...
```

Pass `validate=False` only when iterating files that currently violate the schema (e.g.
during a migration that fixes the violation). The writing step always re-validates.

### Editing frontmatter fields

Use `apply_fields()` from [`scripts/apply_fields.py`](https://github.com/deanmoses/pindata/blob/main/scripts/apply_fields.py)
to add, update, or delete YAML frontmatter fields. It preserves all other content, inserts
fields in canonical schema order, and validates before writing.

### Editing the description body

Use `apply_description()` from [`scripts/apply_description.py`](https://github.com/deanmoses/pindata/blob/main/scripts/apply_description.py)
to update the Markdown body below the frontmatter:

```python
from apply_description import apply_description

apply_description(record.file_path, "New description text.")
```

## When Pindata lib functions are missing functionality

Add the missing capability directly to the Pindata library function rather than
reimplementing it in the temp script.

**Writing tests is mandatory.** Add tests to the corresponding
[`tests/test_*.py`](https://github.com/deanmoses/pindata/tree/main/tests) file,
following the existing pytest patterns:

- Use `tmp_path` fixtures and a symlinked `schema/` directory so validation works
- Test the happy path, edge cases (noop when field absent, list fields), and body preservation
- Run the full suite before executing the bulk script:

```bash
cd ../pindata && uv run pytest tests/
```

## Running the script

Always run via `uv run` from the **pindata project root** so that `Path.cwd()` resolves
correctly and the pindata venv (PyYAML, jsonschema, etc.) is available:

```bash
cd ../pindata && uv run python /tmp/my_task.py
```

## Verifying the results

Use multiple verification methods in order:

### 1. Spot-check files

Open 2–3 affected Markdown files and confirm the change looks correct — fields in the
right order, no corruption, body intact.

### 2. Catalog validation

Run Pindata's built-in schema validator across the entire catalog:

```bash
cd /Users/moses/dev/pindata && uv run python scripts/validate_catalog.py
```

All files should pass. Fix any errors before continuing.

### 3. DuckDB validation

Regenerate the JSON export, rebuild the Pinexplore database, and query it to confirm
the data changed as expected:

```bash
# Export updated catalog to JSON
cd /Users/moses/dev/pindata && uv run python scripts/export_catalog_json.py

# Rebuild the explore database
cd /Users/moses/dev/pinexplore && make explore
```

Then query the result:

```python
import duckdb

con = duckdb.connect("explore.duckdb", read_only=True)

# Example: confirm no gameplay features still have display_order
con.execute("SELECT * FROM gameplay_features WHERE display_order IS NOT NULL").show()
```

Use DuckDB queries tailored to what was changed — counts, spot checks on specific slugs,
or diff-style comparisons before and after are all good approaches.

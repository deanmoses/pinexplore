#!/usr/bin/env python3
"""Parse-check the DuckDB SQL layers without executing them.

Runs each ``sql/*.sql`` file through DuckDB's own parser (the exact dialect the
build uses) and reports syntax errors. This is offline and instant — it needs no
ingest sources and creates no tables, so it can't catch *semantic* errors
(unknown columns/tables, which only surface during ``make explore``), but it
catches typos, unbalanced parens, and malformed statements before they reach a
build.

Usage:
    python scripts/check_sql.py [FILE ...]   # defaults to sql/*.sql
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("Error: duckdb package not found (run: uv sync)")

SQL_DIR = Path("sql")


def _check(paths: list[Path]) -> int:
    """Parse each file; return the number that failed (also printed to stderr)."""
    con = duckdb.connect()  # in-memory; nothing is executed
    failures = 0
    for path in paths:
        try:
            con.extract_statements(path.read_text(encoding="utf-8"))
        except duckdb.ProgrammingError as exc:
            failures += 1
            first_line = str(exc).splitlines()[0] if str(exc) else "parse error"
            print(f"{path}: {first_line}", file=sys.stderr)
    return failures


def main() -> int:
    args = sys.argv[1:]
    paths = [Path(a) for a in args] if args else sorted(SQL_DIR.glob("*.sql"))
    if not paths:
        print(f"No SQL files found in {SQL_DIR}/", file=sys.stderr)
        return 1
    failures = _check(paths)
    if failures:
        print(f"{failures} file(s) failed to parse.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

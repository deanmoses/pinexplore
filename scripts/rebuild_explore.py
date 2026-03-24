#!/usr/bin/env python3
"""Rebuild explore.duckdb from SQL layers.

Usage: scripts/rebuild_explore.py [--remote] [--timeout SECONDS]

--remote   Read ingest sources from R2 instead of local files.
           Requires R2_PUBLIC_URL in .env or environment.

Local mode expects ingest_sources/ to be populated (pull from R2 first).
"""

import argparse
import os
import pathlib
import signal
import sys
import time

try:
    import duckdb
except ImportError:
    sys.exit("Error: duckdb package not found. Install with: pip install duckdb")

DB = "explore.duckdb"
SQL_DIR = pathlib.Path("sql")


def load_dotenv():
    """Load .env file into os.environ (key=value lines only)."""
    env_path = pathlib.Path(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def main():
    parser = argparse.ArgumentParser(description="Rebuild explore.duckdb")
    parser.add_argument("--remote", action="store_true",
                        help="Read ingest sources from R2")
    parser.add_argument("--timeout", type=int, default=20,
                        help="Per-layer timeout in seconds (default: 20)")
    args = parser.parse_args()

    # Clean up any existing database
    for f in [DB, f"{DB}.wal"]:
        if os.path.exists(f):
            os.remove(f)

    # Build the preamble for the raw layer
    if args.remote:
        load_dotenv()
        r2_url = os.environ.get("R2_PUBLIC_URL")
        if not r2_url:
            sys.exit("Error: Set R2_PUBLIC_URL in .env for --remote mode")
        raw_preamble = (
            f"INSTALL httpfs; LOAD httpfs; "
            f"SET VARIABLE ingest_base = '{r2_url}';"
        )
        print(f"Remote mode: reading ingest sources from {r2_url}")
    else:
        raw_preamble = "SET VARIABLE ingest_base = 'ingest_sources';"
        print("Local mode: reading from ingest_sources/")

    # Find SQL layers in order
    sql_files = sorted(SQL_DIR.glob("[0-9]*.sql"))
    if not sql_files:
        sys.exit(f"Error: no SQL files found in {SQL_DIR}/")

    print(f"Rebuilding {DB} (timeout: {args.timeout}s per layer)...")
    total_start = time.time()

    con = duckdb.connect(DB)

    for sql_path in sql_files:
        layer = sql_path.name
        layer_start = time.time()

        sql = sql_path.read_text()
        if layer == "02_raw.sql":
            sql = raw_preamble + "\n" + sql

        # Set a per-layer timeout via alarm
        def timeout_handler(signum, frame):
            raise TimeoutError(f"{layer} exceeded {args.timeout}s timeout")

        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(args.timeout)

        try:
            con.execute(sql)
            # After executing, print any rows from _warnings for
            # the warnings layer, and check for errors.
            if layer.endswith("_print_warnings.sql"):
                for row in con.execute(
                    "SELECT 'WARNING: ' || check_name || ' (' || cnt || ' rows)'"
                    " FROM _warnings WHERE cnt > 0"
                ).fetchall():
                    print(f"    {row[0]}")
            elif layer == "04_error_checks.sql":
                for row in con.execute(
                    "SELECT category || ': ' || count(*) FROM _violations"
                    " GROUP BY category ORDER BY category"
                ).fetchall():
                    print(f"    {row[0]}")
        except TimeoutError:
            elapsed = int(time.time() - layer_start)
            print(f"  FAILED {layer} after {elapsed}s (timeout)", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            elapsed = int(time.time() - layer_start)
            print(f"  FAILED {layer} after {elapsed}s", file=sys.stderr)
            print(f"  {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

        elapsed = int(time.time() - layer_start)
        print(f"  {layer} {elapsed}s")

    con.close()
    total = int(time.time() - total_start)
    print(f"OK in {total}s")


if __name__ == "__main__":
    main()

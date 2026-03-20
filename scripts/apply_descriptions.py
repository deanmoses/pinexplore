#!/usr/bin/env python3
"""Apply manufacturer descriptions from data/manufacturers/ to pindata catalog files.

Iterates over description files in data/manufacturers/*.md and calls
pindata's apply_description.py to write each one into the corresponding
catalog/manufacturers/{slug}.md file.

Usage:
    uv run python scripts/apply_descriptions.py [--overwrite] [--dry-run]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PINEXPLORE_ROOT = Path(__file__).resolve().parent.parent
PINDATA_ROOT = PINEXPLORE_ROOT.parent / "pindata"
DESCRIPTIONS_DIR = PINEXPLORE_ROOT / "data" / "manufacturers"
CATALOG_MFR_DIR = PINDATA_ROOT / "catalog" / "manufacturers"
APPLY_SCRIPT = PINDATA_ROOT / "scripts" / "apply_description.py"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting existing descriptions")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without writing")
    args = parser.parse_args()

    if not DESCRIPTIONS_DIR.is_dir():
        print(f"Error: descriptions directory not found: {DESCRIPTIONS_DIR}", file=sys.stderr)
        return 1

    if not APPLY_SCRIPT.is_file():
        print(f"Error: apply_description.py not found: {APPLY_SCRIPT}", file=sys.stderr)
        return 1

    desc_files = sorted(DESCRIPTIONS_DIR.glob("*.md"))
    if not desc_files:
        print("No description files found.")
        return 0

    applied = 0
    skipped = 0
    errors = 0

    for desc_path in desc_files:
        slug = desc_path.stem
        catalog_path = CATALOG_MFR_DIR / f"{slug}.md"

        if not catalog_path.is_file():
            print(f"  SKIP {slug}: no catalog file at {catalog_path}")
            skipped += 1
            continue

        if args.dry_run:
            print(f"  WOULD APPLY {slug}")
            applied += 1
            continue

        cmd = [
            sys.executable,
            str(APPLY_SCRIPT),
            str(catalog_path),
            str(desc_path),
        ]
        if args.overwrite:
            cmd.insert(2, "--overwrite")

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            print(f"  OK {slug}")
            applied += 1
        else:
            err = result.stderr.strip()
            print(f"  FAIL {slug}: {err}")
            errors += 1

    print(f"\nDone: {applied} applied, {skipped} skipped, {errors} errors")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Extract saved IPDB advanced searches into one observations file.

IPDB's advanced search can filter by Specialty, by Type, by year range, and more.
Each filter renders the same results table, so each saved page is a set of LIVE
OBSERVATIONS of machines -- IPDB's current name, date, manufacturer, type,
production, players, model number and specialties, read months after the xantari
dump last spoke.

That is the whole job here: not to publish rival field values, which
`ipdb.models` already takes from the dump, but to give the build something
current to CHECK the dump against. `ipdb_search_observation_disagrees_with_dump`
is the point of the corpus.

Every page under `ingest_sources/ipdb/ipdb_*/` is read EXCEPT the Specialty
searches, which `extract_ipdb_specialty_to_jsonl.py` owns. Those 27 pages are a
closed census with a vocabulary and a completeness proof behind them, and they
feed a mart view; folding them in here would duplicate every row and lose the
distinction. Staging unions the two into `ipdb_stg.live_observations`, which is
what the checks read.

A search is identified by WHERE IT WAS SAVED -- the folder names the kind, the
filename names the instance -- and not by reading the filter back off the page.
That is a deliberate retreat from what the Specialty extract does. Those pages
echo the search form with the searched option marked `selected`, so they
self-identify; a page saved as the server sent it may carry no such echo (the
Type search does not), and a rule that works on one markup and not the other is
worse than a filename.

WHAT ABSENCE MEANS IS THE CALLER'S PROBLEM, not this file's. Each download is
complete for whatever it filtered on -- every Pure Mechanical machine, every
machine dated 2010 or later -- but only the person who ran the search knows what
that filter was, so nothing here infers a machine's absence to mean anything.
The observations are positive statements only.

Re-running rewrites the file whole, so the diff is what changed at IPDB. Adding a
download means dropping a page in a folder and re-running; no SQL changes.

Usage:
    uv run python scripts/ipdb/extract_ipdb_searches_to_jsonl.py [--src DIR] [--out PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ipdb_search import ENCODING, Machine, ParseError, parse_results

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SRC = REPO_ROOT / "ingest_sources" / "ipdb"
DEFAULT_OUT = DEFAULT_SRC / "ipdb_searches" / "observations.jsonl"

# Owned by the Specialty extract, which proves them complete as a set.
SPECIALTY_DIR = "ipdb_specialty"

# Where the extract looks, and what it calls what it finds.
SEARCH_DIR_PREFIX = "ipdb_"
PAGE_SUFFIXES = (".htm", ".html")


def search_pages(src: Path) -> list[Path]:
    """Every saved search page, in a stable order."""
    return sorted(
        path
        for directory in src.iterdir()
        if directory.is_dir()
        and directory.name.startswith(SEARCH_DIR_PREFIX)
        and directory.name != SPECIALTY_DIR
        for path in directory.iterdir()
        if path.suffix.lower() in PAGE_SUFFIXES
    )


def build(src: Path) -> list[Machine]:
    """Every observation on every saved page, one row per machine per search."""
    pages = search_pages(src)
    if not pages:
        raise ParseError(f"no saved search pages under {src}/{SEARCH_DIR_PREFIX}*/")

    observations: list[Machine] = []
    for path in pages:
        # The folder is the kind of search, the file is which one.
        kind = path.parent.name.removeprefix(SEARCH_DIR_PREFIX)
        name = path.stem
        rows = parse_results(
            path.read_text(encoding=ENCODING, errors="strict"), path.name
        )

        seen: set[int] = set()
        for row in rows:
            if row["ipdb_id"] in seen:
                raise ParseError(f"{path.name}: IPDB {row['ipdb_id']} listed twice")
            seen.add(row["ipdb_id"])
            observations.append({"search_kind": kind, "search_name": name} | row)

    observations.sort(
        key=lambda row: (row["search_kind"], row["search_name"], row["ipdb_id"])
    )
    return observations


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--src", type=Path, default=DEFAULT_SRC, help="the ipdb ingest dir"
    )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, help="where the JSONL goes"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="parse and report, writing nothing"
    )
    args = parser.parse_args()

    try:
        observations = build(args.src)
    except ParseError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    searches = {(row["search_kind"], row["search_name"]) for row in observations}
    machines = {row["ipdb_id"] for row in observations}
    print(
        f"{len(observations)} observations of {len(machines)} machines "
        f"across {len(searches)} search(es)"
    )
    for kind, name in sorted(searches):
        n = sum(
            1
            for row in observations
            if (row["search_kind"], row["search_name"]) == (kind, name)
        )
        print(f"  {kind}/{name}: {n}")
    if args.dry_run:
        return 0

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as handle:
        for row in observations:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

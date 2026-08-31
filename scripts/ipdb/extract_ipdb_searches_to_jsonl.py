#!/usr/bin/env python3
"""Extract every saved IPDB advanced search into two JSONL files.

<https://www.ipdb.org/search.pl?searchtype=advanced> filters the live database
and renders the matches as one table. It can filter by Specialty, by Type and by
year range, and every filter renders that same table, so every saved page is read
the same way and this is the only extract there is.

The pages are behind a bot wall and are saved by hand under
``ingest_sources/ipdb/searches/<kind>/``. The folder names the kind of
search, the file names the instance, and dropping a page into a folder is the
whole act of adding a download -- no code change and no SQL change.

Two files come out, both read by ``sql/02_raw.sql`` through ``read_json_auto``:

* ``search_results.jsonl`` -- one row per IPDB model per saved page, holding that
  model's fields as that page's results table showed them. A model matched by
  three searches is three rows. They are NOT merged here, and that is the point:
  whether the copies agree is a question with an answer, and
  ``ipdb_live_observation_conflict`` asks it in SQL rather than this file
  quietly picking a winner.
* ``specialties.jsonl`` -- IPDB's 27 Specialties with its own numeric ids, read
  off the search form rather than transcribed. It is not derivable from the rows:
  a Specialty no model currently carries appears in the dropdown and nowhere
  else, and that is exactly the case worth catching.

NOTHING IS INTERPRETED HERE. This module parses pages and records what each one
said; every rule about what the corpus MEANS is a check in SQL. That includes the
ones this file could plausibly have enforced -- that the 27 Specialty searches
are complete as a set, that two pages describing one model agree, that IPDB's
dropdown still matches the rules written for it. Keeping them together in SQL
beats splitting them across two languages, and the row's own ``search_filter``
is what makes them expressible there.

Each row carries where it came from and what its page filtered on:

* ``search_kind`` / ``search_name`` -- the folder and the file.
* ``search_filter`` -- the Specialty the page searched for, taken from the
  ``selected`` option in the echoed form. NULL where the page does not say, which
  is not an oversight: the Type search echoes no such form back at all. Only the
  Specialty pages state their filter, and they are the only ones a check needs it
  from.

What a search's absence MEANS is nowhere in this file. Each download is complete
for whatever it filtered on, but a page does not always say what that was, so the
corpus holds positive observations and the reader decides what to make of a gap.

Re-running rewrites both files whole, so the diff is what changed at IPDB.

Usage:
    uv run python scripts/ipdb/extract_ipdb_searches_to_jsonl.py [--src DIR] [--out DIR] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ipdb_search import (
    ENCODING,
    SEARCH_URL,
    Model,
    ParseError,
    parse_results,
    parse_specialty_filter,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SRC = REPO_ROOT / "ingest_sources" / "ipdb" / "searches"
DEFAULT_OUT = DEFAULT_SRC

PAGE_SUFFIXES = (".htm", ".html")


def search_pages(src: Path) -> list[Path]:
    """Every saved page, in a stable order. One directory level, no deeper."""
    return sorted(
        path
        for directory in src.iterdir()
        if directory.is_dir()
        for path in directory.iterdir()
        if path.suffix.lower() in PAGE_SUFFIXES
    )


def build(src: Path) -> tuple[list[Model], list[Model]]:
    """Every saved page under `src` into the two records the raw layer reads."""
    pages = search_pages(src)
    if not pages:
        raise ParseError(f"no saved search pages under {src}/*/")

    results: list[Model] = []
    terms: dict[str, int] = {}
    stated_by: dict[str, str] = {}

    for path in pages:
        page = path.read_text(encoding=ENCODING, errors="strict")
        kind, name = path.parent.name, path.stem

        search_filter = None
        echoed = parse_specialty_filter(page, path.name)
        if echoed is not None:
            page_terms, search_filter = echoed
            # Every page that echoes the form echoes the same one, so a
            # disagreement means the download spans a change at IPDB.
            if terms and terms != page_terms:
                raise ParseError(
                    f"{path.name}: its Specialty dropdown differs from earlier pages' -- "
                    "the download spans a change to IPDB's Specialty list"
                )
            terms = page_terms
        if search_filter is not None:
            if search_filter in stated_by:
                raise ParseError(
                    f"{path.name}: searches for {search_filter!r}, and so does "
                    f"{stated_by[search_filter]}"
                )
            stated_by[search_filter] = path.name

        seen: set[int] = set()
        for row in parse_results(page, path.name):
            if row["ipdb_id"] in seen:
                raise ParseError(f"{path.name}: IPDB {row['ipdb_id']} listed twice")
            seen.add(row["ipdb_id"])
            results.append(
                {
                    "search_kind": kind,
                    "search_name": name,
                    "search_filter": search_filter,
                }
                | row
            )

    if not terms:
        raise ParseError(
            "no saved page echoes IPDB's Specialty dropdown, so the Specialty list "
            "cannot be read; save one Specialty search as the server sends it"
        )

    results.sort(
        key=lambda row: (row["search_kind"], row["search_name"], row["ipdb_id"])
    )
    specialties = [
        {
            "specialty_id": specialty_id,
            "specialty": specialty,
            "source_url": SEARCH_URL.format(id=specialty_id),
            # Whether any saved page searched for this Specialty. False is not an
            # error here; `ipdb_specialty_not_downloaded` decides what it means.
            "downloaded": specialty in stated_by,
        }
        for specialty, specialty_id in sorted(terms.items())
    ]
    return results, specialties


def write(records: list[Model], path: Path) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--src", type=Path, default=DEFAULT_SRC, help="the saved searches"
    )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, help="where the JSONL goes"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="parse and report, writing nothing"
    )
    args = parser.parse_args()

    try:
        results, specialties = build(args.src)
    except ParseError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    searches = {(row["search_kind"], row["search_name"]) for row in results}
    models = {row["ipdb_id"] for row in results}
    print(
        f"{len(results)} rows for {len(models)} models across {len(searches)} searches, "
        f"{len(specialties)} Specialties in IPDB's list"
    )
    for kind in sorted({kind for kind, _ in searches}):
        pages = sum(1 for k, _ in searches if k == kind)
        rows = sum(1 for row in results if row["search_kind"] == kind)
        print(f"  {kind}: {pages} page(s), {rows} rows")
    if args.dry_run:
        return 0

    args.out.mkdir(parents=True, exist_ok=True)
    write(results, args.out / "search_results.jsonl")
    write(specialties, args.out / "specialties.jsonl")
    print(
        f"wrote {args.out / 'search_results.jsonl'} and {args.out / 'specialties.jsonl'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

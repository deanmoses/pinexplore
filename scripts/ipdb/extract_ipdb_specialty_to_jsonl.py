#!/usr/bin/env python3
"""Extract IPDB's advanced-search Specialty results into JSONL.

IPDB's advanced search filters the live database by one Specialty and lists
every machine carrying it:

    https://www.ipdb.org/search.pl?specialty=25&sortby=name&searchtype=advanced

Run once per Specialty, the results are a CENSUS -- the complete classification
of every machine IPDB classifies, taken at one moment. That is what separates
this source from the other two IPDB reads. The xantari dump has never carried
Specialty at all, and the archive.org pages carry it one model at a time, from
captures spanning years. Neither can say what this says: that a machine ABSENT
here has no specialty, which is a fact rather than a gap.

The pages are behind a bot wall, so they are downloaded by hand into
``ingest_sources/ipdb/ipdb_specialty/`` and this reads them off disk. There is
no fetcher to write.

Four properties of the download make it checkable rather than merely trusted,
and this module asserts all four rather than assuming them:

* **Each page states its own query.** The search form is echoed back with the
  searched Specialty's ``<option>`` marked ``selected``, carrying IPDB's own
  numeric id. So a file self-identifies -- the filename is a convenience, never
  read -- and the citable URL is reconstructed rather than guessed.
* **Each page states its own size.** ``(N records match)`` precedes the table,
  and the table holds exactly N rows. A truncated or half-saved download fails
  here instead of quietly shipping a short census.
* **Every row lists the machine's WHOLE specialty set**, not just the one
  searched for, with the full wording in each ``<span title=>`` even where the
  cell text is elided (``Shaker Ball Mac...``). Read the titles, never the text.
* **The corpus is closed.** A specialty named in any row's cell must also have
  its own page listing that machine. This cross-check is what proves the
  download complete; without it a missing file reads as machines simply not
  having that specialty.

The row also carries date, manufacturer, type, production, players, model
number, photo count and rating. These are not published as rival values --
xantari already states them and is the field-level source -- they are carried so
the build can CROSS-CHECK a live read against a dump that is months old. That
check earns its keep: it already disagrees with the dump on IPDB machine 1146,
where IPDB says "Hearts & Spades" and the dump lost the ampersand.

Two files are written, both read by ``sql/02_raw.sql`` through
``read_json_auto``:

* ``census.jsonl`` -- one row per machine, sorted by ``ipdb_id``. ``specialties``
  is a list of fixed-key structs rather than a bare list of strings, so each
  assignment carries the id and URL that evidence it. Every key is present on
  every row, ``null`` when absent, so a row's inferred type does not depend on
  which rows were read.
* ``vocabulary.jsonl`` -- the Specialty dropdown as the download found it, one
  row per option. This is the live vocabulary riding along with the data, and
  ``ipdb_specialty_vocabulary_drifted`` checks it against ``ipdb_ref.specialty``
  in both directions -- which is how a Specialty IPDB adds after this download
  becomes a build failure rather than a silent absence.

Nothing here dates the artifact. The pages carry no timestamp, and file mtime
dates an R2 sync rather than the download, so the acquisition date is recorded
by hand in ``ref.artifact_acquisitions``.

Re-running over a fresh download rewrites both files whole, so the diff is what
changed at IPDB.

Usage:
    uv run python scripts/ipdb/extract_ipdb_specialty_to_jsonl.py [--src DIR] [--out DIR] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from ipdb_search import ENCODING, Machine, ParseError, _text, parse_results

# One saved page, as (its searched specialty, its rows).
Page = tuple[tuple[str, int], list[Machine]]

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SRC = REPO_ROOT / "ingest_sources" / "ipdb" / "ipdb_specialty"
DEFAULT_OUT = DEFAULT_SRC

SEARCH_URL = (
    "https://www.ipdb.org/search.pl?specialty={id}&sortby=name&searchtype=advanced"
)

# The dropdown's do-not-filter option, which is not a Specialty.
ANY_SPECIALTY_ID = "0"

_SPECIALTY_SELECT = re.compile(
    r'<select[^>]*name="specialty"[^>]*>(.*?)</select>', re.DOTALL
)
_OPTION = re.compile(r"<option([^>]*)>(.*?)</option>", re.DOTALL)
_OPTION_VALUE = re.compile(r'value="(\d+)"')


def parse_vocabulary(page: str, source: str) -> tuple[dict[str, int], tuple[str, int]]:
    """The Specialty dropdown, and which of its options this page searched for.

    Returns the whole vocabulary as `{specialty: id}` alongside the one option
    marked `selected`. Every page echoes the same dropdown, so the vocabulary is
    a property of the download rather than of the file it was read from.
    """
    select = _SPECIALTY_SELECT.search(page)
    if select is None:
        raise ParseError(f"{source}: no specialty dropdown; is this a search page?")

    vocabulary: dict[str, int] = {}
    selected: tuple[str, int] | None = None
    for attributes, label in _OPTION.findall(select.group(1)):
        value = _OPTION_VALUE.search(attributes)
        if value is None or value.group(1) == ANY_SPECIALTY_ID:
            continue
        name = _text(label)
        specialty_id = int(value.group(1))
        if name in vocabulary:
            raise ParseError(
                f"{source}: specialty {name!r} listed twice in the dropdown"
            )
        vocabulary[name] = specialty_id
        if "selected" in attributes:
            if selected is not None:
                raise ParseError(f"{source}: two specialties marked selected")
            selected = (name, specialty_id)

    if selected is None:
        raise ParseError(
            f"{source}: no specialty marked selected -- an unfiltered search, "
            "or the form was not echoed back"
        )
    return vocabulary, selected


def parse_page(
    page: str, source: str
) -> tuple[dict[str, int], tuple[str, int], list[Machine]]:
    """One saved search page: its vocabulary, the specialty it searched, its rows."""
    vocabulary, selected = parse_vocabulary(page, source)
    return vocabulary, selected, parse_results(page, source)


def merge(pages: dict[str, Page]) -> dict[int, Machine]:
    """Fold every page's rows into one record per machine.

    A machine with several specialties is listed on several pages, and its
    non-specialty fields are then read more than once. They must agree -- the
    pages were saved minutes apart from one database -- and a disagreement means
    the download spans an IPDB edit, so it raises rather than picking a winner.
    """
    machines: dict[int, Machine] = {}
    for source, (_, rows) in sorted(pages.items()):
        for row in rows:
            row = dict(row)
            # Lifted out under a private name: the shared parser gives each row
            # the specialties ITS page listed, and what a machine carries is the
            # union across pages. Keeping it out of `row` is also what lets the
            # rest of the record be compared for equality below.
            specialties = row.pop("specialties")
            ipdb_id = row["ipdb_id"]
            existing = machines.get(ipdb_id)
            if existing is None:
                machines[ipdb_id] = row | {"_specialties": set(specialties)}
                continue
            held = {
                key: value for key, value in existing.items() if key != "_specialties"
            }
            if held != row:
                differing = {k: (held[k], row[k]) for k in held if held[k] != row[k]}
                raise ParseError(
                    f"IPDB {ipdb_id} differs between pages ({source} and an earlier "
                    f"one): {differing} -- the download spans an edit at IPDB"
                )
            existing["_specialties"].update(specialties)
    return machines


def check_closure(machines: dict[int, Machine], pages: dict[str, Page]) -> None:
    """Every specialty a row claims must have its own page listing that machine.

    This is what proves the download COMPLETE rather than merely consistent. Each
    row states the machine's whole specialty set, so a specialty nobody
    downloaded still shows up in other pages' cells -- and would otherwise be
    published as an assignment nothing corroborates, while the machines carrying
    only that specialty went missing entirely.
    """
    listed_by: dict[str, set[int]] = {}
    for (specialty, _), rows in pages.values():
        listed_by.setdefault(specialty, set()).update(row["ipdb_id"] for row in rows)

    missing_pages = sorted(
        {
            specialty
            for machine in machines.values()
            for specialty in machine["_specialties"]
            if specialty not in listed_by
        }
    )
    if missing_pages:
        raise ParseError(
            "no downloaded page for specialty/ies "
            + ", ".join(repr(name) for name in missing_pages)
            + " -- rows name them, so the census is incomplete until they are saved"
        )

    unlisted = [
        (ipdb_id, specialty)
        for ipdb_id, machine in machines.items()
        for specialty in machine["_specialties"]
        if ipdb_id not in listed_by[specialty]
    ]
    if unlisted:
        sample = ", ".join(
            f"{ipdb_id} ({specialty})" for ipdb_id, specialty in unlisted[:5]
        )
        raise ParseError(
            f"{len(unlisted)} assignment(s) name a specialty whose own page omits the "
            f"machine, e.g. {sample} -- pages were saved at different times"
        )


def build(src: Path) -> tuple[list[Machine], list[Machine]]:
    """Every saved page in `src` into the two records the raw layer reads."""
    sources = sorted(src.glob("*.html"))
    if not sources:
        raise ParseError(f"no saved search pages in {src}")

    pages: dict[str, Page] = {}
    vocabulary: dict[str, int] = {}
    for path in sources:
        page_vocabulary, selected, rows = parse_page(
            path.read_text(encoding=ENCODING, errors="strict"), path.name
        )
        # Every page echoes the same form, so a disagreement means the pages were
        # downloaded either side of IPDB changing its vocabulary.
        if vocabulary and vocabulary != page_vocabulary:
            raise ParseError(
                f"{path.name}: its specialty dropdown differs from earlier pages' -- "
                "the download spans a vocabulary change at IPDB"
            )
        vocabulary = page_vocabulary
        if selected[0] in {held[0] for held, _ in pages.values()}:
            raise ParseError(f"{path.name}: specialty {selected[0]!r} saved twice")
        pages[path.name] = (selected, rows)

    machines = merge(pages)
    check_closure(machines, pages)

    # A dropdown option nobody searched for. Not fatal: IPDB may genuinely
    # classify nothing under it, and the closure check above already catches the
    # case that matters -- a specialty other rows name with no page behind it.
    searched = {specialty for (specialty, _), _ in pages.values()}
    for name in sorted(set(vocabulary) - searched):
        print(
            f"note: no saved page for {name!r}; its machines are absent",
            file=sys.stderr,
        )

    census: list[Machine] = []
    for ipdb_id in sorted(machines):
        machine = dict(machines[ipdb_id])
        specialties = sorted(machine.pop("_specialties"))
        census.append(
            machine
            | {
                # Fixed-key structs rather than bare strings: each assignment
                # carries the id and the URL that evidence it, so a row cites
                # without a join back to the vocabulary.
                "specialties": [
                    {
                        "specialty": name,
                        "specialty_id": vocabulary[name],
                        "source_url": SEARCH_URL.format(id=vocabulary[name]),
                    }
                    for name in specialties
                ]
            }
        )

    terms = [
        {
            "specialty_id": specialty_id,
            "specialty": name,
            "source_url": SEARCH_URL.format(id=specialty_id),
            "downloaded": name in searched,
        }
        for name, specialty_id in sorted(vocabulary.items())
    ]
    return census, terms


def write(records: list[Machine], path: Path) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--src", type=Path, default=DEFAULT_SRC, help="saved search pages"
    )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, help="where the JSONL goes"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="parse and report, writing nothing"
    )
    args = parser.parse_args()

    try:
        census, vocabulary = build(args.src)
    except ParseError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    assignments = sum(len(machine["specialties"]) for machine in census)
    print(
        f"{len(census)} machines, {assignments} specialty assignments, "
        f"{len(vocabulary)} terms in IPDB's vocabulary"
    )
    if args.dry_run:
        return 0

    args.out.mkdir(parents=True, exist_ok=True)
    write(census, args.out / "census.jsonl")
    write(vocabulary, args.out / "vocabulary.jsonl")
    print(f"wrote {args.out / 'census.jsonl'} and {args.out / 'vocabulary.jsonl'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

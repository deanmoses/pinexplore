#!/usr/bin/env python3
"""Extract every cached IPDB machine page into one JSONL file.

Reads the raw HTML blobs behind the web cache's ``machine.cgi?id=N`` pages,
runs each through :func:`parse_ipdb.parse_model_page`, and writes one JSON
object per model to ``ingest_sources/ipdb_archive/models.jsonl``.

Two consumers read it, both through ``read_json_auto``. This repo's own build
folds it in beside the xantari dump as ``ipdb_raw.archive_models`` (see
``sql/02_raw.sql``), and patch-authoring sessions in flippatch read it from
DuckDB alongside the Flipcommons analytics layer — the same cross-repo reach
`flippatch/scripts/analysis/evidence.sql` already makes into this cache:

    SELECT * FROM read_json_auto(
      '../pinexplore/ingest_sources/ipdb_archive/models.jsonl', sample_size = -1);

``sample_size = -1`` is not optional advice. Inference over a sample types a
column by the rows it saw, and the rarest fields here are on a handful of
models in six thousand (one page in the current corpus states Easter Eggs), so
a sampled read can type a real struct as NULL and lose it silently.

Three properties of the emitted shape exist for that reader:

* **No dynamic-key objects.** ``credits``, ``documents`` and the unknown-label
  backstop are label-keyed maps in :class:`~parse_ipdb.IpdbModel`. As JSON
  objects each row would infer a *different* STRUCT type and the union of them
  would be unusable, so each is emitted as a list of fixed-key structs and the
  label rides as a value (``role``, ``section``, ``label``).
* **Every key on every row**, ``null`` or ``[]`` when absent, at every depth —
  so a row's inferred type does not depend on which rows were read.
* **Nothing padded.** A date IPDB stated as a month stays a month: ``iso`` is
  ``1989-05``, and ``day`` is null rather than the 1st.

What is dropped, and why: the parser's ``fields`` map — the verbatim text of
every labeled row — is over half the bytes and, for a label this parser models,
restates a value already typed here. Only rows under a label the parser does
*not* model survive, as ``unknown_fields``. That keeps the artifact lossless
where lossiness would actually cost something: the day IPDB prints a label
nobody has seen, its text is here and the parser needs a case for it. The image
grid is dropped outright — the catalog's media work does not read from here.

``unknown_fields`` is also the one column whose DuckDB type moves: empty on
every row (the corpus's healthy state) it infers as ``JSON[]``, and gains a
real struct the day a page carries a label the parser has no case for. Ask it
``len(unknown_fields) > 0``, which reads the same either way, before reaching
into an element.

The file is one row per model, sorted by ``ipdb_id``, and derives from the
cache alone, so re-running after a fetch campaign rewrites it whole and the
diff is the campaign.

Usage:
    uv run python scripts/web_scrape/extract_ipdb_to_jsonl.py [--out PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import web_cache
from parse_ipdb import (
    Citation,
    Field,
    IpdbDate,
    IpdbModel,
    IpdbParseError,
    Link,
    Manufacturer,
    Mpu,
    NotAModelPageError,
    Production,
    Rating,
    parse_model_page,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable, Sequence

    from web_cache import PageRow

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
OUT_PATH = REPO_ROOT / "ingest_sources" / "ipdb_archive" / "models.jsonl"

# The cache key of a machine page. LIKE, not GLOB: `?` is a literal here and a
# single-character wildcard there.
_PAGE_LIKE = "https://www.ipdb.org/machine.cgi?id=%"
_PAGE_ID_RE = re.compile(r"^https://www\.ipdb\.org/machine\.cgi\?id=(\d+)$")


# --------------------------------------------------------------------------- #
# Value conversion
#
# One function per parsed type rather than a reflective walk: the key set of
# each struct is then written down, which is what makes a row's shape
# independent of which fields that particular model happens to state.
# --------------------------------------------------------------------------- #


def _date(value: IpdbDate | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "text": value.text,
        "year": value.year,
        "month": value.month,
        "day": value.day,
        "iso": value.iso,
        "precision": value.precision.value,
    }


def _link(value: Link) -> dict[str, Any]:
    return {"text": value.text, "url": value.url}


def _citation(value: Citation | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {"text": value.text, "url": value.url}


def _manufacturer(value: Manufacturer | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "text": value.text,
        "name": value.name,
        "ipdb_id": value.ipdb_id,
        "trade_name": value.trade_name,
        "location": value.location,
        "first_year": value.first_year,
        "last_year": value.last_year,
    }


def _mpu(value: Mpu | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {"name": value.name, "ipdb_id": value.ipdb_id}


def _production(value: Production | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "text": value.text,
        "units": value.units,
        "qualifier": value.qualifier.value if value.qualifier else None,
        "status": value.status,
        "never_produced": value.never_produced,
    }


def _rating(value: Rating | None) -> dict[str, Any] | None:
    if value is None:
        return None
    # Decimal to float: the score is one printed decimal place ("7.5/10"), so
    # a double holds what IPDB stated and JSON has nowhere better to put it.
    return {
        "text": value.text,
        "score": float(value.score) if value.score is not None else None,
        "ratings": value.ratings,
        "comments": value.comments,
        "provisional": value.provisional,
    }


def _credits(model: IpdbModel) -> list[dict[str, Any]]:
    """The "… by" rows flattened to one row per credited person, page order."""
    return [
        {"role": role, "name": person.name, "url": person.url}
        for role, people in model.credits.items()
        for person in people
    ]


def _documents(model: IpdbModel) -> list[dict[str, Any]]:
    """Every document listing flattened to one row per file, page order."""
    return [
        {
            "section": section,
            "name": entry.name,
            "url": entry.url,
            "kind": entry.kind,
            "size": entry.size,
            "credit": entry.credit,
        }
        for section, entries in model.documents.items()
        for entry in entries
    ]


def _unknown_fields(model: IpdbModel) -> list[dict[str, Any]]:
    """The labeled rows this parser has no case for, verbatim.

    Non-empty means IPDB is stating something the parser drops on the floor,
    so it is both the escape hatch for the reader and the signal to model it.
    """
    out: list[dict[str, Any]] = []
    for label in model.unknown_labels:
        field: Field | None = model.fields.get(label)
        if field is None:
            continue
        out.append(
            {
                "label": field.label,
                "text": field.text,
                "lines": list(field.lines),
                "links": [_link(link) for link in field.links],
            }
        )
    return out


def _date_source(model: IpdbModel) -> str | None:
    """Which row ``date`` came from — and the flag on the ambiguous case.

    ``header`` means IPDB printed a date in the page header but stated neither
    a Date Of Manufacture nor a Project Date row, so nothing on the page says
    whether that year is when the machine shipped or when the design was filed.
    That ambiguity is exactly what the catalog's own date fields must not
    inherit blindly.
    """
    if model.manufacture_date is not None:
        return "manufacture"
    if model.project_date is not None:
        return "project"
    if model.header_date is not None:
        return "header"
    return None


def to_row(model: IpdbModel, page: PageRow) -> dict[str, Any]:
    """One JSONL row: the parse, plus the provenance a citation needs."""
    return {
        "ipdb_id": model.ipdb_id,
        "name": model.name,
        "players": model.players,
        # Production date if IPDB states one, else project — the same choice
        # its own header line makes, and the one the catalog derives.
        "date": _date(model.date),
        "date_source": _date_source(model),
        "header_date": _date(model.header_date),
        "manufacture_date": _date(model.manufacture_date),
        "project_date": _date(model.project_date),
        "manufacturer": _manufacturer(model.manufacturer),
        "model_number": model.model_number,
        "common_abbreviations": list(model.common_abbreviations),
        "mpu": _mpu(model.mpu),
        "type_code": model.type_code.value if model.type_code else None,
        "type_text": model.type_text,
        "production": _production(model.production),
        "rating": _rating(model.rating),
        "themes": list(model.themes),
        "specialties": list(model.specialties),
        "notable_features": model.notable_features,
        "toys": model.toys,
        "easter_eggs": _citation(model.easter_eggs),
        "notes": model.notes,
        "marketing_slogans": list(model.marketing_slogans),
        "photos_in": [_citation(c) for c in model.photos_in],
        "source": model.source,
        "credits": _credits(model),
        "rule_sheets": [_link(link) for link in model.rule_sheets],
        "additional_media": [_link(link) for link in model.additional_media],
        "serial_number_database_url": model.serial_number_database_url,
        "owners_list_url": model.owners_list_url,
        "documents": _documents(model),
        "unknown_fields": _unknown_fields(model),
        # Provenance. `source_url` is the address a patch cites; `raw_url` is
        # where the bytes actually came from, which for an archived page is a
        # Wayback capture — and then `archive_capture_date` is the date the
        # quoted words were true, not today.
        "source_url": page["url"],
        "raw_url": page["raw_url"],
        "archive_capture_date": web_cache.archive_capture_date(page),
        "content_sha": page["content_sha"],
        "last_fetched_at": page["last_fetched_at"],
    }


# --------------------------------------------------------------------------- #
# Extraction
# --------------------------------------------------------------------------- #


class Report:
    """What the run did, in the shape the operator needs to act on it."""

    def __init__(self) -> None:
        self.pages = 0
        self.rows: list[dict[str, Any]] = []
        self.not_a_model: list[str] = []
        self.no_blob: list[str] = []
        self.unreadable: list[tuple[str, str]] = []
        self.id_mismatch: list[tuple[str, int]] = []
        self.duplicates: list[tuple[int, str]] = []
        self.unknown_labels: dict[str, int] = {}


def _pages(con: sqlite3.Connection) -> Iterable[PageRow]:
    cursor = con.execute(
        "SELECT * FROM pages WHERE url LIKE ? ORDER BY url", (_PAGE_LIKE,)
    )
    for row in cursor:
        yield cast("PageRow", dict(row))


def extract(con: sqlite3.Connection | None = None) -> Report:
    """Parse every cached machine page into rows, sorted by ``ipdb_id``."""
    own = con is None
    db = con if con is not None else web_cache.connect(read_only=True)
    report = Report()
    by_id: dict[int, dict[str, Any]] = {}
    try:
        for page in _pages(db):
            report.pages += 1
            url = page["url"]
            blob = web_cache.blob_for(page)
            if blob is None or not blob.exists():
                report.no_blob.append(url)
                continue
            try:
                model = parse_model_page(blob.read_bytes())
            except NotAModelPageError:
                # IPDB answers an id it doesn't hold with its front page, so an
                # archive capture taken before a machine was added is a normal
                # member of the corpus, not a fault.
                report.not_a_model.append(url)
                continue
            except IpdbParseError as error:
                report.unreadable.append((url, str(error)))
                continue

            match = _PAGE_ID_RE.match(url)
            if match is not None and int(match.group(1)) != model.ipdb_id:
                # The cached bytes are some other machine's page. Writing the
                # parse anyway would attribute one model's facts to another.
                report.id_mismatch.append((url, model.ipdb_id))
                continue

            for label in model.unknown_labels:
                report.unknown_labels[label] = report.unknown_labels.get(label, 0) + 1

            if model.ipdb_id in by_id:
                report.duplicates.append((model.ipdb_id, url))
                continue
            by_id[model.ipdb_id] = to_row(model, page)
    finally:
        if own:
            db.close()
    report.rows = [by_id[key] for key in sorted(by_id)]
    return report


def write(rows: Sequence[dict[str, Any]], out: Path) -> None:
    """Write the JSONL, replacing any prior file atomically.

    Through a temp file so an interrupted run cannot leave a half-written
    artifact that reads as a complete, shorter corpus.
    """
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
    tmp.replace(out)


def _print_report(report: Report, out: Path, *, wrote: bool) -> None:
    print(f"pages scanned: {report.pages}")
    print(f"models {'written' if wrote else 'extracted'}: {len(report.rows)}")
    if report.not_a_model:
        print(
            f"skipped, IPDB served its front page (id not in that capture): "
            f"{len(report.not_a_model)}"
        )
    for url in report.no_blob:
        print(f"WARNING: cached row has no raw blob on disk: {url}", file=sys.stderr)
    for url, error in report.unreadable:
        print(f"WARNING: unreadable page: {url}: {error}", file=sys.stderr)
    for url, parsed in report.id_mismatch:
        print(
            f"WARNING: capture is a different machine's page (parsed IPD No. "
            f"{parsed}): {url}",
            file=sys.stderr,
        )
    for ipdb_id, url in report.duplicates:
        print(f"WARNING: duplicate IPD No. {ipdb_id}, dropped: {url}", file=sys.stderr)
    if report.unknown_labels:
        print("\nlabels this parser does not model (carried as unknown_fields):")
        for label, count in sorted(
            report.unknown_labels.items(), key=lambda item: (-item[1], item[0])
        ):
            print(f"  {count:5d}  {label}")
    if wrote:
        print(f"\nwrote {out}")


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--out", type=Path, default=OUT_PATH, help=f"output path (default: {OUT_PATH})"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="parse and report, write nothing"
    )
    args = parser.parse_args()

    report = extract()
    if not args.dry_run and report.rows:
        write(report.rows, args.out)
    _print_report(report, args.out, wrote=bool(report.rows) and not args.dry_run)

    if not report.rows:
        print("ERROR: no machine pages in the web cache", file=sys.stderr)
        return 1
    # A page that is HTML but not readable as a machine page is a parser
    # regression, not a property of the corpus: fail so a build notices.
    return 1 if report.unreadable else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Resolve document subjects and citation refs against the Flipcommons dev DB.

Re-runnable. Subjects join their IPDB ids against ``catalog_machinemodel``
(model scope) and ``catalog_corporateentity`` (corporate-entity scope);
writes go through ``attach_document_subject``, so PKs overwrite (repairing a
Flipcommons rebuild), labels refresh to the current catalog name, and an
unchanged row is untouched. ``documents.citation_ref`` resolves by URL join
against slug-addressed citation sources, fill-only: refs are frozen, so a
stored ref that disagrees is reported for a person, never overwritten.
Unresolved subjects are counted and left standing — they still search by
their IPDB names.

Usage:
    uv run python scripts/web_scrape/web_enrich_flipcommons.py [--flipcommons-db PATH] [--dry-run]
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import TypedDict

import web_cache


class SubjectTarget(TypedDict):
    """A Flipcommons row an IPDB id resolves to: its PK and current name."""

    pk: int
    name: str


class CitationRef(TypedDict):
    """A slug-addressed citation source: the URL it lives at, and its ref."""

    url: str
    ref: str


class FlipcommonsMaps(TypedDict):
    """Everything ``collect()`` reads out of Flipcommons, keyed by IPDB id."""

    models: dict[int, SubjectTarget]
    entities: dict[int, SubjectTarget]
    refs: list[CitationRef]


class RefMismatch(TypedDict):
    """A stored citation ref that disagrees with the resolved one."""

    document_id: int
    stored: str
    resolved: str


class EnrichCounts(TypedDict):
    """What one enrichment pass did — the run's whole report."""

    subjects_resolved: int
    subjects_unresolved: int
    refs_filled: int
    ref_mismatches: list[RefMismatch]


DEFAULT_FLIPCOMMONS_DB = Path("~/dev/flipcommons/backend/db.sqlite3").expanduser()


def collect(flipcommons_db: Path) -> FlipcommonsMaps:
    """Pull the resolution maps out of the Flipcommons dev DB as plain data.

    The only function that touches Flipcommons, so ``enrich()`` stays
    testable offline and the dependency stays enrichment-time only.
    """
    con = sqlite3.connect(f"file:{flipcommons_db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        models = {
            r["ipdb_id"]: SubjectTarget(pk=r["id"], name=r["name"])
            for r in con.execute(
                "SELECT id, name, ipdb_id FROM catalog_machinemodel "
                "WHERE ipdb_id IS NOT NULL"
            )
        }
        entities = {
            r["ipdb_manufacturer_id"]: SubjectTarget(pk=r["id"], name=r["name"])
            for r in con.execute(
                "SELECT id, name, ipdb_manufacturer_id FROM catalog_corporateentity "
                "WHERE ipdb_manufacturer_id IS NOT NULL"
            )
        }
        # Slug-addressed children only: their cite ref is parent:child. A web
        # source's ref is its URL — nothing to resolve.
        refs = [
            CitationRef(url=r["url"], ref=f"{r['parent_slug']}:{r['child_slug']}")
            for r in con.execute(
                "SELECT l.url AS url, p.slug AS parent_slug, s.slug AS child_slug "
                "FROM citation_citationsourcelink AS l "
                "JOIN citation_citationsource AS s ON s.id = l.citation_source_id "
                "JOIN citation_citationsource AS p ON p.id = s.parent_id "
                "WHERE s.source_type IN ('document', 'periodical') "
                "  AND s.slug IS NOT NULL AND p.slug IS NOT NULL"
            )
        ]
        return FlipcommonsMaps(models=models, entities=entities, refs=refs)
    finally:
        con.close()


def enrich(con: sqlite3.Connection, data: FlipcommonsMaps) -> EnrichCounts:
    """Apply the resolution maps to the cache; returns what happened.

    Commits nothing — the caller commits (or rolls back, for a dry run).
    """
    counts = EnrichCounts(
        subjects_resolved=0,
        subjects_unresolved=0,
        refs_filled=0,
        ref_mismatches=[],
    )

    subject_queries = (
        (
            "model",
            "ipdb_machine_id",
            "SELECT document_id, flipcommons_pk, label, "
            "  ipdb_machine_id, ipdb_manufacturer_id FROM document_subjects "
            "WHERE scope = 'model' AND ipdb_machine_id IS NOT NULL",
            data["models"],
        ),
        (
            "corporate_entity",
            "ipdb_manufacturer_id",
            "SELECT document_id, flipcommons_pk, label, "
            "  ipdb_machine_id, ipdb_manufacturer_id FROM document_subjects "
            "WHERE scope = 'corporate_entity' AND ipdb_manufacturer_id IS NOT NULL",
            data["entities"],
        ),
    )
    for scope, id_col, query, mapping in subject_queries:
        rows = con.execute(query).fetchall()
        for row in rows:
            target = mapping.get(row[id_col])
            if target is None:
                counts["subjects_unresolved"] += 1
                continue
            if row["flipcommons_pk"] != target["pk"] or row["label"] != target["name"]:
                counts["subjects_resolved"] += 1
            web_cache.attach_document_subject(
                con,
                row["document_id"],
                scope,
                flipcommons_pk=target["pk"],
                label=target["name"],
                ipdb_machine_id=row["ipdb_machine_id"],
                ipdb_manufacturer_id=row["ipdb_manufacturer_id"],
            )

    for entry in data["refs"]:
        url = web_cache.normalize_url(entry["url"])
        row = con.execute(
            "SELECT d.id, d.citation_ref FROM documents AS d "
            "JOIN document_urls AS u ON u.document_id = d.id WHERE u.url = ?",
            (url,),
        ).fetchone()
        if row is None:
            continue  # Flipcommons cites things the cache doesn't hold; fine
        if row["citation_ref"] is None:
            web_cache.set_document_fields(con, row["id"], citation_ref=entry["ref"])
            counts["refs_filled"] += 1
        elif row["citation_ref"] != entry["ref"]:
            counts["ref_mismatches"].append(
                RefMismatch(
                    document_id=row["id"],
                    stored=row["citation_ref"],
                    resolved=entry["ref"],
                )
            )
    return counts


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Resolve document subjects and citation refs against the "
        "Flipcommons dev DB (re-runnable)."
    )
    parser.add_argument(
        "--flipcommons-db",
        type=Path,
        default=DEFAULT_FLIPCOMMONS_DB,
        help="Flipcommons dev database (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="run the whole enrichment, report, and roll back instead of committing",
    )
    args = parser.parse_args(argv)

    if not args.flipcommons_db.exists():
        print(f"not found: {args.flipcommons_db}", file=sys.stderr)
        return 1
    data = collect(args.flipcommons_db)
    print(
        f"collected: {len(data['models'])} model ids, "
        f"{len(data['entities'])} corporate-entity ids, "
        f"{len(data['refs'])} slug-addressed source links"
    )

    con = web_cache.connect()
    web_cache.init_schema(con)
    try:
        counts = enrich(con, data)
        if args.dry_run:
            con.rollback()
            print("dry run: rolled back")
        else:
            con.commit()
    finally:
        con.close()
    print(f"subjects_resolved: {counts['subjects_resolved']}")
    print(f"subjects_unresolved: {counts['subjects_unresolved']}")
    print(f"refs_filled: {counts['refs_filled']}")
    for mm in counts["ref_mismatches"]:
        print(
            f"ref mismatch on document {mm['document_id']}: stored "
            f"{mm['stored']!r} vs resolved {mm['resolved']!r} — resolve by hand",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

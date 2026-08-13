#!/usr/bin/env python3
"""Seed the IPDB document trove into the web cache's document index.

Reads pinexplore's ``explore.duckdb`` (the classified trove) and writes
through the same registration functions as every other writer. One document
per ``file_url``, never merged here; every listing retained verbatim;
classes recorded as ``ipdb_pattern`` guesses; and one model-scope subject
per distinct machine a listing appears under — the seed asserts only what
IPDB asserts, so corporate-entity subjects are later judgments. The subset
is non-image listings minus ROM sets, plus image listings carrying a class
match. A URL already owned by a captured page is enriched in place.
``publisher`` stays NULL: the filename-derived prefix misreads enough names
that it cannot be asserted as fact.

Idempotent; a re-run (including a widened subset) is safe.

Usage:
    uv run python scripts/web_scrape/web_seed_ipdb.py [--explore-db PATH] [--dry-run]
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import web_cache

if TYPE_CHECKING:
    import sqlite3

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# One qualifying listing admits its URL; all the URL's listings then ride in.
_QUALIFYING_SQL = """
WITH qualifying AS (
  SELECT DISTINCT file_url FROM ipdb_documents
  WHERE (
      ipdb_category NOT IN ('image', 'rom')
      AND NOT list_contains(class_matches, 'rom_set')
    )
    OR (ipdb_category = 'image' AND len(class_matches) > 0)
)
SELECT
  d.ipdb_id, d.machine_name, d.ipdb_category, d.file_name, d.file_url,
  d.container, d.machine_manufacturer, d.machine_mpu, d.class_matches,
  d.machines_referencing, d.titles_referencing, d.systems_referencing,
  m.ManufacturerId AS ipdb_manufacturer_id
FROM ipdb_documents AS d
JOIN qualifying USING (file_url)
LEFT JOIN ipdb_machines AS m ON m.IpdbId = d.ipdb_id
ORDER BY d.file_url, d.ipdb_id, d.ipdb_category
"""


def collect(explore_db: Path) -> dict[str, Any]:
    """Pull the seed's inputs out of DuckDB as plain rows.

    The only function that touches DuckDB, so ``seed()`` stays testable
    offline and the cache never depends on the analytics DB at query time.
    """
    import duckdb

    con = duckdb.connect(str(explore_db), read_only=True)
    try:

        def rows(sql: str) -> list[dict[str, Any]]:
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

        return {
            "vocab": [
                r["document_class"]
                for r in rows("SELECT document_class FROM ref_document_class")
            ],
            "parent_edges": [
                (r["document_class"], r["parent_class"])
                for r in rows("SELECT * FROM ref_document_class_parent")
            ],
            "listings": rows(_QUALIFYING_SQL),
            "patents": rows(
                "SELECT DISTINCT file_url, jurisdiction, patent_number "
                "FROM ipdb_patents WHERE patent_number IS NOT NULL"
            ),
            "articles": rows(
                "SELECT DISTINCT file_url, publication, issue_date, pages "
                "FROM ipdb_trade_articles"
            ),
        }
    finally:
        con.close()


def seed(con: sqlite3.Connection, data: dict[str, Any]) -> dict[str, int]:
    """Write the collected trove into the cache; returns what it did.

    Takes an open writable cache connection with the schema initialized.
    Commits nothing — the caller commits once, so a failed seed rolls back
    whole instead of landing half a trove.
    """
    counts = {
        "vocab": 0,
        "documents_new": 0,
        "documents_enriched": 0,
        "listings": 0,
        "classes": 0,
        "subjects": 0,
    }

    for document_class in data["vocab"]:
        cur = con.execute(
            "INSERT OR IGNORE INTO document_class_vocab VALUES (?)", (document_class,)
        )
        counts["vocab"] += cur.rowcount
    for child, parent in data["parent_edges"]:
        con.execute(
            "INSERT OR IGNORE INTO document_class_parents VALUES (?, ?)",
            (child, parent),
        )

    patents = {p["file_url"]: p for p in data["patents"]}
    articles = {a["file_url"]: a for a in data["articles"]}

    by_url: dict[str, list[dict[str, Any]]] = {}
    for listing in data["listings"]:
        by_url.setdefault(listing["file_url"], []).append(listing)

    for file_url, listings in by_url.items():
        url = web_cache.normalize_url(file_url)
        title = next((li["file_name"] for li in listings if li["file_name"]), None)
        existed = web_cache.resolve_document(con, url) is not None
        doc_id = web_cache.ensure_document_for_url(
            con, url, title=title, role="catalog"
        )
        if existed:
            # The backfill defaulted this URL to `reference`; identifying it
            # as an IPDB catalog copy re-judges the role too.
            con.execute(
                "UPDATE document_urls SET role = 'catalog' WHERE url = ?", (url,)
            )
        counts["documents_enriched" if existed else "documents_new"] += 1

        # Scalar facts fill blanks only, so a re-run (or a backfill-owned
        # document) never has its metadata overwritten by the seed.
        first = listings[0]
        patent = patents.get(file_url, {})
        article = articles.get(file_url, {})
        con.execute(
            "UPDATE documents SET "
            "  ipdb_machines_referencing   = coalesce(ipdb_machines_referencing, ?), "
            "  catalog_titles_referencing  = coalesce(catalog_titles_referencing, ?), "
            "  catalog_systems_referencing = coalesce(catalog_systems_referencing, ?), "
            "  patent_jurisdiction         = coalesce(patent_jurisdiction, ?), "
            "  patent_number               = coalesce(patent_number, ?), "
            "  article_publication         = coalesce(article_publication, ?), "
            "  article_issue_date          = coalesce(article_issue_date, ?), "
            "  article_pages               = coalesce(article_pages, ?) "
            "WHERE id = ?",
            (
                first["machines_referencing"],
                first["titles_referencing"],
                first["systems_referencing"],
                patent.get("jurisdiction"),
                patent.get("patent_number"),
                article.get("publication"),
                article.get("issue_date"),
                article.get("pages"),
                doc_id,
            ),
        )

        classes = {c for li in listings for c in (li["class_matches"] or [])}
        for document_class in sorted(classes):
            if web_cache.add_document_class(
                con, doc_id, document_class, source="ipdb_pattern"
            ):
                counts["classes"] += 1

        for li in listings:
            cur = con.execute(
                "INSERT OR IGNORE INTO document_ipdb_listings "
                "(document_id, ipdb_id, file_url, ipdb_category, ipdb_name, "
                " container, machine_name, machine_manufacturer, "
                " ipdb_manufacturer_id, machine_mpu) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    doc_id,
                    li["ipdb_id"],
                    li["file_url"],  # verbatim dump value, not normalized
                    li["ipdb_category"],
                    li["file_name"],
                    li["container"],
                    li["machine_name"],
                    li["machine_manufacturer"],
                    li["ipdb_manufacturer_id"],
                    li["machine_mpu"],
                ),
            )
            counts["listings"] += cur.rowcount

        seen_machines: set[int] = set()
        for li in listings:
            if li["ipdb_id"] in seen_machines:
                continue
            seen_machines.add(li["ipdb_id"])
            if web_cache.attach_document_subject(
                con,
                doc_id,
                "model",
                ipdb_machine_id=li["ipdb_id"],
                ipdb_machine_name=li["machine_name"],
                ipdb_manufacturer=li["machine_manufacturer"],
            ):
                counts["subjects"] += 1

    return counts


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Seed the IPDB document trove into the web cache "
        "(one-time; idempotent on re-run)."
    )
    parser.add_argument(
        "--explore-db",
        type=Path,
        default=REPO_ROOT / "explore.duckdb",
        help="pinexplore analytics DB to read (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="collect and report what would be seeded; write nothing",
    )
    args = parser.parse_args(argv)

    if not args.explore_db.exists():
        print(f"not found: {args.explore_db} — run `make all` first", file=sys.stderr)
        return 1
    data = collect(args.explore_db)
    urls = {li["file_url"] for li in data["listings"]}
    print(
        f"collected: {len(data['listings'])} listings across {len(urls)} URLs, "
        f"{len(data['vocab'])} classes, {len(data['patents'])} patents, "
        f"{len(data['articles'])} trade articles"
    )
    if args.dry_run:
        return 0

    con = web_cache.connect()
    web_cache.init_schema(con)
    try:
        counts = seed(con, data)
        con.commit()
    finally:
        con.close()
    for key, val in counts.items():
        print(f"{key}: {val}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

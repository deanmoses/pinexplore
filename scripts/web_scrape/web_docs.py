#!/usr/bin/env python3
"""Document metadata CLI: register, classify, attach subjects, hunt, merge.

Every command is a thin wrapper over the shared functions in ``web_cache.py``
— the same write path the fetcher's registration and the seed/enrichment
scripts use — so there is exactly one metadata write path. A ``<doc>``
argument is a document id or any URL the document owns.
"""

from __future__ import annotations

import sqlite3
import sys
from typing import TYPE_CHECKING, Final

import web_cache

if TYPE_CHECKING:
    import argparse


class _CliError(Exception):
    """A command-level failure: printed to stderr, exit 1 — never a traceback."""


def _resolve_or_die(con: sqlite3.Connection, ref: str) -> int:
    doc_id = web_cache.resolve_document(con, ref)
    if doc_id is None:
        raise _CliError(f"no document for {ref!r} (id or an owned URL)")
    return doc_id


# The scalar fields `show` prints, in display order — the merge-hint counts
# are omitted. `Final` is load-bearing: it makes mypy read these as literal
# keys rather than plain `str`, so a name that is not a DocumentRecord field
# is an error where they index it, and each name is written exactly once.
_SHOWN_FIELDS: Final = (
    "id",
    "title",
    "publisher",
    "citation_ref",
    "patent_jurisdiction",
    "patent_number",
    "article_publication",
    "article_issue_date",
    "article_pages",
    "created_at",
    "updated_at",
)


def _print_document(rec: web_cache.DocumentRecord) -> None:
    for field in _SHOWN_FIELDS:
        value = rec[field]
        if value is not None:
            print(f"{field}: {value}")
    for u in rec["urls"]:
        state = "captured" if u["captured"] else "not acquired"
        role = u["role"] or "?"
        print(f"url: {u['url']}  [{role}, {state}]")
    for c in rec["classes"]:
        print(f"class: {c['document_class']}  ({c['source']}, {c['created_at']})")
    for s in rec["subjects"]:
        name = s["label"] or s["ipdb_machine_name"] or s["ipdb_manufacturer"] or "?"
        bits = [s["scope"], name]
        if s["flipcommons_pk"] is not None:
            bits.append(f"pk={s['flipcommons_pk']}")
        if s["ipdb_machine_id"] is not None:
            bits.append(f"ipdb={s['ipdb_machine_id']}")
        if s["ipdb_manufacturer_id"] is not None:
            bits.append(f"ipdb_mfr={s['ipdb_manufacturer_id']}")
        print(f"subject: {'  '.join(bits)}")
    for li in rec["ipdb_listings"]:
        print(
            f"ipdb listing: #{li['ipdb_id']} {li['machine_name'] or '?'} "
            f"[{li['ipdb_category']}] {li['ipdb_name'] or ''}"
        )
    for h in rec["hunts"]:
        note = f" — {h['note']}" if h["note"] else ""
        print(f"hunt: not at {h['tried']} @ {h['created_at']}{note}")


def _cmd_show(con: sqlite3.Connection, args: argparse.Namespace) -> int:
    rec = web_cache.document_record(con, _resolve_or_die(con, args.doc))
    assert rec is not None  # _resolve_or_die verified existence
    _print_document(rec)
    return 0


def _cmd_register(con: sqlite3.Connection, args: argparse.Namespace) -> int:
    url = web_cache.normalize_url(args.url)
    before = web_cache.resolve_document(con, url)
    doc_id = web_cache.ensure_document_for_url(
        con, url, title=args.title, role=args.role
    )
    if before is not None and args.title is not None:
        # The URL was already owned, so ensure_ left title alone; an explicit
        # --title on an existing document is a deliberate set.
        web_cache.set_document_fields(con, doc_id, title=args.title)
    verb = "registered" if before is None else "already registered as"
    print(f"{verb} document {doc_id}: {url}")
    return 0


def _cmd_set(con: sqlite3.Connection, args: argparse.Namespace) -> int:
    doc_id = _resolve_or_die(con, args.doc)
    fields: dict[str, object] = {}
    if args.title is not None:
        fields["title"] = args.title or None  # --title '' clears
    if args.publisher is not None:
        fields["publisher"] = args.publisher or None
    if args.citation_ref is not None:
        fields["citation_ref"] = args.citation_ref or None
    if not fields:
        print(
            "nothing to set (use --title/--publisher/--citation-ref)", file=sys.stderr
        )
        return 1
    web_cache.set_document_fields(con, doc_id, **fields)
    print(f"document {doc_id} updated: {', '.join(fields)}")
    return 0


def _cmd_classify(con: sqlite3.Connection, args: argparse.Namespace) -> int:
    doc_id = _resolve_or_die(con, args.doc)
    if args.remove:
        gone = web_cache.remove_document_class(con, doc_id, args.document_class)
        print(
            f"document {doc_id}: {args.document_class} "
            + ("withdrawn" if gone else "was not recorded")
        )
        return 0
    added = web_cache.add_document_class(
        con, doc_id, args.document_class, source=args.source
    )
    print(
        f"document {doc_id}: {args.document_class} "
        + (f"recorded ({args.source})" if added else "already recorded")
    )
    return 0


def _cmd_subject(con: sqlite3.Connection, args: argparse.Namespace) -> int:
    doc_id = _resolve_or_die(con, args.doc)
    inserted = web_cache.attach_document_subject(
        con,
        doc_id,
        args.scope,
        flipcommons_pk=args.pk,
        label=args.label,
        ipdb_machine_id=args.ipdb_machine_id,
        ipdb_manufacturer_id=args.ipdb_manufacturer_id,
        ipdb_machine_name=args.ipdb_machine_name,
        ipdb_manufacturer=args.ipdb_manufacturer,
    )
    print(
        f"document {doc_id}: {args.scope} subject "
        + ("attached" if inserted else "reconciled into existing row")
    )
    return 0


def _cmd_hunt(con: sqlite3.Connection, args: argparse.Namespace) -> int:
    doc_id = _resolve_or_die(con, args.doc)
    web_cache.record_document_hunt(con, doc_id, args.tried, note=args.note)
    print(f"document {doc_id}: recorded not at {args.tried}")
    return 0


def _cmd_merge(con: sqlite3.Connection, args: argparse.Namespace) -> int:
    survivor = _resolve_or_die(con, args.survivor)
    loser = _resolve_or_die(con, args.loser)
    result = web_cache.merge_documents(con, survivor, loser)
    print(f"document {loser} merged into {survivor}")
    for col, val in result["dropped"].items():
        print(f"dropped (survivor's {col} kept): {val!r}", file=sys.stderr)
    return 0


def _cmd_reindex(con: sqlite3.Connection, args: argparse.Namespace) -> int:
    del args  # uniform command signature; this command takes no arguments
    count = web_cache.rebuild_documents_fts(con)
    print(f"metadata FTS rebuilt: {count} documents indexed")
    return 0


def _cmd_classes(con: sqlite3.Connection, args: argparse.Namespace) -> int:
    del args  # uniform command signature; this command takes no arguments
    rows = con.execute(
        "SELECT v.document_class, count(c.document_id) AS docs "
        "FROM document_class_vocab AS v "
        "LEFT JOIN document_classes AS c USING (document_class) "
        "GROUP BY 1 ORDER BY docs DESC, v.document_class"
    ).fetchall()
    if not rows:
        print("vocabulary is empty (the trove seed loads it)", file=sys.stderr)
        return 1
    width = max(len(r["document_class"]) for r in rows)
    for r in rows:
        print(f"{r['document_class']:<{width}}  {r['docs']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Edit document metadata in the web evidence cache."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_show = sub.add_parser("show", help="one document with all its children")
    p_show.add_argument("doc", help="document id, or any URL the document owns")
    p_show.set_defaults(func=_cmd_show, writes=False)

    p_register = sub.add_parser(
        "register",
        help="ensure a document exists for a URL (captured or not)",
        description="One URL, one document: if the URL is already owned, this "
        "reports the owner instead of minting a duplicate (an explicit "
        "--title still applies).",
    )
    p_register.add_argument("url")
    p_register.add_argument("--title", help="the work's own title")
    p_register.add_argument(
        "--role",
        choices=["reference", "catalog", "archive"],
        default="reference",
        help="what this URL is to the work: its canonical address (reference), "
        "a third-party index holding a copy (catalog), or a preserved "
        "snapshot (archive). Default %(default)s",
    )
    p_register.set_defaults(func=_cmd_register, writes=True)

    p_set = sub.add_parser("set", help="update title / publisher / citation ref")
    p_set.add_argument("doc")
    p_set.add_argument("--title", help="'' clears")
    p_set.add_argument("--publisher", help="'' clears")
    p_set.add_argument(
        "--citation-ref",
        help="the Flipcommons citation source's cite ref "
        "(e.g. williams:some-manual-slug); '' clears",
    )
    p_set.set_defaults(func=_cmd_set, writes=True)

    p_classify = sub.add_parser(
        "classify",
        help="add or withdraw a class judgment",
        description="A judgment with provenance, never a verdict. The class "
        "must exist in the vocabulary — a misspelling fails loudly.",
    )
    p_classify.add_argument("doc")
    p_classify.add_argument("document_class", metavar="class")
    p_classify.add_argument(
        "--source",
        choices=["manual", "ai"],
        default="manual",
        help="who is making this judgment (default %(default)s; the seed "
        "writes ipdb_pattern)",
    )
    p_classify.add_argument(
        "--remove", action="store_true", help="withdraw the judgment instead"
    )
    p_classify.set_defaults(func=_cmd_classify, writes=True)

    p_subject = sub.add_parser(
        "subject",
        help="attach a subject (model or corporate entity)",
        description="Reconciles by identity: re-attaching an existing subject "
        "updates its row instead of duplicating it. A subject carrying only "
        "a Flipcommons PK needs --label — it is the row's only searchable "
        "name.",
    )
    p_subject.add_argument("doc")
    p_subject.add_argument(
        "--scope", choices=["model", "corporate_entity"], required=True
    )
    p_subject.add_argument(
        "--pk", type=int, help="Flipcommons PK (machinemodel or corporateentity)"
    )
    p_subject.add_argument("--label", help="searchable name snapshot")
    p_subject.add_argument(
        "--ipdb-machine-id", type=int, help="IPDB machine id (model scope only)"
    )
    p_subject.add_argument(
        "--ipdb-manufacturer-id",
        type=int,
        help="IPDB manufacturer id (corporate_entity scope only)",
    )
    p_subject.add_argument("--ipdb-machine-name")
    p_subject.add_argument("--ipdb-manufacturer")
    p_subject.set_defaults(func=_cmd_subject, writes=True)

    p_hunt = sub.add_parser(
        "hunt",
        help='record a dated negative: "looked, not there"',
        description="For a URL that IS the document's but couldn't be reached "
        "(403, auth), don't record a hunt — the address belongs in the "
        "document's URLs and the failed fetch is already logged.",
    )
    p_hunt.add_argument("doc")
    p_hunt.add_argument("tried", help="the URL or site searched")
    p_hunt.add_argument("--note", help="what was searched, why concluded absent")
    p_hunt.set_defaults(func=_cmd_hunt, writes=True)

    p_merge = sub.add_parser(
        "merge",
        help="fold one document into another (the loser is deleted)",
        description="URLs, listings and hunts move; classes union; subjects "
        "reconcile. Scalar fields fill the survivor's blanks only — a "
        "conflicting loser value is reported, never silently kept.",
    )
    p_merge.add_argument("survivor")
    p_merge.add_argument("loser")
    p_merge.set_defaults(func=_cmd_merge, writes=True)

    p_classes = sub.add_parser(
        "classes", help="per-class document counts, whole vocabulary"
    )
    p_classes.set_defaults(func=_cmd_classes, writes=False)

    p_reindex = sub.add_parser(
        "reindex",
        help="rebuild the metadata FTS whole (repair; normally never needed)",
        description="The registration library keeps the index current on "
        "every mutation, and init_schema heals count drift on open — this "
        "is the manual override for anything they miss.",
    )
    p_reindex.set_defaults(func=_cmd_reindex, writes=True)

    args = parser.parse_args(argv)
    con = web_cache.connect(read_only=not args.writes)
    if args.writes:
        web_cache.init_schema(con)
    try:
        rc: int = args.func(con, args)
        if args.writes:
            con.commit()
        return rc
    except (_CliError, ValueError) as exc:
        # ValueError is the library refusing an incompatible act (conflicting
        # subject identities, a self-merge) — same treatment as a CLI error.
        print(exc, file=sys.stderr)
        return 1
    except sqlite3.IntegrityError as exc:
        # The schema said no: an unknown class, a scope/id mismatch, a
        # label-less PK-only subject. The constraint text is the explanation.
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())

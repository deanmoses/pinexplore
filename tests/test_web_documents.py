"""Document-library tables: registration, backfill, and constraint behavior.

Covers the work-grain layer added by docs/plans/ManufacturerDocs.md — the
page→document invariant (every ``pages`` row is owned by exactly one
document), the one-URL-one-document rule, and the constraints that keep
judgment tables honest (vocabulary FK on classes, scope CHECKs and partial
unique indexes on subjects). All offline against the tmp SQLite, like every
other test here.
"""

from __future__ import annotations

import sqlite3

import pytest
import web_cache


def _add_page(con: sqlite3.Connection, url: str, title: str | None = None) -> str:
    """Upsert a minimal page row; returns the normalized URL."""
    normalized = web_cache.normalize_url(url)
    web_cache.upsert_page(
        con,
        url=normalized,
        raw_url=url,
        content_sha=web_cache.content_sha(url.encode()),
        fetched_at=web_cache.now_iso(),
        title=title,
        http_status=200,
        content_type="text/html",
        text=f"---\ntitle: {title or ''}\n---\n\nbody of {url}\n",
    )
    return normalized


def _one(con: sqlite3.Connection, sql: str, *params: object) -> object:
    return con.execute(sql, params).fetchone()[0]


def _mint_document(con: sqlite3.Connection) -> int:
    now = web_cache.now_iso()
    cur = con.execute(
        "INSERT INTO documents (created_at, updated_at) VALUES (?, ?)", (now, now)
    )
    assert cur.lastrowid is not None
    return cur.lastrowid


# --------------------------------------------------------------------------- #
# Registration + the page→document invariant
# --------------------------------------------------------------------------- #


def test_upsert_page_registers_a_document_in_the_same_write(cache):
    url = _add_page(cache, "https://example.com/manual", title="Gorgar Manual")
    row = cache.execute(
        "SELECT d.id, d.title, u.role FROM documents AS d "
        "JOIN document_urls AS u ON u.document_id = d.id WHERE u.url = ?",
        (url,),
    ).fetchone()
    assert row is not None
    assert row["title"] == "Gorgar Manual"
    assert row["role"] == "reference"


def test_refetch_neither_duplicates_nor_retitles_the_document(cache):
    url = _add_page(cache, "https://example.com/manual", title="First Title")
    _add_page(cache, "https://example.com/manual", title="Retitled On Refetch")
    assert _one(cache, "SELECT count(*) FROM documents") == 1
    # The document's title is the work's registration-time title; a capture's
    # drifting <title> doesn't rewrite it.
    assert _one(cache, "SELECT title FROM documents") == "First Title"
    assert _one(cache, "SELECT count(*) FROM document_urls WHERE url = ?", url) == 1


def test_ensure_document_returns_the_existing_owner(cache):
    url = _add_page(cache, "https://example.com/manual")
    owner = int(_one(cache, "SELECT document_id FROM document_urls WHERE url = ?", url))
    assert (
        web_cache.ensure_document_for_url(cache, url, title="ignored", role="catalog")
        == owner
    )
    # The second registrar's title/role did not overwrite the first's.
    assert (
        _one(cache, "SELECT role FROM document_urls WHERE url = ?", url) == "reference"
    )


def _upsert_redirected(cache, requested: str, final: str) -> None:
    web_cache.upsert_page(
        cache,
        url=web_cache.normalize_url(final),
        raw_url=requested,
        content_sha=web_cache.content_sha(final.encode()),
        fetched_at=web_cache.now_iso(),
        http_status=200,
        content_type="application/pdf",
    )


def test_a_redirect_attaches_to_the_requested_urls_document(cache):
    # Registered before acquired (the trove case), then the fetch 301s.
    requested = "https://www.ipdb.org/files/1/manual.pdf"
    final = "https://cdn.ipdb.org/1/manual.pdf"
    doc = web_cache.ensure_document_for_url(
        cache, web_cache.normalize_url(requested), title="Manual", role="catalog"
    )
    _upsert_redirected(cache, requested, final)
    assert web_cache.resolve_document(cache, final) == doc  # no second document
    rec = web_cache.document_record(cache, doc)
    by_url = {u["url"]: u for u in rec["urls"]}
    assert by_url[web_cache.normalize_url(final)]["role"] == "catalog"  # inherited
    assert by_url[web_cache.normalize_url(final)]["captured"] is True
    assert by_url[web_cache.normalize_url(requested)]["captured"] is False


def test_a_redirect_between_two_owned_urls_warns_not_merges(cache, capsys):
    requested = "https://a.test/manual.pdf"
    final = "https://b.test/manual.pdf"
    doc_a = web_cache.ensure_document_for_url(
        cache, web_cache.normalize_url(requested), title="A"
    )
    doc_b = web_cache.ensure_document_for_url(
        cache, web_cache.normalize_url(final), title="B"
    )
    _upsert_redirected(cache, requested, final)
    err = capsys.readouterr().err
    assert "web_docs.py merge" in err
    # Nothing was merged: both documents stand, each owning its URL.
    assert web_cache.resolve_document(cache, requested) == doc_a
    assert web_cache.resolve_document(cache, final) == doc_b


def test_backfill_adopts_pages_that_predate_the_tables(cache):
    # Simulate a pre-documents cache: pages exist, ownership rows don't.
    _add_page(cache, "https://example.com/a", title="A")
    _add_page(cache, "https://example.com/b", title="B")
    cache.execute("DELETE FROM document_urls")
    cache.execute("DELETE FROM documents")
    cache.commit()

    web_cache.init_schema(cache)
    assert _one(cache, "SELECT count(*) FROM documents") == 2
    titles = {r[0] for r in cache.execute("SELECT title FROM documents").fetchall()}
    assert titles == {"A", "B"}

    # Idempotent: a second open mints nothing.
    web_cache.init_schema(cache)
    assert _one(cache, "SELECT count(*) FROM documents") == 2


# --------------------------------------------------------------------------- #
# One URL, one document
# --------------------------------------------------------------------------- #


def test_a_url_cannot_belong_to_two_documents(cache):
    url = _add_page(cache, "https://example.com/manual")
    other = _mint_document(cache)
    with pytest.raises(sqlite3.IntegrityError):
        cache.execute(
            "INSERT INTO document_urls (url, document_id, role, created_at) "
            "VALUES (?, ?, 'catalog', ?)",
            (url, other, web_cache.now_iso()),
        )


def test_a_document_may_own_several_urls(cache):
    doc = _mint_document(cache)
    now = web_cache.now_iso()
    for url, role in [
        ("https://ipdb.org/files/1/m.pdf", "catalog"),
        ("https://archive.org/m.pdf", "archive"),
    ]:
        cache.execute(
            "INSERT INTO document_urls (url, document_id, role, created_at) "
            "VALUES (?, ?, ?, ?)",
            (url, doc, role, now),
        )
    assert (
        _one(cache, "SELECT count(*) FROM document_urls WHERE document_id = ?", doc)
        == 2
    )


# --------------------------------------------------------------------------- #
# Constraint behavior: the schema refuses what the design forbids
# --------------------------------------------------------------------------- #


def test_foreign_keys_are_enforced(cache):
    with pytest.raises(sqlite3.IntegrityError):
        cache.execute(
            "INSERT INTO document_urls (url, document_id, created_at) "
            "VALUES ('https://x.test/a', 999999, ?)",
            (web_cache.now_iso(),),
        )


def test_a_class_row_must_name_a_vocabulary_entry(cache):
    doc = _mint_document(cache)
    now = web_cache.now_iso()
    with pytest.raises(sqlite3.IntegrityError):
        cache.execute(
            "INSERT INTO document_classes VALUES (?, 'operations_manuel', 'manual', ?)",
            (doc, now),
        )
    cache.execute("INSERT INTO document_class_vocab VALUES ('operations_manual')")
    cache.execute(
        "INSERT INTO document_classes VALUES (?, 'operations_manual', 'manual', ?)",
        (doc, now),
    )


def test_subject_scope_checks(cache):
    doc = _mint_document(cache)
    now = web_cache.now_iso()

    def insert(scope: str, machine_id=None, manufacturer_id=None, pk=None, label=None):
        cache.execute(
            "INSERT INTO document_subjects "
            "(document_id, scope, flipcommons_pk, label, ipdb_machine_id, "
            " ipdb_manufacturer_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (doc, scope, pk, label, machine_id, manufacturer_id, now),
        )

    insert("model", machine_id=1234)
    insert("corporate_entity", manufacturer_id=56)
    # PK-only (Flipcommons holds models IPDB doesn't): the label is the row's
    # only searchable name, so it is mandatory and non-empty.
    insert("model", pk=42, label="Yukon Yeti")

    with pytest.raises(sqlite3.IntegrityError):  # unknown scope
        insert("manufacturer", manufacturer_id=56)
    with pytest.raises(sqlite3.IntegrityError):  # machine id off model scope
        insert("corporate_entity", machine_id=1234)
    with pytest.raises(sqlite3.IntegrityError):  # manufacturer id off entity scope
        insert("model", manufacturer_id=56)
    with pytest.raises(sqlite3.IntegrityError):  # a subject must be identified
        insert("model")
    with pytest.raises(sqlite3.IntegrityError):  # PK-only without a label
        insert("model", pk=99)
    with pytest.raises(sqlite3.IntegrityError):  # an empty label is no label
        insert("model", pk=99, label="")
    with pytest.raises(sqlite3.IntegrityError):  # whitespace tokenizes to nothing
        insert("model", pk=99, label="   ")


def test_subject_partial_unique_indexes_guard_reruns(cache):
    doc = _mint_document(cache)
    now = web_cache.now_iso()
    cache.execute(
        "INSERT INTO document_subjects (document_id, scope, ipdb_machine_id, created_at) "
        "VALUES (?, 'model', 1234, ?)",
        (doc, now),
    )
    with pytest.raises(sqlite3.IntegrityError):
        cache.execute(
            "INSERT INTO document_subjects (document_id, scope, ipdb_machine_id, created_at) "
            "VALUES (?, 'model', 1234, ?)",
            (doc, now),
        )
    # Distinct PK-only subjects don't collide with each other or the IPDB row.
    for pk, label in ((42, "Yukon Yeti"), (43, "Barrels of Fun")):
        cache.execute(
            "INSERT INTO document_subjects "
            "(document_id, scope, flipcommons_pk, label, created_at) "
            "VALUES (?, 'model', ?, ?, ?)",
            (doc, pk, label, now),
        )
    with pytest.raises(sqlite3.IntegrityError):
        cache.execute(
            "INSERT INTO document_subjects "
            "(document_id, scope, flipcommons_pk, label, created_at) "
            "VALUES (?, 'model', 42, 'Yukon Yeti', ?)",
            (doc, now),
        )

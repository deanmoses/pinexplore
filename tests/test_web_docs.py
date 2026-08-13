"""The document metadata library and its CLI (web_docs.py).

Exercises the write path web_docs.py, the seed and the enrichment scripts all
share: subject attachment with identity reconciliation, class judgments
against the vocabulary, merges that move children and refuse to silently
rewrite metadata, and the id-or-URL document addressing. Offline, like every
test here.
"""

from __future__ import annotations

import pytest
import web_cache
import web_docs


@pytest.fixture
def doc(cache) -> int:
    """A registered document with one URL."""
    return web_cache.ensure_document_for_url(
        cache, "https://example.com/manual", title="Gorgar Manual"
    )


def _subjects(cache, doc_id):
    return [
        dict(r)
        for r in cache.execute(
            "SELECT * FROM document_subjects WHERE document_id = ? ORDER BY rowid",
            (doc_id,),
        ).fetchall()
    ]


# --------------------------------------------------------------------------- #
# Subject attachment: the reconciler
# --------------------------------------------------------------------------- #


def test_attach_reconciles_on_pk_instead_of_duplicating(cache, doc):
    assert web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=42, label="Yukon Yeti"
    )
    assert not web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=42, label="Yukon Yeti (2024)"
    )
    rows = _subjects(cache, doc)
    assert len(rows) == 1
    # label refreshes (it is a snapshot, not provenance)…
    assert rows[0]["label"] == "Yukon Yeti (2024)"


def test_attach_pk_fills_the_ipdb_seeded_row(cache, doc):
    # The seed asserts what IPDB asserts; enrichment later brings the PK.
    web_cache.attach_document_subject(
        cache, doc, "model", ipdb_machine_id=1062, ipdb_machine_name="Gorgar"
    )
    inserted = web_cache.attach_document_subject(
        cache, doc, "model", ipdb_machine_id=1062, flipcommons_pk=7, label="Gorgar"
    )
    assert not inserted
    rows = _subjects(cache, doc)
    assert len(rows) == 1
    assert rows[0]["flipcommons_pk"] == 7
    assert rows[0]["ipdb_machine_name"] == "Gorgar"


def test_attach_rederives_pk_via_ipdb_identity(cache, doc):
    web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=7, ipdb_machine_id=1062, label="Gorgar"
    )
    # Same IPDB identity, different PK: a re-resolution (Flipcommons PKs are
    # re-derivable), so the incoming PK wins — this is what lets enrichment
    # repair PKs after a Flipcommons rebuild.
    web_cache.attach_document_subject(
        cache, doc, "model", ipdb_machine_id=1062, flipcommons_pk=999
    )
    rows = _subjects(cache, doc)
    assert len(rows) == 1
    assert rows[0]["flipcommons_pk"] == 999


def test_attach_refuses_a_conflicting_ipdb_identity(cache, doc):
    web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=7, ipdb_machine_id=1062, label="Gorgar"
    )
    # The IPDB id is senior: the same PK claiming a different machine is an
    # incompatible mapping, resolved by a person rather than absorbed.
    with pytest.raises(ValueError, match="resolve by hand"):
        web_cache.attach_document_subject(
            cache, doc, "model", flipcommons_pk=7, ipdb_machine_id=9999
        )


def test_attach_collapses_split_identity_rows(cache, doc):
    # A PK-only row and an IPDB-only row, never linked — the attachment
    # naming both identities is what asserts they are one subject.
    web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=42, label="Yukon Yeti"
    )
    web_cache.attach_document_subject(
        cache, doc, "model", ipdb_machine_id=1062, ipdb_machine_name="Yukon Yeti"
    )
    assert len(_subjects(cache, doc)) == 2
    inserted = web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=42, ipdb_machine_id=1062
    )
    assert not inserted
    rows = _subjects(cache, doc)
    assert len(rows) == 1
    assert rows[0]["flipcommons_pk"] == 42
    assert rows[0]["ipdb_machine_id"] == 1062
    assert rows[0]["label"] == "Yukon Yeti"
    assert rows[0]["ipdb_machine_name"] == "Yukon Yeti"


def test_attach_refuses_to_collapse_incompatible_rows(cache, doc):
    web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=42, ipdb_machine_id=1111, label="A"
    )
    web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=7, ipdb_machine_id=1062, label="B"
    )
    # An attachment binding one row's PK to the other row's IPDB id claims
    # two contradictory mappings at once.
    with pytest.raises(ValueError, match="resolve by hand"):
        web_cache.attach_document_subject(
            cache, doc, "model", flipcommons_pk=42, ipdb_machine_id=1062
        )
    assert len(_subjects(cache, doc)) == 2  # nothing was destroyed


def test_attach_distinct_subjects_coexist(cache, doc):
    web_cache.attach_document_subject(cache, doc, "model", ipdb_machine_id=1062)
    web_cache.attach_document_subject(cache, doc, "model", ipdb_machine_id=2222)
    web_cache.attach_document_subject(
        cache, doc, "corporate_entity", ipdb_manufacturer_id=56
    )
    assert len(_subjects(cache, doc)) == 3


# --------------------------------------------------------------------------- #
# Classes, fields, hunts
# --------------------------------------------------------------------------- #


def test_class_judgments_add_once_and_withdraw(cache, doc):
    cache.execute("INSERT INTO document_class_vocab VALUES ('manual')")
    assert web_cache.add_document_class(cache, doc, "manual", "manual")
    assert not web_cache.add_document_class(cache, doc, "manual", "ai")
    # The original judgment's provenance survives the re-add attempt.
    assert (
        cache.execute("SELECT source FROM document_classes").fetchone()[0] == "manual"
    )
    assert web_cache.remove_document_class(cache, doc, "manual")
    assert not web_cache.remove_document_class(cache, doc, "manual")


def test_set_fields_distinguishes_unset_from_none(cache, doc):
    web_cache.set_document_fields(cache, doc, publisher="Williams")
    web_cache.set_document_fields(cache, doc, citation_ref="williams:gorgar-manual")
    row = cache.execute("SELECT * FROM documents WHERE id = ?", (doc,)).fetchone()
    assert row["title"] == "Gorgar Manual"  # untouched by either call
    assert row["publisher"] == "Williams"
    web_cache.set_document_fields(cache, doc, publisher=None)  # explicit clear
    row = cache.execute("SELECT * FROM documents WHERE id = ?", (doc,)).fetchone()
    assert row["publisher"] is None
    assert row["citation_ref"] == "williams:gorgar-manual"


def test_metadata_writes_bump_updated_at(cache, doc):
    cache.execute(
        "UPDATE documents SET updated_at = '2000-01-01T00:00:00Z' WHERE id = ?", (doc,)
    )
    web_cache.attach_document_subject(cache, doc, "model", ipdb_machine_id=1)
    stamp = cache.execute(
        "SELECT updated_at FROM documents WHERE id = ?", (doc,)
    ).fetchone()[0]
    assert stamp != "2000-01-01T00:00:00Z"


# --------------------------------------------------------------------------- #
# Merge
# --------------------------------------------------------------------------- #


def test_merge_moves_children_and_deletes_the_loser(cache):
    survivor = web_cache.ensure_document_for_url(
        cache, "https://ipdb.org/files/1/m.pdf", title="Manual", role="catalog"
    )
    loser = web_cache.ensure_document_for_url(
        cache, "https://archive.org/m.pdf", title="Manual", role="archive"
    )
    cache.execute("INSERT INTO document_class_vocab VALUES ('manual')")
    web_cache.add_document_class(cache, loser, "manual", "ai")
    web_cache.attach_document_subject(cache, loser, "model", ipdb_machine_id=1062)
    web_cache.record_document_hunt(cache, loser, "https://planetarypinball.com")

    result = web_cache.merge_documents(cache, survivor, loser)
    assert result["dropped"] == {}

    rec = web_cache.document_record(cache, survivor)
    assert {u["url"] for u in rec["urls"]} == {
        "https://ipdb.org/files/1/m.pdf",
        "https://archive.org/m.pdf",
    }
    assert [c["document_class"] for c in rec["classes"]] == ["manual"]
    assert len(rec["subjects"]) == 1
    assert len(rec["hunts"]) == 1
    assert web_cache.document_record(cache, loser) is None


def test_merge_fills_blanks_and_reports_conflicts(cache):
    survivor = web_cache.ensure_document_for_url(
        cache, "https://a.test/m.pdf", title="Operations Manual"
    )
    loser = web_cache.ensure_document_for_url(
        cache, "https://b.test/m.pdf", title="A Different Title"
    )
    web_cache.set_document_fields(cache, loser, publisher="Williams")

    result = web_cache.merge_documents(cache, survivor, loser)
    row = cache.execute(
        "SELECT title, publisher FROM documents WHERE id = ?", (survivor,)
    ).fetchone()
    assert row["title"] == "Operations Manual"  # survivor's value stood
    assert row["publisher"] == "Williams"  # loser filled the blank
    assert result["dropped"] == {"title": "A Different Title"}


def test_merge_reconciles_shared_subjects_and_classes(cache):
    survivor = web_cache.ensure_document_for_url(cache, "https://a.test/m.pdf")
    loser = web_cache.ensure_document_for_url(cache, "https://b.test/m.pdf")
    cache.execute("INSERT INTO document_class_vocab VALUES ('manual')")
    for d in (survivor, loser):
        web_cache.add_document_class(cache, d, "manual", "ai")
        web_cache.attach_document_subject(cache, d, "model", ipdb_machine_id=1062)

    web_cache.merge_documents(cache, survivor, loser)
    rec = web_cache.document_record(cache, survivor)
    assert len(rec["classes"]) == 1
    assert len(rec["subjects"]) == 1


def test_merge_refuses_self_and_missing(cache, doc):
    with pytest.raises(ValueError, match="itself"):
        web_cache.merge_documents(cache, doc, doc)
    with pytest.raises(ValueError, match="no such document"):
        web_cache.merge_documents(cache, doc, 999999)


# --------------------------------------------------------------------------- #
# Addressing + CLI
# --------------------------------------------------------------------------- #


def test_resolve_document_by_id_and_url(cache, doc):
    assert web_cache.resolve_document(cache, str(doc)) == doc
    # Normalized on the way in: tracking params and casing don't matter.
    assert (
        web_cache.resolve_document(cache, "HTTPS://EXAMPLE.COM/manual?utm_source=x")
        == doc
    )
    assert web_cache.resolve_document(cache, "https://example.com/other") is None
    assert web_cache.resolve_document(cache, "999999") is None


def test_cli_register_classify_subject_show(cache, capsys):
    rc = web_docs.main(
        [
            "register",
            "https://ipdb.org/files/9/x.pdf",
            "--title",
            "X Manual",
            "--role",
            "catalog",
        ]
    )
    assert rc == 0
    cache.execute("INSERT INTO document_class_vocab VALUES ('manual')")
    cache.commit()
    assert web_docs.main(["classify", "https://ipdb.org/files/9/x.pdf", "manual"]) == 0
    assert (
        web_docs.main(
            [
                "subject",
                "https://ipdb.org/files/9/x.pdf",
                "--scope",
                "model",
                "--pk",
                "42",
                "--label",
                "X",
            ]
        )
        == 0
    )
    assert web_docs.main(["show", "https://ipdb.org/files/9/x.pdf"]) == 0
    out = capsys.readouterr().out
    assert "X Manual" in out
    assert "manual" in out
    assert "not acquired" in out


def test_cli_refuses_what_the_schema_refuses(cache, capsys):
    web_docs.main(["register", "https://a.test/m.pdf"])
    rc = web_docs.main(
        ["subject", "https://a.test/m.pdf", "--scope", "model", "--pk", "42"]
    )
    assert rc == 1
    assert "refused" in capsys.readouterr().err


def test_cli_unknown_document_fails_cleanly(cache, capsys):
    rc = web_docs.main(["show", "https://nowhere.test/x"])
    assert rc == 1
    assert "no document" in capsys.readouterr().err

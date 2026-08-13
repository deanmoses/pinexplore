"""The enrichment core (web_enrich_flipcommons.enrich), fed synthetic maps."""

from __future__ import annotations

import web_cache
import web_enrich_flipcommons


def _data(**over):
    base = {"models": {}, "entities": {}, "refs": []}
    base.update(over)
    return base


def _doc_with_subjects(cache) -> int:
    doc = web_cache.ensure_document_for_url(
        cache,
        "https://www.ipdb.org/files/1062/manual.pdf",
        title="Manual",
        role="catalog",
    )
    web_cache.attach_document_subject(
        cache, doc, "model", ipdb_machine_id=1062, ipdb_machine_name="Gorgar"
    )
    web_cache.attach_document_subject(
        cache,
        doc,
        "corporate_entity",
        ipdb_manufacturer_id=350,
        ipdb_manufacturer="Williams",
    )
    return doc


def test_enrich_resolves_both_scopes(cache):
    doc = _doc_with_subjects(cache)
    counts = web_enrich_flipcommons.enrich(
        cache,
        _data(
            models={1062: {"pk": 7, "name": "Gorgar"}},
            entities={350: {"pk": 91, "name": "Williams Electronics, Inc."}},
        ),
    )
    assert counts["subjects_resolved"] == 2
    assert counts["subjects_unresolved"] == 0
    rec = web_cache.document_record(cache, doc)
    by_scope = {s["scope"]: s for s in rec["subjects"]}
    assert by_scope["model"]["flipcommons_pk"] == 7
    assert by_scope["model"]["label"] == "Gorgar"
    assert by_scope["corporate_entity"]["flipcommons_pk"] == 91
    assert by_scope["corporate_entity"]["label"] == "Williams Electronics, Inc."


def test_enrich_repairs_pks_after_a_rebuild(cache):
    doc = _doc_with_subjects(cache)
    web_enrich_flipcommons.enrich(
        cache, _data(models={1062: {"pk": 7, "name": "Gorgar"}})
    )
    # Flipcommons rebuilt: same IPDB id, new PK. Re-derivation must repair.
    counts = web_enrich_flipcommons.enrich(
        cache, _data(models={1062: {"pk": 7007, "name": "Gorgar"}})
    )
    assert counts["subjects_resolved"] == 1
    rec = web_cache.document_record(cache, doc)
    model = next(s for s in rec["subjects"] if s["scope"] == "model")
    assert model["flipcommons_pk"] == 7007


def test_enrich_leaves_unresolved_subjects_standing(cache):
    doc = _doc_with_subjects(cache)
    counts = web_enrich_flipcommons.enrich(cache, _data())
    assert counts["subjects_resolved"] == 0
    assert counts["subjects_unresolved"] == 2
    rec = web_cache.document_record(cache, doc)
    # Still findable by IPDB name; nothing was deleted or nulled.
    assert {
        s["ipdb_machine_name"] or s["ipdb_manufacturer"] for s in rec["subjects"]
    } == {"Gorgar", "Williams"}


def test_enrich_fills_citation_ref_and_reports_mismatch(cache):
    doc = _doc_with_subjects(cache)
    counts = web_enrich_flipcommons.enrich(
        cache,
        _data(
            refs=[
                {
                    "url": "https://www.ipdb.org/files/1062/manual.pdf",
                    "ref": "williams:gorgar-manual",
                }
            ]
        ),
    )
    assert counts["refs_filled"] == 1
    assert web_cache.document_record(cache, doc)["citation_ref"] == (
        "williams:gorgar-manual"
    )
    # A disagreeing resolution is reported, never overwritten: refs are frozen.
    counts = web_enrich_flipcommons.enrich(
        cache,
        _data(
            refs=[
                {
                    "url": "https://www.ipdb.org/files/1062/manual.pdf",
                    "ref": "williams:gorgar-manual-1979",
                }
            ]
        ),
    )
    assert counts["refs_filled"] == 0
    assert counts["ref_mismatches"] == [
        {
            "document_id": doc,
            "stored": "williams:gorgar-manual",
            "resolved": "williams:gorgar-manual-1979",
        }
    ]
    assert web_cache.document_record(cache, doc)["citation_ref"] == (
        "williams:gorgar-manual"
    )


def test_enrich_ignores_refs_for_urls_the_cache_lacks(cache):
    counts = web_enrich_flipcommons.enrich(
        cache, _data(refs=[{"url": "https://elsewhere.test/x.pdf", "ref": "a:b"}])
    )
    assert counts["refs_filled"] == 0
    assert counts["ref_mismatches"] == []


def test_enrich_rerun_changes_and_restamps_nothing(cache):
    doc = _doc_with_subjects(cache)
    data = _data(
        models={1062: {"pk": 7, "name": "Gorgar"}},
        entities={350: {"pk": 91, "name": "Williams"}},
        refs=[
            {
                "url": "https://www.ipdb.org/files/1062/manual.pdf",
                "ref": "williams:gorgar-manual",
            }
        ],
    )
    web_enrich_flipcommons.enrich(cache, data)
    cache.execute("UPDATE documents SET updated_at = '2000-01-01T00:00:00Z'")
    counts = web_enrich_flipcommons.enrich(cache, data)
    assert counts["subjects_resolved"] == 0
    assert counts["refs_filled"] == 0
    stamp = cache.execute(
        "SELECT updated_at FROM documents WHERE id = ?", (doc,)
    ).fetchone()[0]
    assert stamp == "2000-01-01T00:00:00Z"

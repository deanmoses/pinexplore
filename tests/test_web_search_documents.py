"""The metadata search tier: docs_fts maintenance and the search partition.

The third bm25 space beside text and ocr. Covered: the library keeping the
index current through every mutation path (register, classify, subject,
set, merge), init_schema healing drift, the captured/un-acquired partition,
metadata-only matches on held documents, held-hit decoration, blocked and
hunt annotations, and class-name tokenization (operations_manual answers
"manual"). Offline throughout.
"""

from __future__ import annotations

import web_cache


def _register(cache, url, title=None, **kw):
    return web_cache.ensure_document_for_url(cache, url, title=title, **kw)


def _capture(cache, url, text):
    web_cache.upsert_page(
        cache,
        url=url,
        raw_url=url,
        content_sha=web_cache.content_sha(text.encode()),
        fetched_at=web_cache.now_iso(),
        title="A Captured Page",
        http_status=200,
        content_type="text/html",
        text=text,
    )


def _found(cache, term):
    return {d["document_id"] for d in web_cache.search_documents(term, con=cache)}


# --------------------------------------------------------------------------- #
# Index maintenance
# --------------------------------------------------------------------------- #


def test_every_mutation_path_keeps_the_index_current(cache):
    doc = _register(cache, "https://a.test/m.pdf", title="Operations Manual")
    assert doc in _found(cache, "operations")  # register indexed the title

    cache.execute("INSERT INTO document_class_vocab VALUES ('schematic')")
    web_cache.add_document_class(cache, doc, "schematic", "manual")
    assert doc in _found(cache, "schematic")  # classify

    web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=7, label="Yukon Yeti"
    )
    assert doc in _found(cache, "yukon")  # subject label

    web_cache.set_document_fields(cache, doc, title="Renamed Entirely")
    assert doc in _found(cache, "renamed")
    assert doc not in _found(cache, "operations")  # stale text is gone

    web_cache.remove_document_class(cache, doc, "schematic")
    assert doc not in _found(cache, "schematic")


def test_class_names_tokenize_on_underscore(cache):
    doc = _register(cache, "https://a.test/m.pdf")
    cache.execute("INSERT INTO document_class_vocab VALUES ('operations_manual')")
    web_cache.add_document_class(cache, doc, "operations_manual", "manual")
    assert doc in _found(cache, "manual")


def test_merge_removes_the_loser_from_the_index(cache):
    survivor = _register(cache, "https://a.test/m.pdf", title="Manual Alpha")
    loser = _register(cache, "https://b.test/m.pdf", title="Manual Beta")
    web_cache.merge_documents(cache, survivor, loser)
    hits = web_cache.search_documents("beta", con=cache)
    # The loser's row is gone; its title survives only via the survivor's
    # blank-fill (which didn't apply here — the survivor had a title).
    assert {d["document_id"] for d in hits} == set()
    assert survivor in _found(cache, "alpha")


def test_init_schema_heals_index_drift(cache):
    doc = _register(cache, "https://a.test/m.pdf", title="Operations Manual")
    cache.execute("DELETE FROM docs_fts")
    cache.commit()
    assert _found(cache, "operations") == set()
    web_cache.init_schema(cache)
    assert doc in _found(cache, "operations")


# --------------------------------------------------------------------------- #
# The partition
# --------------------------------------------------------------------------- #


def test_captured_flag_partitions_hits(cache):
    held = _register(cache, "https://a.test/held.pdf", title="Gorgar Manual")
    trove = _register(
        cache,
        "https://www.ipdb.org/files/1/x.pdf",
        title="Gorgar Schematic",
        role="catalog",
    )
    _capture(cache, "https://a.test/held.pdf", "the words of the manual")
    by_id = {
        d["document_id"]: d for d in web_cache.search_documents("gorgar", con=cache)
    }
    assert by_id[held]["captured"] is True
    assert by_id[trove]["captured"] is False
    assert by_id[trove]["urls"][0]["role"] == "catalog"


def test_display_title_leads_with_the_subject(cache):
    doc = _register(
        cache, "https://a.test/m.pdf", title="Schematic Diagram (continuous)"
    )
    web_cache.attach_document_subject(
        cache, doc, "model", ipdb_machine_id=1062, ipdb_machine_name="Gorgar"
    )
    hit = web_cache.search_documents("continuous", con=cache)[0]
    assert hit["display_title"] == "Gorgar — Schematic Diagram (continuous)"
    # …but not when the subject is already in the title.
    web_cache.set_document_fields(cache, doc, title="Gorgar Schematic Diagram")
    hit = web_cache.search_documents("schematic", con=cache)[0]
    assert hit["display_title"] == "Gorgar Schematic Diagram"


def test_blocked_and_hunt_annotations_ride_the_hit(cache):
    doc = _register(
        cache,
        "https://www.ipdb.org/files/1/x.pdf",
        title="Blocked Manual",
        role="catalog",
    )
    web_cache.append_fetch(
        cache,
        url="https://www.ipdb.org/files/1/x.pdf",
        fetched_at="2026-08-12T00:00:00Z",
        search_query=None,
        http_status=403,
    )
    web_cache.record_document_hunt(
        cache, doc, "https://archive.org", note="searched, nothing"
    )
    hit = web_cache.search_documents("blocked", con=cache)[0]
    assert hit["urls"][0]["blocked"] == "@ 2026-08-12 (HTTP 403)"
    assert hit["hunts"] == [f"not at https://archive.org @ {web_cache.now_iso()[:10]}"]


def test_a_successful_fetch_clears_the_blocked_annotation(cache):
    _register(cache, "https://a.test/m.pdf", title="Once Blocked")
    web_cache.append_fetch(
        cache,
        url="https://a.test/m.pdf",
        fetched_at="2026-01-01T00:00:00Z",
        search_query=None,
        http_status=403,
    )
    web_cache.append_fetch(
        cache,
        url="https://a.test/m.pdf",
        fetched_at="2026-02-01T00:00:00Z",
        search_query=None,
        http_status=200,
    )
    hit = web_cache.search_documents("once", con=cache)[0]
    assert hit["urls"][0]["blocked"] is None  # only the LATEST fetch judges


# --------------------------------------------------------------------------- #
# Held-hit decoration
# --------------------------------------------------------------------------- #


def test_search_decorates_held_hits_with_classes_and_subjects(cache):
    url = "https://a.test/held.pdf"
    _capture(cache, url, "gorgar's flipper coil specifications")
    doc = web_cache.resolve_document(cache, url)
    cache.execute("INSERT INTO document_class_vocab VALUES ('manual')")
    web_cache.add_document_class(cache, doc, "manual", "manual")
    web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=7, label="Gorgar"
    )
    hit = web_cache.search("coil", con=cache)[0]
    assert hit["document_id"] == doc
    assert hit["classes"] == ["manual"]
    assert hit["subjects"] == ["Gorgar"]


def test_metadata_only_match_on_a_held_document_is_findable(cache):
    # The text never says "Gorgar"; only the subject label does.
    url = "https://a.test/held.pdf"
    _capture(cache, url, "flipper coil specifications, no game name here")
    doc = web_cache.resolve_document(cache, url)
    web_cache.attach_document_subject(
        cache, doc, "model", flipcommons_pk=7, label="Gorgar"
    )
    assert web_cache.search("gorgar", con=cache) == []  # text tiers: nothing
    by_id = {
        d["document_id"]: d for d in web_cache.search_documents("gorgar", con=cache)
    }
    assert by_id[doc]["captured"] is True

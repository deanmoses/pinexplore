"""The IPDB trove seed's core (web_seed_ipdb.seed), fed synthetic rows.

``collect()`` is the only DuckDB-touching function and is deliberately thin;
these tests exercise ``seed()`` offline with hand-built rows shaped like its
output, covering the rules the design doc pins: one document per URL with
duplicate listings collapsing, verbatim listing retention, model-only
subjects, the backfill collision (enrich in place, title stands), kind
fields, and idempotent re-runs.
"""

from __future__ import annotations

import web_cache
import web_seed_ipdb


def _listing(**over):
    base = {
        "ipdb_id": 1062,
        "machine_name": "Gorgar",
        "ipdb_category": "documentation",
        "file_name": "Operations Manual",
        "file_url": "https://www.ipdb.org/files/1062/manual.pdf",
        "container": "pdf",
        "machine_manufacturer": "Williams",
        "machine_mpu": "System 6",
        "class_matches": ["manual", "operations_manual"],
        "machines_referencing": 1,
        "titles_referencing": 1,
        "systems_referencing": 1,
        "ipdb_manufacturer_id": 350,
    }
    base.update(over)
    return base


def _data(**over):
    base = {
        "vocab": ["manual", "operations_manual", "schematic", "patent"],
        "parent_edges": [("operations_manual", "manual")],
        "listings": [],
        "patents": [],
        "articles": [],
    }
    base.update(over)
    return base


def test_duplicate_listings_collapse_into_one_document(cache):
    # The same URL under two categories and, separately, two machines.
    data = _data(
        listings=[
            _listing(ipdb_category="file"),
            _listing(ipdb_category="multimedia"),
            _listing(ipdb_id=2222, machine_name="Other Machine"),
        ]
    )
    counts = web_seed_ipdb.seed(cache, data)
    assert counts["documents_new"] == 1
    assert counts["listings"] == 3
    assert counts["subjects"] == 2  # one per distinct machine, not per listing

    doc_id = web_cache.resolve_document(
        cache, "https://www.ipdb.org/files/1062/manual.pdf"
    )
    rec = web_cache.document_record(cache, doc_id)
    assert rec["title"] == "Operations Manual"
    assert len(rec["ipdb_listings"]) == 3
    assert {s["ipdb_machine_id"] for s in rec["subjects"]} == {1062, 2222}
    assert all(s["scope"] == "model" for s in rec["subjects"])
    assert [c["document_class"] for c in rec["classes"]] == [
        "manual",
        "operations_manual",
    ]
    assert all(c["source"] == "ipdb_pattern" for c in rec["classes"])
    assert rec["urls"][0]["role"] == "catalog"
    # The listing keeps the dump's verbatim URL even though the document key
    # is normalized.
    assert rec["ipdb_listings"][0]["file_url"] == (
        "https://www.ipdb.org/files/1062/manual.pdf"
    )


def test_seed_enriches_a_backfill_owned_document_in_place(cache):
    # The corpus backfill got there first: a captured page owns the URL.
    url = "https://www.ipdb.org/files/1062/manual.pdf"
    existing = web_cache.ensure_document_for_url(
        cache, web_cache.normalize_url(url), title="Captured Page Title"
    )
    counts = web_seed_ipdb.seed(cache, _data(listings=[_listing()]))
    assert counts["documents_new"] == 0
    assert counts["documents_enriched"] == 1

    rec = web_cache.document_record(cache, existing)
    assert rec["title"] == "Captured Page Title"  # the seed didn't retitle
    assert len(rec["ipdb_listings"]) == 1
    assert len(rec["subjects"]) == 1
    assert rec["ipdb_machines_referencing"] == 1
    # The backfill's mechanical `reference` judgment is rejudged: the seed is
    # the act that identifies this URL as an IPDB catalog copy.
    assert rec["urls"][0]["role"] == "catalog"


def test_patent_and_article_fields_land_on_their_documents(cache):
    patent_url = "https://www.ipdb.org/files/9/patent.pdf"
    article_url = "https://www.ipdb.org/files/9/article.pdf"
    data = _data(
        listings=[
            _listing(
                file_url=patent_url,
                file_name="Patent 4,373,731",
                class_matches=["patent"],
            ),
            _listing(
                file_url=article_url, file_name="Coin Slot Article", class_matches=[]
            ),
        ],
        patents=[
            {"file_url": patent_url, "jurisdiction": "US", "patent_number": "4373731"},
        ],
        articles=[
            {
                "file_url": article_url,
                "publication": "Coin Slot",
                "issue_date": "Spring 1992",
                "pages": "21-22",
            },
        ],
    )
    web_seed_ipdb.seed(cache, data)

    patent = web_cache.document_record(
        cache, web_cache.resolve_document(cache, patent_url)
    )
    assert patent["patent_jurisdiction"] == "US"
    assert patent["patent_number"] == "4373731"
    article = web_cache.document_record(
        cache, web_cache.resolve_document(cache, article_url)
    )
    assert article["article_publication"] == "Coin Slot"
    assert article["article_issue_date"] == "Spring 1992"
    assert article["patent_number"] is None


def test_seed_is_idempotent(cache):
    data = _data(listings=[_listing()])
    first = web_seed_ipdb.seed(cache, data)
    # A sentinel, not the real stamp: now_iso() is second-precision, so a
    # same-second rerun could restamp undetectably.
    cache.execute("UPDATE documents SET updated_at = '2000-01-01T00:00:00Z'")
    again = web_seed_ipdb.seed(cache, data)
    assert first["documents_new"] == 1
    assert again["documents_new"] == 0
    assert again["listings"] == 0
    assert again["classes"] == 0
    assert again["subjects"] == 0
    assert cache.execute("SELECT count(*) FROM documents").fetchone()[0] == 1
    assert cache.execute("SELECT count(*) FROM document_subjects").fetchone()[0] == 1
    # Statefully idempotent too: a rerun that changed nothing restamps nothing.
    assert (
        cache.execute("SELECT updated_at FROM documents").fetchone()[0]
        == "2000-01-01T00:00:00Z"
    )


def test_vocabulary_and_edges_load(cache):
    web_seed_ipdb.seed(cache, _data())
    classes = {
        r[0] for r in cache.execute("SELECT * FROM document_class_vocab").fetchall()
    }
    assert "operations_manual" in classes
    edges = cache.execute("SELECT * FROM document_class_parents").fetchall()
    assert [tuple(e) for e in edges] == [("operations_manual", "manual")]

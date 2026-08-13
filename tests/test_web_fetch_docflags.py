"""web_fetch's --doc-class / --subject sugar over the registration library."""

from __future__ import annotations

from argparse import Namespace

import web_cache
import web_fetch


def _args(**over):
    base = {
        "doc_class": [],
        "subject_pk": None,
        "subject_label": None,
        "subject_scope": "model",
    }
    base.update(over)
    return Namespace(**base)


def _page(cache, url):
    web_cache.upsert_page(
        cache,
        url=url,
        raw_url=url,
        content_sha=web_cache.content_sha(url.encode()),
        fetched_at=web_cache.now_iso(),
        http_status=200,
        content_type="text/html",
        text="body",
    )


def test_flags_apply_through_the_shared_library(cache):
    url = "https://a.test/manual.pdf"
    _page(cache, url)
    cache.execute("INSERT INTO document_class_vocab VALUES ('manual')")
    web_fetch._apply_document_metadata(
        cache,
        url,
        _args(doc_class=["manual"], subject_pk=7, subject_label="Gorgar"),
    )
    doc = web_cache.resolve_document(cache, url)
    rec = web_cache.document_record(cache, doc)
    assert [c["document_class"] for c in rec["classes"]] == ["manual"]
    assert [c["source"] for c in rec["classes"]] == ["manual"]
    assert rec["subjects"][0]["label"] == "Gorgar"
    assert rec["subjects"][0]["flipcommons_pk"] == 7


def test_an_unknown_class_warns_without_killing_the_batch(cache, capsys):
    url = "https://a.test/manual.pdf"
    _page(cache, url)
    web_fetch._apply_document_metadata(cache, url, _args(doc_class=["manuel"]))
    assert "refused class 'manuel'" in capsys.readouterr().err
    doc = web_cache.resolve_document(cache, url)
    assert web_cache.document_record(cache, doc)["classes"] == []


def test_no_flags_is_a_no_op(cache):
    url = "https://a.test/manual.pdf"
    _page(cache, url)
    web_fetch._apply_document_metadata(cache, url, _args())
    doc = web_cache.resolve_document(cache, url)
    rec = web_cache.document_record(cache, doc)
    assert rec["classes"] == []
    assert rec["subjects"] == []

"""web_fetch's --doc-class / --subject sugar over the registration library."""

from __future__ import annotations

from argparse import Namespace

import pytest
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


def test_an_uncached_url_is_registered_and_annotated(cache, capsys):
    # Nothing was captured (unsupported type, 404, dead host), so the judgment
    # lands on a document the library holds as not acquired.
    url = "https://a.test/flyer.webp"
    cache.execute("INSERT INTO document_class_vocab VALUES ('flyer')")
    web_fetch._apply_document_metadata(cache, url, _args(doc_class=["flyer"]))
    assert "not acquired" in capsys.readouterr().out
    doc = web_cache.resolve_document(cache, url)
    rec = web_cache.document_record(cache, doc)
    assert [c["document_class"] for c in rec["classes"]] == ["flyer"]
    assert [(u["url"], u["captured"]) for u in rec["urls"]] == [(url, 0)]


def test_a_video_is_annotated_under_its_canonical_watch_url(cache):
    # A video with no captions leaves no page row, so this is the registering
    # path — and it must land on the key the fetch path uses, or the same video
    # cited two ways becomes two documents.
    watch = "https://www.youtube.com/watch?v=abc123"
    _page(cache, watch)
    cache.execute("INSERT INTO document_class_vocab VALUES ('review')")
    web_fetch._apply_document_metadata(
        cache, "https://youtu.be/abc123", _args(doc_class=["review"])
    )
    assert cache.execute("SELECT count(*) FROM documents").fetchone()[0] == 1
    rec = web_cache.document_record(cache, web_cache.resolve_document(cache, watch))
    assert [c["document_class"] for c in rec["classes"]] == ["review"]


def test_an_alias_annotates_the_document_that_already_owns_it(cache):
    # The canonical collapse must not outrun ownership: a document registered
    # under the alias before anything was captured still owns the video.
    alias = "https://youtu.be/abc123"
    owner = web_cache.ensure_document_for_url(cache, web_cache.normalize_url(alias))
    cache.execute("INSERT INTO document_class_vocab VALUES ('review')")
    web_fetch._apply_document_metadata(cache, alias, _args(doc_class=["review"]))
    assert cache.execute("SELECT count(*) FROM documents").fetchone()[0] == 1
    rec = web_cache.document_record(cache, owner)
    assert [c["document_class"] for c in rec["classes"]] == ["review"]


@pytest.mark.parametrize(
    "bad",
    [
        "https://a.test:99999/x",  # normalize_url itself rejects the port
        "ftp://a.test/x",  # not a web scheme
        "https://not a url/",  # a host http.client refuses at connect time
        "https://not%20a%20url/",  # the same, wearing an escape urllib decodes
        "https://us%20er@a.test/x",  # unusable userinfo, which travels with it
    ],
)
def test_a_non_web_url_warns_rather_than_registering(cache, capsys, bad):
    # The fetcher refuses each of these, so nothing can ever be captured for
    # them, and a document minted here would be permanent — merge is the only
    # path that deletes one.
    web_fetch._apply_document_metadata(cache, bad, _args(doc_class=["flyer"]))
    assert "cannot annotate" in capsys.readouterr().err
    assert cache.execute("SELECT count(*) FROM documents").fetchone()[0] == 0


def test_no_flags_is_a_no_op(cache):
    url = "https://a.test/manual.pdf"
    _page(cache, url)
    web_fetch._apply_document_metadata(cache, url, _args())
    doc = web_cache.resolve_document(cache, url)
    rec = web_cache.document_record(cache, doc)
    assert rec["classes"] == []
    assert rec["subjects"] == []

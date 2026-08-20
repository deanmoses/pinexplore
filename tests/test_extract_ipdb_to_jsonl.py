"""Tests for extract_ipdb_to_jsonl — cached IPDB pages to one JSONL file.

Offline: a tmp cache seeded with hand-built machine pages (the same fixture
style as test_parse_ipdb, since the parser is exercised there), read back
through the real cache API.

What's covered is what the file promises its reader — a DuckDB session in
flippatch that types the corpus by reading it. A row's shape must not depend
on which model it describes, a label-keyed map must not reach JSON as an
object, and a value IPDB stated at month precision must not arrive padded to a
day. Plus the corpus's two normal faults: an archive capture taken before a
machine existed, and one holding the wrong machine's page.
"""

from __future__ import annotations

import json

import extract_ipdb_to_jsonl as extract_ipdb
import web_cache as wc


def header(
    ipdb_id: int = 2006, name: str = "Rollergames", tail: str = "/ 4 Players"
) -> str:
    """The header row: IPD No., name, and the italic date/players line."""
    return (
        '<tr><td valign=top colspan=5><table border=0 width="100%"><tr><td>'
        f'<font size="+1"><B><a name="{ipdb_id}">{name}</a></B> / IPD No. '
        f'<a class="linkid" href="machine.cgi?id={ipdb_id}">{ipdb_id}</a> '
        f"<I>{tail}</I></font><br>&nbsp;</td></tr></table></td></tr>"
    )


def page(*rows: str, ipdb_id: int = 2006, tail: str = "/ 4 Players") -> bytes:
    """A machine page: IPDB's outer table, its header row, then ``rows``."""
    return (
        "<html><body>"
        '<table border=0 align=center width="80%" cellpadding=1 cellspacing=0>'
        f"{header(ipdb_id=ipdb_id, tail=tail)}{''.join(rows)}"
        "</table></body></html>"
    ).encode()


def row(label: str, value: str) -> str:
    return (
        f'<tr><td nowrap width="20%" valign=baseline align=right><b>{label} </b></td>'
        f"<td colspan=4 align=left valign=baseline>{value}</td></tr>"
    )


def file_row(label: str, size: str, kind: str, href: str, name: str) -> str:
    return (
        f'<tr valign=top><td width="20%" align="right"><b>{label} </b></td>'
        f"<td align=right nowrap> {size}</td>"
        f'<td align=left nowrap width="5%">&nbsp;{kind}&nbsp;</td>'
        f'<td align=left><a href="{href}">{name}</a></td>'
        "<td> [Williams Electronic Games]</td></tr>"
    )


def seed(
    cache,
    body: bytes,
    *,
    ipdb_id: int = 2006,
    raw_url: str | None = None,
    write_blob: bool = True,
) -> str:
    """Store ``body`` as the cached page for ``machine.cgi?id=<ipdb_id>``."""
    url = wc.normalize_url(f"https://www.ipdb.org/machine.cgi?id={ipdb_id}")
    sha = wc.content_sha(body)
    if write_blob:
        path = wc.blob_path(sha, "html")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    wc.upsert_page(
        cache,
        url=url,
        raw_url=raw_url or url,
        content_sha=sha,
        fetched_at=wc.now_iso(),
        http_status=200,
        content_type="text/html",
        text="extracted text",
        text_source="html",
    )
    return url


def only(cache) -> dict:
    """Extract, asserting exactly one model came back."""
    report = extract_ipdb.extract(con=cache)
    assert len(report.rows) == 1
    return report.rows[0]


# --------------------------------------------------------------------------- #
# The shape the reader types the corpus by
# --------------------------------------------------------------------------- #


def test_a_sparse_model_and_a_rich_one_have_identical_keys(cache):
    """Inference over JSONL types a column by the rows it read, so a key that
    appears only on rich models is a column that vanishes on a sparse corpus."""
    seed(cache, page(), ipdb_id=2006)
    seed(
        cache,
        page(
            row("Date Of Manufacture:", "April 04, 1990"),
            row(
                "Design by:", '<a href="search.pl?ppl=Steve+Ritchie">Steve Ritchie</a>'
            ),
            row("Theme:", "Sports - Skating"),
            row("Production:", "1,200 units (confirmed)"),
            ipdb_id=2007,
        ),
        ipdb_id=2007,
    )
    sparse, rich = extract_ipdb.extract(con=cache).rows
    assert list(sparse) == list(rich)
    # Absence is a null or an empty list, never a missing key.
    assert sparse["manufacture_date"] is None
    assert sparse["credits"] == []
    assert sparse["themes"] == []


def test_credits_carry_their_role_as_a_value(cache):
    """As a JSON object keyed by role, every model would infer its own struct
    type and the union of them would be unreadable."""
    seed(
        cache,
        page(
            row(
                "Concept by:",
                '<a href="search.pl?ppl=Python+Anghelo">Python Anghelo</a>',
            ),
            row(
                "Design by:",
                '<a href="search.pl?ppl=Steve+Ritchie">Steve Ritchie</a>, '
                '<a href="search.pl?ppl=John+Trudeau">John Trudeau</a>',
            ),
        ),
    )
    credits = only(cache)["credits"]
    assert [(c["role"], c["name"]) for c in credits] == [
        ("Concept by", "Python Anghelo"),
        ("Design by", "Steve Ritchie"),
        ("Design by", "John Trudeau"),
    ]
    assert all(c["url"] is not None for c in credits)


def test_documents_carry_their_listing_as_a_value(cache):
    seed(
        cache,
        page(
            file_row("Documentation:", "5 MB", "PDF", "files/2006/manual.pdf", "Manual")
        ),
    )
    assert only(cache)["documents"] == [
        {
            "section": "Documentation",
            "name": "Manual",
            "url": "https://www.ipdb.org/files/2006/manual.pdf",
            "kind": "PDF",
            "size": "5 MB",
            "credit": "Williams Electronic Games",
        }
    ]


def test_a_month_precision_date_is_not_padded_to_a_day(cache):
    seed(cache, page(row("Date Of Manufacture:", "May, 1989")))
    model = only(cache)
    assert model["manufacture_date"]["iso"] == "1989-05"
    assert model["manufacture_date"]["day"] is None
    assert model["manufacture_date"]["precision"] == "month"


def test_date_names_the_row_it_came_from(cache):
    """A production date and a project date mean different things, and the
    header line alone says which it printed for neither."""
    seed(cache, page(row("Project Date:", "1979"), ipdb_id=1), ipdb_id=1)
    seed(cache, page(row("Date Of Manufacture:", "1979"), ipdb_id=2), ipdb_id=2)
    seed(cache, page(ipdb_id=3, tail="/ May, 1989 / 4 Players"), ipdb_id=3)
    seed(cache, page(ipdb_id=4), ipdb_id=4)
    rows = extract_ipdb.extract(con=cache).rows
    assert [r["date_source"] for r in rows] == [
        "project",
        "manufacture",
        "header",
        None,
    ]
    assert rows[0]["date"]["year"] == 1979
    assert rows[3]["date"] is None


# --------------------------------------------------------------------------- #
# The backstop
# --------------------------------------------------------------------------- #


def test_only_unmodeled_labels_are_carried_verbatim(cache):
    """The parser's full field map is dropped as a restatement of what is
    already typed — except under a label it has no case for, where the verbatim
    text is the only record that IPDB said anything at all."""
    seed(
        cache,
        page(
            row("Notes:", "A note."),
            row(
                "Cabinet Colors:",
                'Red<br>Blue (<a href="machine.cgi?id=940">as 4 Square</a>)',
            ),
        ),
    )
    assert only(cache)["unknown_fields"] == [
        {
            "label": "Cabinet Colors",
            "text": "Red Blue (as 4 Square)",
            "lines": ["Red", "Blue (as 4 Square)"],
            "links": [
                {
                    "text": "as 4 Square",
                    "url": "https://www.ipdb.org/machine.cgi?id=940",
                }
            ],
        }
    ]


def test_a_page_of_only_modeled_labels_carries_nothing_verbatim(cache):
    seed(cache, page(row("Notes:", "A note."), row("Source:", "pictures")))
    model = only(cache)
    assert model["unknown_fields"] == []
    assert model["notes"] == "A note."
    assert model["source"] == "pictures"


# --------------------------------------------------------------------------- #
# Provenance
# --------------------------------------------------------------------------- #


def test_an_archived_page_carries_the_date_its_words_were_true(cache):
    seed(
        cache,
        page(),
        raw_url=(
            "https://web.archive.org/web/20260223140052id_/"
            "https://www.ipdb.org/machine.cgi?id=2006"
        ),
    )
    model = only(cache)
    assert model["source_url"] == "https://www.ipdb.org/machine.cgi?id=2006"
    assert model["archive_capture_date"] == "2026-02-23"
    assert model["content_sha"]


def test_a_live_page_has_no_capture_date(cache):
    seed(cache, page())
    assert only(cache)["archive_capture_date"] is None


# --------------------------------------------------------------------------- #
# The corpus's normal faults
# --------------------------------------------------------------------------- #


def test_a_capture_predating_the_machine_is_skipped_not_failed(cache):
    """IPDB answers an id it doesn't hold with its front page, and archive.org
    captured those too — a normal member of the corpus, not a fault."""
    seed(cache, b"<html><body><h1>The Internet Pinball Database</h1></body></html>")
    seed(cache, page(ipdb_id=2007), ipdb_id=2007)
    report = extract_ipdb.extract(con=cache)
    assert [r["ipdb_id"] for r in report.rows] == [2007]
    assert report.not_a_model == ["https://www.ipdb.org/machine.cgi?id=2006"]
    assert report.unreadable == []


def test_a_capture_of_a_different_machine_is_dropped(cache):
    """Writing the parse anyway would file one model's facts under another."""
    seed(cache, page(), ipdb_id=999)
    report = extract_ipdb.extract(con=cache)
    assert report.rows == []
    assert report.id_mismatch == [("https://www.ipdb.org/machine.cgi?id=999", 2006)]


def test_a_row_whose_blob_is_gone_is_reported(cache):
    seed(cache, page(), write_blob=False)
    report = extract_ipdb.extract(con=cache)
    assert report.rows == []
    assert report.no_blob == ["https://www.ipdb.org/machine.cgi?id=2006"]


def test_unreadable_bytes_are_reported_apart_from_a_front_page(cache):
    seed(cache, b"")
    report = extract_ipdb.extract(con=cache)
    assert report.not_a_model == []
    assert len(report.unreadable) == 1


# --------------------------------------------------------------------------- #
# The file
# --------------------------------------------------------------------------- #


def test_the_file_is_one_object_per_line_sorted_by_id(cache, tmp_path):
    for ipdb_id in (2007, 3, 41):
        seed(cache, page(ipdb_id=ipdb_id), ipdb_id=ipdb_id)
    out = tmp_path / "out" / "models.jsonl"
    extract_ipdb.write(extract_ipdb.extract(con=cache).rows, out)
    lines = out.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["ipdb_id"] for line in lines] == [3, 41, 2007]


def test_writing_twice_replaces_rather_than_appends(cache, tmp_path):
    seed(cache, page())
    out = tmp_path / "models.jsonl"
    rows = extract_ipdb.extract(con=cache).rows
    extract_ipdb.write(rows, out)
    extract_ipdb.write(rows, out)
    assert len(out.read_text(encoding="utf-8").splitlines()) == 1
    assert not list(out.parent.glob("*.tmp"))

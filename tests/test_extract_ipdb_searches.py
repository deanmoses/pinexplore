"""Tests for extract_ipdb_searches_to_jsonl — saved searches to two files.

The extract's contract is that dropping a page into a folder is the whole act of
adding a download: no code change, no SQL change. So what is covered here is the
walk -- which folders are read, how a search gets its name, and what each row
records about where it came from -- rather than the table parsing, which is
ipdb_search's and is tested against both markups there.

The other thing covered is what the extract deliberately does NOT do. It records
what each page said and interprets none of it: two pages describing one model
differently are two rows, not a merge or an error, because that question has an
answer and SQL is where it is asked.
"""

from __future__ import annotations

import extract_ipdb_searches_to_jsonl as extract
import ipdb_search
import pytest

from .ipdb_pages import SPECIALTIES, SPECIALTY_ORDER, TYPE_ORDER, page, specialty_cell


def save(root, folder: str, name: str, body: str) -> None:
    directory = root / folder
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(body, encoding=ipdb_search.ENCODING)


def test_a_search_is_named_by_where_it_was_saved(tmp_path):
    """The folder is the kind, the file is the instance."""
    save(
        tmp_path,
        "years",
        "ipdb_1970_1980.html",
        page(SPECIALTY_ORDER, selected="Widebody"),
    )
    (result,), _ = extract.build(tmp_path)

    assert result["search_kind"] == "years"
    assert result["search_name"] == "ipdb_1970_1980"
    assert result["ipdb_id"] == 936


def test_a_page_records_the_specialty_it_searched_for(tmp_path):
    """`search_filter` is what lets SQL ask whether the 27 searches are complete."""
    save(
        tmp_path,
        "specialties",
        "zipper.html",
        page(SPECIALTY_ORDER, selected="Zipper Flippers"),
    )
    (result,), _ = extract.build(tmp_path)

    assert result["search_filter"] == "Zipper Flippers"


def test_a_page_that_states_no_filter_records_none(tmp_path):
    """IPDB's Type search echoes no form, and that absence must not be invented."""
    save(
        tmp_path,
        "specialties",
        "zipper.html",
        page(SPECIALTY_ORDER, selected="Zipper Flippers"),
    )
    save(
        tmp_path,
        "technology_generations",
        "pm.htm",
        page(TYPE_ORDER, browser_saved=False, ipdb_id=59, with_form=False),
    )
    results, _ = extract.build(tmp_path)

    filters = {row["search_kind"]: row["search_filter"] for row in results}
    assert filters == {"specialties": "Zipper Flippers", "technology_generations": None}


@pytest.mark.parametrize("suffix", [".htm", ".html"])
def test_both_page_suffixes_are_read(tmp_path, suffix):
    """Saving by hand produces either, and neither says anything about content."""
    save(tmp_path, "years", f"y{suffix}", page(SPECIALTY_ORDER, selected="Widebody"))
    results, _ = extract.build(tmp_path)

    assert len(results) == 1


def test_the_specialty_list_comes_from_the_form_not_the_rows(tmp_path):
    """A Specialty no model carries is in the dropdown and nowhere else."""
    save(
        tmp_path,
        "specialties",
        "zipper.html",
        page(SPECIALTY_ORDER, selected="Zipper Flippers"),
    )
    _, specialties = extract.build(tmp_path)

    assert {s["specialty"] for s in specialties} == set(SPECIALTIES)
    downloaded = {s["specialty"]: s["downloaded"] for s in specialties}
    assert downloaded["Zipper Flippers"] is True
    assert downloaded["Widebody"] is False


def test_the_specialty_list_carries_ipdbs_own_id_and_url(tmp_path):
    save(
        tmp_path,
        "specialties",
        "zipper.html",
        page(SPECIALTY_ORDER, selected="Zipper Flippers"),
    )
    _, specialties = extract.build(tmp_path)

    (zipper,) = [s for s in specialties if s["specialty"] == "Zipper Flippers"]
    assert zipper["specialty_id"] == 25
    assert zipper["source_url"] == (
        "https://www.ipdb.org/search.pl?specialty=25&sortby=name&searchtype=advanced"
    )


def test_a_model_matched_by_two_searches_is_two_rows(tmp_path):
    """Not merged here: whether the copies agree is a question for the build."""
    save(tmp_path, "years", "y.html", page(SPECIALTY_ORDER, selected="Widebody"))
    save(
        tmp_path,
        "specialties",
        "z.html",
        page(SPECIALTY_ORDER, selected="Zipper Flippers"),
    )
    results, _ = extract.build(tmp_path)

    assert len(results) == 2
    assert {row["ipdb_id"] for row in results} == {936}


def test_every_row_lists_the_models_whole_specialty_set(tmp_path):
    """Which is why a year page witnesses Specialties as well as a Specialty page."""
    cell = specialty_cell("Bingo Machine", "Zipper Flippers", elide=True)
    save(tmp_path, "years", "y.html", page(SPECIALTY_ORDER, specialties=cell))
    (result,), _ = extract.build(tmp_path)

    assert result["specialties"] == ["Bingo Machine", "Zipper Flippers"]


def test_a_model_listed_twice_on_one_page_is_refused(tmp_path):
    """One search cannot match a model twice; if it does, the parse is wrong."""
    save(
        tmp_path, "years", "y.html", page(SPECIALTY_ORDER, rows=2, selected="Widebody")
    )
    with pytest.raises(ipdb_search.ParseError, match="listed twice"):
        extract.build(tmp_path)


def test_two_pages_searching_one_specialty_are_refused(tmp_path):
    """The same search saved twice would double every row it contributes."""
    save(tmp_path, "specialties", "a.html", page(SPECIALTY_ORDER, selected="Widebody"))
    save(tmp_path, "specialties", "b.html", page(SPECIALTY_ORDER, selected="Widebody"))
    with pytest.raises(ipdb_search.ParseError, match="searches for 'Widebody'"):
        extract.build(tmp_path)


def test_pages_with_different_specialty_lists_are_refused(tmp_path):
    """The list moved mid-download, so the two halves describe different things."""
    save(tmp_path, "specialties", "a.html", page(SPECIALTY_ORDER, selected="Widebody"))
    save(
        tmp_path,
        "specialties",
        "b.html",
        page(SPECIALTY_ORDER, selected="Zipper Flippers").replace(
            '<option value="0">Any Specialty&nbsp;</option>',
            '<option value="0">Any Specialty&nbsp;</option><option value="30">Pitch And Bat\n</option>',
        ),
    )
    with pytest.raises(
        ipdb_search.ParseError, match="spans a change to IPDB's Specialty list"
    ):
        extract.build(tmp_path)


def test_a_corpus_with_no_specialty_form_anywhere_is_refused(tmp_path):
    """Without one echoed form there is no Specialty list, and no drift check."""
    save(tmp_path, "years", "y.html", page(SPECIALTY_ORDER, with_form=False))
    with pytest.raises(ipdb_search.ParseError, match="no saved page echoes"):
        extract.build(tmp_path)


def test_an_empty_source_directory_is_refused(tmp_path):
    with pytest.raises(ipdb_search.ParseError, match="no saved search pages"):
        extract.build(tmp_path)

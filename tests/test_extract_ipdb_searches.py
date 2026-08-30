"""Tests for extract_ipdb_searches_to_jsonl — saved searches to one corpus.

The extract's contract is that dropping a page into a folder is the whole act of
adding a download: no code change, no SQL change. So what is covered here is the
walk -- which folders are read, which is deliberately not, and how a search gets
its name -- rather than the table parsing, which is ipdb_search's and is tested
against both markups there.
"""

from __future__ import annotations

import extract_ipdb_searches_to_jsonl as extract
import ipdb_search
import pytest

from .ipdb_pages import SPECIALTY_ORDER, page


def save(root, folder: str, name: str, body: str) -> None:
    directory = root / folder
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(body, encoding=ipdb_search.ENCODING)


def test_a_search_is_named_by_where_it_was_saved(tmp_path):
    """The folder is the kind, the file is the instance -- no filter is read back."""
    save(
        tmp_path,
        "ipdb_years",
        "ipdb_1970_1980.html",
        page(SPECIALTY_ORDER, browser_saved=True),
    )
    (observation,) = extract.build(tmp_path)

    assert observation["search_kind"] == "years"
    assert observation["search_name"] == "ipdb_1970_1980"
    assert observation["ipdb_id"] == 936


@pytest.mark.parametrize("suffix", [".htm", ".html"])
def test_both_page_suffixes_are_read(tmp_path, suffix):
    """Saving by hand produces either, and neither is a statement about content."""
    save(
        tmp_path, "ipdb_years", f"y{suffix}", page(SPECIALTY_ORDER, browser_saved=True)
    )

    assert len(extract.build(tmp_path)) == 1


def test_the_specialty_folder_is_left_to_its_own_extract(tmp_path):
    """Folding it in would duplicate every row and lose the completeness proof."""
    save(
        tmp_path,
        "ipdb_specialty",
        "zipper.html",
        page(SPECIALTY_ORDER, browser_saved=True),
    )
    save(tmp_path, "ipdb_years", "y.html", page(SPECIALTY_ORDER, browser_saved=True))
    observations = extract.build(tmp_path)

    assert [row["search_kind"] for row in observations] == ["years"]


def test_a_machine_matched_by_two_searches_is_observed_twice(tmp_path):
    """Not deduplicated here: whether the copies agree is a question for the build."""
    save(tmp_path, "ipdb_years", "y.html", page(SPECIALTY_ORDER, browser_saved=True))
    save(
        tmp_path,
        "ipdb_technology_generations",
        "pm.htm",
        page(SPECIALTY_ORDER, browser_saved=False),
    )
    observations = extract.build(tmp_path)

    assert len(observations) == 2
    assert {row["ipdb_id"] for row in observations} == {936}
    assert {row["search_kind"] for row in observations} == {
        "years",
        "technology_generations",
    }


def test_a_machine_listed_twice_on_one_page_is_refused(tmp_path):
    """One search cannot match a machine twice; if it does, the parse is wrong."""
    save(
        tmp_path,
        "ipdb_years",
        "y.html",
        page(SPECIALTY_ORDER, browser_saved=True, rows=2),
    )
    with pytest.raises(ipdb_search.ParseError, match="listed twice"):
        extract.build(tmp_path)


def test_folders_outside_the_naming_convention_are_ignored(tmp_path):
    """The dir holds the dump and the archive extract too; only searches are read."""
    save(tmp_path, "notes", "scratch.html", page(SPECIALTY_ORDER, browser_saved=True))
    with pytest.raises(ipdb_search.ParseError, match="no saved search pages"):
        extract.build(tmp_path)

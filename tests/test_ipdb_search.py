"""Tests for ipdb_search — the advanced-search results table, in both markups.

The table is the same whatever filter produced it, but the SAVED PAGES are not.
A page kept as the server sent it has IPDB's HTML 4.01: bare attributes, relative
hrefs, no `tbody`. A page saved out of a browser has the DOM the browser built:
everything quoted, hrefs absolutised, a `tbody` inserted. Both are downloaded by
hand, so both arrive, and the parser may not prefer either.

The other thing these cover is column ORDER. IPDB ships more than one: the
Specialty searches put `Prod.` after `Specialty`, the Type search puts it before.
Reading by position would not have failed on that -- it would have silently read
production out of the specialty column -- so the header row is what locates every
field, and these tests are what hold that to it.
"""

from __future__ import annotations

import re

import ipdb_search as search
import pytest

from .ipdb_pages import SPECIALTIES, SPECIALTY_ORDER, TYPE_ORDER, page


@pytest.mark.parametrize(
    "browser_saved", [True, False], ids=["browser-saved", "server-html"]
)
@pytest.mark.parametrize(
    "order", [SPECIALTY_ORDER, TYPE_ORDER], ids=["specialty-order", "type-order"]
)
def test_every_field_lands_in_both_markups_and_both_orders(order, browser_saved):
    """The cross product: neither the markup nor the column order may matter."""
    (model,) = search.parse_results(page(order, browser_saved=browser_saved), "p.html")

    assert model["ipdb_id"] == 936
    assert model["name"] == "4 Queens"
    assert model["manufacturer_full"] == "Bally Manufacturing Corporation"
    assert model["type_code"] == "EM"
    # The two that swap places. Reading by position puts each in the other's slot.
    assert model["production_units"] == 1256
    assert model["specialties"] == ["Zipper Flippers"]
    assert model["players"] == 1
    assert model["model_number"] == "890"
    assert model["rating_score"] == 7.3


def test_reordering_columns_moves_no_value():
    """The same row, two orders, must parse to the same model."""
    (as_specialty,) = search.parse_results(
        page(SPECIALTY_ORDER, browser_saved=True), "a.html"
    )
    (as_type,) = search.parse_results(page(TYPE_ORDER, browser_saved=False), "b.htm")

    assert as_specialty == as_type


def test_a_missing_column_is_refused():
    """Dropped rather than moved: the field would otherwise arrive empty."""
    order = [name for name in SPECIALTY_ORDER if name != "Prod."]
    with pytest.raises(search.ParseError, match=re.escape("no 'Prod.' column")):
        search.parse_results(page(order, browser_saved=True), "p.html")


def test_a_page_shorter_than_it_declares_is_refused():
    body = page(SPECIALTY_ORDER, browser_saved=True, rows=1).replace(
        "(1 records match)", "(32 records match)"
    )
    with pytest.raises(search.ParseError, match="declares 32 records"):
        search.parse_results(body, "p.html")


@pytest.mark.parametrize(
    ("cell", "text", "units"),
    [
        ("1,256", "1,256", 1256),
        ("~200", "~200", 200),
        ("none", "none", None),
        ("few", "few", None),
        ("unknown", "unknown", None),
        ("", None, None),
    ],
)
def test_production_words_that_have_no_count(cell, text, units):
    """`unknown` is IPDB saying out loud what a blank cell leaves implied."""
    body = page(SPECIALTY_ORDER, browser_saved=True).replace(">1,256<", f">{cell}<")
    (model,) = search.parse_results(body, "p.html")

    assert model["production_text"] == text
    assert model["production_units"] == units


def test_an_unknown_production_word_is_refused():
    """A word nobody has seen must raise, not be kept as a count that is not one."""
    body = page(SPECIALTY_ORDER, browser_saved=True).replace(">1,256<", ">a handful<")
    with pytest.raises(search.ParseError, match="unrecognised production"):
        search.parse_results(body, "p.html")


# --------------------------------------------------------------------------- #
# The echoed search form, which is where a page states its own filter
# --------------------------------------------------------------------------- #


def test_the_form_states_the_searched_specialty_and_the_whole_list():
    """A page that says what it filtered on also says what it chose from."""
    body = page(SPECIALTY_ORDER, selected="Zipper Flippers")
    terms, selected = search.parse_specialty_filter(body, "p.html")

    assert selected == "Zipper Flippers"
    assert terms == SPECIALTIES


def test_a_page_with_no_form_states_no_filter():
    """IPDB's Type search echoes nothing back; that is absence, not failure."""
    assert (
        search.parse_specialty_filter(page(TYPE_ORDER, with_form=False), "pm.htm")
        is None
    )


def test_a_form_echoed_unfiltered_states_a_list_but_no_filter():
    body = page(SPECIALTY_ORDER, selected=None)
    terms, selected = search.parse_specialty_filter(body, "p.html")

    assert selected is None
    assert terms == SPECIALTIES


def test_two_selected_specialties_are_refused():
    body = page(SPECIALTY_ORDER, selected="Widebody").replace(
        '<option value="25">', '<option selected="selected" value="25">'
    )
    with pytest.raises(search.ParseError, match="two Specialties marked selected"):
        search.parse_specialty_filter(body, "p.html")

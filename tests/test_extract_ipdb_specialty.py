"""Tests for extract_ipdb_specialty_to_jsonl — saved advanced-search pages to JSONL.

Offline: pages are built here from the markup IPDB actually serves, since the
real ones are 7MB of saved HTML and the interesting variation is a handful of
cells.

What's covered is what makes the census trustworthy. The extract's whole claim
is COMPLETENESS -- a machine absent from it has no specialty -- and a census
reads complete whether or not it is, so every way the download can be short must
raise rather than emit. Hence the refusal tests outnumber the parsing ones: a
declared count that disagrees with the rows, a specialty other pages name with
no page of its own, a results table whose columns moved, pages saved either side
of an edit at IPDB.

The parsing tests cover the cells that are not merely text -- the specialty set
read from `title` attributes rather than the elided cell, the `*` that marks a
Project Date, production stated as a word or an approximation, and a rating IPDB
disclaims.
"""

from __future__ import annotations

import json

import extract_ipdb_specialty_to_jsonl as extract
import ipdb_search
import pytest

VOCABULARY = {
    "Bingo Machine": 3,
    "Not A Pinball": 18,
    "Widebody": 14,
    "Zipper Flippers": 25,
}


def dropdown(selected: str) -> str:
    options = ['<option value="0">Any Specialty&nbsp;</option>']
    for name, identifier in sorted(VOCABULARY.items()):
        mark = ' selected="selected"' if name == selected else ""
        options.append(f'<option{mark} value="{identifier}">{name}\n</option>')
    return '<select tabindex="9" name="specialty">' + "".join(options) + "</select>"


def specialty_cell(*names: str, elide: bool = False) -> str:
    spans = []
    for name in names:
        # IPDB tags the Not A Pinball span with a class, so `title` is not the
        # first attribute on every span.
        css = ' class="notapinballspec"' if name == "Not A Pinball" else ""
        shown = name[:15] + "..." if elide else name
        spans.append(f'<span{css} title="{name}">{shown}</span>')
    return "&nbsp;" + ",<br>&nbsp;".join(spans)


def row(
    ipdb_id: int = 936,
    name: str = "4 Queens",
    date: str = "1970-12",
    mfg: str = "Bally",
    mfg_full: str = "Bally Manufacturing Corporation",
    type_code: str = "EM",
    type_text: str = "Electro-Mechanical Game",
    specialties: str = "",
    production: str = "1,256",
    players: str = "1 Player Game",
    model: str = "890",
    photos: str = "12",
    rating: str = '<span title="Rated 7.3 after 14 ratings">7.3</span>',
    anchor: bool = False,
) -> str:
    href = (
        f'href="#{ipdb_id}"'
        if anchor
        else f'href="https://ipdb.org/machine.cgi?id={ipdb_id}"'
    )
    if date.endswith("*"):
        date = (
            '<span class="date-tooltip"><span class="date-tooltiptext">'
            "* indicates Project Date, not Manufacture Date</span>"
            f"{date}</span>"
        )
    players_cell = f'<span title="{players}">x</span>' if players else ""
    return (
        '<tr valign="top" class="oddrow">'
        f'<td nowrap class="normal" align="left">{date} </td>'
        f'<td class="normal" align="left"><a class="linkid" {href}>{name}</a></td>'
        f'<td nowrap class="normal"><span title="{mfg_full}">{mfg}</span></td>'
        f'<td nowrap class="normal"><span title="{type_text}">{type_code}</span></td>'
        f'<td nowrap class="normal" align="left">{specialties}</td>'
        f'<td nowrap class="normal" align="right">{production}</td>'
        f'<td nowrap class="normal" align="left">{players_cell}</td>'
        f'<td nowrap class="normal" align="left">{model}</td>'
        f'<td nowrap class="normal" align="right">{photos}</td>'
        f'<td nowrap class="normal" align="center">{rating}</td>'
        "</tr>"
    )


HEADERS = [
    "Date",
    "Name&nbsp;&nbsp;(Click to display that game)",
    "MFG",
    "Type",
    "Specialty",
    "Prod.",
    "Pl.",
    "Model",
    "Pics",
    "Rating",
]


def page(
    selected: str,
    rows: list[str],
    declared: int | None = None,
    headers: list[str] | None = None,
) -> str:
    cells = "".join(f"<th nowrap align=center>{h}</th>" for h in (headers or HEADERS))
    count = len(rows) if declared is None else declared
    return (
        "<html><body>"
        f"<form>{dropdown(selected)}</form>"
        f"<b>({count} records match)</b>"
        f'<table class="sortable" id="gamelist" width="90%"><tbody>'
        f'<tr valign="top">{cells}</tr>'
        + "".join(rows)
        + "</tbody></table></body></html>"
    )


def write(tmp_path, pages: dict[str, str]):
    for name, body in pages.items():
        (tmp_path / f"{name}.html").write_text(body, encoding=ipdb_search.ENCODING)
    return tmp_path


# --------------------------------------------------------------------------- #
# The census the pages add up to
# --------------------------------------------------------------------------- #


def test_a_machine_on_two_pages_carries_both_specialties(tmp_path):
    """The union across pages is the point: one row per machine, all its terms."""
    write(
        tmp_path,
        {
            "widebody": page(
                "Widebody",
                [row(specialties=specialty_cell("Widebody", "Zipper Flippers"))],
            ),
            "zipper": page(
                "Zipper Flippers",
                [row(specialties=specialty_cell("Widebody", "Zipper Flippers"))],
            ),
        },
    )
    census, _ = extract.build(tmp_path)

    assert len(census) == 1
    assert [s["specialty"] for s in census[0]["specialties"]] == [
        "Widebody",
        "Zipper Flippers",
    ]


def test_each_assignment_carries_the_search_that_evidences_it(tmp_path):
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers", [row(specialties=specialty_cell("Zipper Flippers"))]
            )
        },
    )
    census, _ = extract.build(tmp_path)

    assignment = census[0]["specialties"][0]
    assert assignment["specialty_id"] == 25
    assert assignment["source_url"] == (
        "https://www.ipdb.org/search.pl?specialty=25&sortby=name&searchtype=advanced"
    )


def test_specialties_come_from_the_title_not_the_elided_cell_text(tmp_path):
    """IPDB truncates the cell to `Shaker Ball Mac...` and spells it in `title`."""
    cell = specialty_cell("Bingo Machine", "Not A Pinball", elide=True)
    write(
        tmp_path,
        {
            "bingo": page("Bingo Machine", [row(specialties=cell)]),
            "not_a_pinball": page("Not A Pinball", [row(specialties=cell)]),
        },
    )
    census, _ = extract.build(tmp_path)

    assert [s["specialty"] for s in census[0]["specialties"]] == [
        "Bingo Machine",
        "Not A Pinball",
    ]


def test_a_small_result_page_links_the_machine_by_fragment(tmp_path):
    """Few enough matches and IPDB inlines the records, anchoring rather than linking."""
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers",
                [
                    row(
                        ipdb_id=6763,
                        anchor=True,
                        specialties=specialty_cell("Zipper Flippers"),
                    )
                ],
            )
        },
    )
    census, _ = extract.build(tmp_path)

    assert census[0]["ipdb_id"] == 6763


def test_the_vocabulary_records_every_term_and_whether_it_was_saved(tmp_path):
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers", [row(specialties=specialty_cell("Zipper Flippers"))]
            )
        },
    )
    _, vocabulary = extract.build(tmp_path)

    assert len(vocabulary) == len(VOCABULARY)
    saved = {term["specialty"]: term["downloaded"] for term in vocabulary}
    assert saved["Zipper Flippers"] is True
    assert saved["Widebody"] is False


def test_every_key_is_present_on_every_row(tmp_path):
    """A row's inferred type must not depend on which rows read_json_auto saw."""
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers",
                [
                    row(specialties=specialty_cell("Zipper Flippers")),
                    row(
                        ipdb_id=59,
                        name="Alligator",
                        date="????",
                        production="",
                        model="",
                        photos="",
                        rating="",
                        players="",
                        specialties=specialty_cell("Zipper Flippers"),
                    ),
                ],
            )
        },
    )
    census, _ = extract.build(tmp_path)

    assert set(census[0]) == set(census[1])
    # Sorted by id, so the sparse row (59) comes first. It survives a JSON round
    # trip with its absences intact rather than dropped.
    sparse = json.loads(json.dumps(census[0]))
    assert sparse["ipdb_id"] == 59
    assert sparse["production_units"] is None
    assert sparse["rating_score"] is None
    assert sparse["date_year"] is None


# --------------------------------------------------------------------------- #
# Cells that are not merely text
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("cell", "expected"),
    [
        ("1970-12", ("1970-12", 1970, 12, False)),
        ("1957", ("1957", 1957, None, False)),
        ("????", (None, None, None, False)),
        ("1954-05*", ("1954-05*", 1954, 5, True)),
        ("1941*", ("1941*", 1941, None, True)),
    ],
)
def test_dates_keep_their_precision_and_their_project_mark(tmp_path, cell, expected):
    """A year IPDB stated alone must not arrive padded to a January 1st."""
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers",
                [row(date=cell, specialties=specialty_cell("Zipper Flippers"))],
            )
        },
    )
    census, _ = extract.build(tmp_path)
    machine = census[0]

    assert (
        machine["date_text"],
        machine["date_year"],
        machine["date_month"],
        machine["date_is_project_date"],
    ) == expected


@pytest.mark.parametrize(
    ("cell", "text", "units", "approximate"),
    [
        ("1,256", "1,256", 1256, False),
        ("~200", "~200", 200, True),
        ("none", "none", None, False),
        ("few", "few", None, False),
        ("", None, None, False),
    ],
)
def test_production_keeps_the_words_that_have_no_integer(
    tmp_path, cell, text, units, approximate
):
    """`none` and no value both have no count, and only the text tells them apart."""
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers",
                [row(production=cell, specialties=specialty_cell("Zipper Flippers"))],
            )
        },
    )
    census, _ = extract.build(tmp_path)
    machine = census[0]

    assert (
        machine["production_text"],
        machine["production_units"],
        machine["production_approximate"],
    ) == (text, units, approximate)


def test_a_rating_ipdb_disclaims_is_kept_and_flagged(tmp_path):
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers",
                [
                    row(
                        rating='<span title="Too few ratings to count; only 9 so far ">'
                        '<font color="#AAAAAA">7.4</font></span>',
                        specialties=specialty_cell("Zipper Flippers"),
                    )
                ],
            )
        },
    )
    census, _ = extract.build(tmp_path)

    assert census[0]["rating_score"] == 7.4
    assert census[0]["rating_ratings"] == 9
    assert census[0]["rating_provisional"] is True


def test_the_manufacturer_tooltip_spells_out_the_abbreviated_cell(tmp_path):
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers", [row(specialties=specialty_cell("Zipper Flippers"))]
            )
        },
    )
    census, _ = extract.build(tmp_path)

    assert census[0]["manufacturer"] == "Bally"
    assert census[0]["manufacturer_full"] == "Bally Manufacturing Corporation"


# --------------------------------------------------------------------------- #
# Refusals — every way the download can be short
# --------------------------------------------------------------------------- #


def test_a_page_shorter_than_it_declares_is_refused(tmp_path):
    """A half-saved download would otherwise ship as a smaller census."""
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers",
                [row(specialties=specialty_cell("Zipper Flippers"))],
                declared=32,
            )
        },
    )
    with pytest.raises(ipdb_search.ParseError, match="declares 32 records"):
        extract.build(tmp_path)


def test_a_specialty_named_by_rows_with_no_page_of_its_own_is_refused(tmp_path):
    """The closure check. Without it, a missing file reads as machines lacking the term."""
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers",
                [row(specialties=specialty_cell("Widebody", "Zipper Flippers"))],
            )
        },
    )
    with pytest.raises(
        ipdb_search.ParseError, match="no downloaded page for specialty"
    ):
        extract.build(tmp_path)


def test_a_machine_missing_from_its_own_specialtys_page_is_refused(tmp_path):
    """Pages saved far enough apart that they disagree about who carries what."""
    write(
        tmp_path,
        {
            "widebody": page(
                "Widebody",
                [
                    row(ipdb_id=1, name="One", specialties=specialty_cell("Widebody")),
                ],
            ),
            "zipper": page(
                "Zipper Flippers",
                [
                    row(
                        ipdb_id=2,
                        name="Two",
                        specialties=specialty_cell("Widebody", "Zipper Flippers"),
                    ),
                ],
            ),
        },
    )
    with pytest.raises(
        ipdb_search.ParseError, match="whose own page omits the machine"
    ):
        extract.build(tmp_path)


def test_a_moved_results_column_is_refused(tmp_path):
    """Every field is read by position, so an inserted column would shift them all."""
    headers = [*HEADERS[:4], "Country", *HEADERS[4:-1]]
    write(
        tmp_path,
        {
            "zipper": page(
                "Zipper Flippers",
                [row(specialties=specialty_cell("Zipper Flippers"))],
                headers=headers,
            )
        },
    )
    with pytest.raises(ipdb_search.ParseError, match="IPDB changed the results table"):
        extract.build(tmp_path)


def test_pages_disagreeing_about_a_machine_are_refused(tmp_path):
    """They were saved minutes apart from one database; disagreement means an edit."""
    write(
        tmp_path,
        {
            "widebody": page(
                "Widebody",
                [
                    row(
                        production="1,256",
                        specialties=specialty_cell("Widebody", "Zipper Flippers"),
                    )
                ],
            ),
            "zipper": page(
                "Zipper Flippers",
                [
                    row(
                        production="1,300",
                        specialties=specialty_cell("Widebody", "Zipper Flippers"),
                    )
                ],
            ),
        },
    )
    with pytest.raises(ipdb_search.ParseError, match="spans an edit at IPDB"):
        extract.build(tmp_path)


def test_pages_with_different_dropdowns_are_refused(tmp_path):
    """The vocabulary moved mid-download, so the two halves describe different things."""
    stale = page("Widebody", [row(specialties=specialty_cell("Widebody"))])
    write(
        tmp_path,
        {
            "widebody": stale,
            "zipper": page(
                "Zipper Flippers", [row(specialties=specialty_cell("Zipper Flippers"))]
            ).replace(
                '<option value="0">Any Specialty&nbsp;</option>',
                '<option value="0">Any Specialty&nbsp;</option>'
                '<option value="30">Pitch And Bat\n</option>',
            ),
        },
    )
    with pytest.raises(ipdb_search.ParseError, match="spans a vocabulary change"):
        extract.build(tmp_path)


def test_an_unfiltered_search_page_is_refused(tmp_path):
    """Without a selected option the page does not say what it is a census of."""
    body = page("Widebody", [row(specialties=specialty_cell("Widebody"))]).replace(
        ' selected="selected"', ""
    )
    write(tmp_path, {"widebody": body})
    with pytest.raises(ipdb_search.ParseError, match="no specialty marked selected"):
        extract.build(tmp_path)


def test_an_empty_source_directory_is_refused(tmp_path):
    with pytest.raises(ipdb_search.ParseError, match="no saved search pages"):
        extract.build(tmp_path)

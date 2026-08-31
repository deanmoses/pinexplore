"""Tests for parse_ipdb — the IPDB machine-page reader — all offline.

The parser is a pure function, so the fixtures are HTML built here rather than
captured pages: each one is IPDB's real markup for the row under test (the
``&nbsp;``-glued names, the ``redirect.pl?`` hops, the ``[?]`` help images, the
five-cell document rows), trimmed to what the assertion is about.

What's covered is the two claims the module makes. Fidelity: a value IPDB
states at month precision must not come back padded to a day, ``Never
Produced`` must survive as a fact, and the page's own text must survive
verbatim alongside every parse of it. Totality: a label this parser has never
seen still arrives as data — text, lines and links — and says so in
``unknown_labels``.
"""

from __future__ import annotations

from decimal import Decimal

import parse_ipdb
import pytest
from parse_ipdb import (
    Credit,
    DatePrecision,
    FileSection,
    IpdbType,
    NotAModelPageError,
    parse_model_page,
)

HEADER = (
    '<tr><td valign=top colspan=5><table border=0 width="100%"><tr><td>'
    '<font size="+1"><B><a name="2006">Rollergames</a></B> / IPD No. '
    '<a class="linkid" href="machine.cgi?id=2006">2006</a> '
    "<I>/ April 04, 1990 / 4 Players</I></font><br>&nbsp;</td>"
    '<td align=right valign=top><font size="-1">'
    '[ <a href="edit.pl?gid=2006">Submit Changes</a> ]</font></td></tr></table></td></tr>'
)


def page(*rows: str, header: str = HEADER) -> str:
    """A machine page: IPDB's outer table, its header row, then ``rows``."""
    return (
        "<html><body>"
        '<table border=0 align=center width="80%" cellpadding=1 cellspacing=0>'
        f"{header}{''.join(rows)}"
        "</table></body></html>"
    )


def row(label: str, value: str) -> str:
    """A two-cell field row, the shape all but the listings use."""
    return (
        f'<tr><td nowrap width="20%" valign=baseline align=right><b>{label} </b></td>'
        f"<td colspan=4 align=left valign=baseline>{value}</td></tr>"
    )


def file_row(label: str | None, size: str, kind: str, href: str, name: str) -> str:
    """A five-cell document-listing row; ``label=None`` is a continuation row."""
    first = f"<b>{label} </b>" if label else "&nbsp;"
    return (
        f'<tr valign=top><td width="20%" align="right">{first}</td>'
        f"<td align=right nowrap> {size}</td>"
        f'<td align=left nowrap width="5%">&nbsp;{kind}&nbsp;</td>'
        f'<td align=left><a href="{href}">{name}</a></td>'
        "<td> [Williams Electronic Games]</td></tr>"
    )


# --------------------------------------------------------------------------- #
# The header line
# --------------------------------------------------------------------------- #


def test_header_gives_id_name_date_and_players():
    model = parse_model_page(page())
    assert model.ipdb_id == 2006
    assert model.name == "Rollergames"
    assert model.players == 4
    assert model.header_date is not None
    assert model.header_date.iso == "1990-04-04"


def test_header_without_a_date_still_parses():
    header = HEADER.replace("/ April 04, 1990 / 4 Players", "/ 1 Player")
    model = parse_model_page(page(header=header))
    assert model.players == 1
    assert model.header_date is None
    assert model.date is None


def test_front_page_for_an_unknown_id_is_not_a_model_page():
    """IPDB answers an id it doesn't have with its front page, not a 404, and
    archive.org captured those too — the one failure that must never look like
    a model whose every field is missing."""
    front_page = "<html><body><h1>The Internet Pinball Database</h1></body></html>"
    with pytest.raises(NotAModelPageError):
        parse_model_page(front_page)


def test_a_cross_reference_to_another_model_is_not_the_header():
    """Notes link other models by the same URL shape as the header's self-link."""
    notes = (
        'Copy of <a target="game940" href="machine.cgi?id=940">'
        "Gottlieb's 1971 '4 Square'</a>."
    )
    model = parse_model_page(page(row("Notes:", notes)))
    assert model.ipdb_id == 2006


# --------------------------------------------------------------------------- #
# Dates — the precision the dump threw away
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("printed", "iso", "precision"),
    [
        ("April 04, 1990", "1990-04-04", DatePrecision.DAY),
        ("October, 1991", "1991-10", DatePrecision.MONTH),
        ("1979", "1979", DatePrecision.YEAR),
    ],
)
def test_a_date_keeps_the_precision_ipdb_printed(printed, iso, precision):
    model = parse_model_page(page(row("Date Of Manufacture:", printed)))
    assert model.manufacture_date is not None
    assert model.manufacture_date.iso == iso
    assert model.manufacture_date.precision is precision
    assert model.manufacture_date.text == printed


def test_a_month_is_never_padded_to_a_day():
    """The inherited dump parsed "October, 1991" into 1991-10-01, inventing a
    day IPDB never stated. Anything reading `.iso` must not see one."""
    model = parse_model_page(page(row("Date Of Manufacture:", "October, 1991")))
    assert model.manufacture_date is not None
    assert model.manufacture_date.day is None
    assert model.manufacture_date.iso == "1991-10"


def test_project_date_is_read_and_kept_apart_from_the_production_date():
    model = parse_model_page(page(row("Project Date:", "October 11, 1982")))
    assert model.project_date is not None
    assert model.project_date.iso == "1982-10-11"
    assert model.manufacture_date is None
    assert model.date == model.project_date


def test_the_production_date_wins_over_the_project_date():
    model = parse_model_page(
        page(
            row("Project Date:", "1988"),
            row("Date Of Manufacture:", "May, 1989"),
        )
    )
    assert model.date is not None
    assert model.date.iso == "1989-05"


def test_an_unreadable_date_leaves_the_raw_text_in_fields():
    model = parse_model_page(page(row("Date Of Manufacture:", "Spring of 1932")))
    assert model.manufacture_date is None
    assert model.fields["Date Of Manufacture"].text == "Spring of 1932"


# --------------------------------------------------------------------------- #
# Production — the row that isn't always a number
# --------------------------------------------------------------------------- #


def test_never_produced_survives_as_a_fact():
    model = parse_model_page(page(row("Production:", "<i>Never Produced</i>")))
    assert model.production is not None
    assert model.production.never_produced is True
    assert model.production.status == "Never Produced"
    assert model.production.units is None
    assert model.production.text == "Never Produced"


def test_a_quantity_carries_its_qualifier():
    value = '5,000 units &nbsp;&nbsp;<font size="-1">(<i>approximate</i>)</font>'
    model = parse_model_page(page(row("Production:", value)))
    assert model.production is not None
    assert model.production.units == 5000
    assert model.production.qualifier == "approximate"
    assert model.production.status is None
    assert model.production.never_produced is False


def test_a_production_status_ipdb_has_never_printed_before_still_arrives():
    """IPDB italicizes whichever of the two the row means, so a new status is
    told apart from a confidence by the markup rather than by a word list."""
    model = parse_model_page(page(row("Production:", "<i>Cancelled In Tooling</i>")))
    assert model.production is not None
    assert model.production.status == "Cancelled In Tooling"
    assert model.production.qualifier is None
    assert model.production.never_produced is False


# --------------------------------------------------------------------------- #
# The rest of the modeled rows
# --------------------------------------------------------------------------- #


def test_manufacturer_splits_into_name_trade_name_years_and_id():
    value = (
        '<a href="search.pl?searchtype=advanced&amp;mfgid=349">Williams Electronics '
        "Games, Incorporated, a subsidiary of WMS Ind.,<br> Incorporated (1985-1999) "
        "<i>[Trade Name: Williams</i>]</a>"
    )
    model = parse_model_page(page(row("Manufacturer:", value)))
    assert model.manufacturer is not None
    assert model.manufacturer.ipdb_id == 349
    assert model.manufacturer.trade_name == "Williams"
    assert model.manufacturer.first_year == 1985
    assert model.manufacturer.last_year == 1999
    assert model.manufacturer.name == (
        "Williams Electronics Games, Incorporated, a subsidiary of WMS Ind., Incorporated"
    )


def test_manufacturer_location_is_split_off_the_name():
    value = (
        '<a href="search.pl?searchtype=advanced&amp;mfgid=204">Maquinas Recreativas '
        'Sociedad Anonima, <font size="-1">of Madrid,<br> Spain</font> '
        "<i>[Trade Name: Maresa</i>]</a>"
    )
    model = parse_model_page(page(row("Manufacturer:", value)))
    assert model.manufacturer is not None
    assert model.manufacturer.name == "Maquinas Recreativas Sociedad Anonima"
    assert model.manufacturer.location == "Madrid, Spain"
    assert model.manufacturer.trade_name == "Maresa"
    assert model.manufacturer.first_year is None


@pytest.mark.parametrize(
    ("years", "first", "last"),
    [
        ("(1931-1932)", 1931, 1932),  # the ordinary range
        ("(1933)", 1933, None),  # a single year, no dash
        ("(2016-now)", 2016, None),  # still operating
        ("(0-1925)", None, 1925),  # IPDB's "start unknown"
    ],
)
def test_the_years_are_read_without_the_location_following_them_in(years, first, last):
    """A punctuation rule over the flattened cell has to guess where the
    location ends, and guesses wrong on every year form but the ordinary one —
    swallowing "of Corpus Christi, Texas, USA" into the name. Splitting on the
    markup instead, the location is wherever its ``<font>`` is."""
    value = (
        f'<a href="search.pl?searchtype=advanced&amp;mfgid=112">Electro Black Diamond '
        f'Company, <font size="-1">of Corpus Christi,<br> Texas, USA</font> {years}</a>'
    )
    model = parse_model_page(page(row("Manufacturer:", value)))
    assert model.manufacturer is not None
    assert model.manufacturer.name == "Electro Black Diamond Company"
    assert model.manufacturer.location == "Corpus Christi, Texas, USA"
    assert model.manufacturer.first_year == first
    assert model.manufacturer.last_year == last


def test_type_takes_its_code_from_the_help_link_ipdb_attaches():
    """The row's ``[?]`` links IPDB's classification help and names the code in
    the URL, which is markup rather than a parenthesis convention in prose."""
    value = (
        "Solid State Electronic (SS)&nbsp;"
        '<a href="showhelp.pl?item=type&amp;highlight=SS#SS">'
        '<img alt="[?]" src="/graphic/question.gif"></a>'
    )
    model = parse_model_page(page(row("Type:", value)))
    assert model.type_code is IpdbType.SOLID_STATE
    assert model.type_text == "Solid State Electronic (SS)"


def test_type_falls_back_to_the_parenthesis_when_the_help_link_is_gone():
    """A capture that lost the help link, and one that rendered its ``[?]`` as
    text rather than an image — the code must survive both."""
    bare = "Electro-mechanical (EM)"
    model = parse_model_page(page(row("Type:", bare)))
    assert model.type_code is IpdbType.ELECTRO_MECHANICAL

    trailing = f'{bare}&nbsp;<a href="showhelp.pl?item=type">[?]</a>'
    model = parse_model_page(page(row("Type:", trailing)))
    assert model.type_code is IpdbType.ELECTRO_MECHANICAL


def test_an_unknown_type_code_leaves_the_text_and_no_enum():
    model = parse_model_page(page(row("Type:", "Some New Thing (XY)")))
    assert model.type_code is None
    assert model.type_text == "Some New Thing (XY)"


def test_rating_reads_score_ratings_and_comments():
    value = (
        '<a href="rate/showrate.pl?gid=2006"><b>7.5</b>/10&nbsp;&nbsp;</a>'
        '(<a href="rate/showrate.pl?gid=2006">58 ratings/39 comments</a>)'
    )
    model = parse_model_page(page(row("Average Fun Rating:", value)))
    assert model.rating is not None
    assert model.rating.score == Decimal("7.5")
    assert model.rating.ratings == 58
    assert model.rating.comments == 39
    assert model.rating.provisional is False


def test_a_score_ipdb_disowns_is_provisional():
    value = (
        '<font color=red>Needs More Ratings!</font> <font color="#666666">7.0 / 10'
        '</font>&nbsp;&nbsp;(<a href="rate/showrate.pl?gid=120">9 ratings</a>)'
    )
    model = parse_model_page(page(row("Average Fun Rating:", value)))
    assert model.rating is not None
    assert model.rating.provisional is True
    assert model.rating.score == Decimal("7.0")


def test_an_unrated_model_has_no_rating():
    value = "<font color=red>No ratings on file</font>"
    model = parse_model_page(page(row("Average Fun Rating:", value)))
    assert model.rating is None


def test_people_come_back_named_and_linked():
    value = (
        '<span title="…"><a href="search.pl?searchtype=advanced&amp;ppl=Pat%20McMahon">'
        "Pat&nbsp;McMahon</a></span>, "
        '<span title="…"><a href="search.pl?searchtype=advanced&amp;ppl=Linda%20Deal">'
        "Linda&nbsp;Deal&nbsp;(aka&nbsp;Doane)</a></span>"
    )
    model = parse_model_page(page(row("Art by:", value)))
    art = model.credited(Credit.ART)
    assert [person.name for person in art] == [
        "Pat McMahon",
        "Linda Deal (aka Doane)",
    ]
    assert (
        art[0].url
        == "https://www.ipdb.org/search.pl?searchtype=advanced&ppl=Pat%20McMahon"
    )


def test_a_credit_role_this_parser_has_never_seen_is_still_read_as_people():
    """Credits are found by the label's "… by" shape, not by a list of roles,
    so a role IPDB adds arrives as people rather than as a raw string."""
    value = (
        '<a href="search.pl?searchtype=advanced&amp;ppl=Jane%20Doe">Jane&nbsp;Doe</a>'
    )
    model = parse_model_page(page(row("Choreography by:", value)))
    assert [person.name for person in model.credited("Choreography by")] == ["Jane Doe"]
    assert model.unknown_labels == ()


def test_themes_and_specialties_split_the_way_ipdb_writes_them():
    themes = "Sports - Roller Derby  - Roller Skating - Licensed Theme"
    specialty = (
        'Bingo Machine&nbsp;<a href="glossary.php#Bingo"><img alt="[?]"></a><br>'
        'One Ball Game&nbsp;<a href="glossary.php#OneBall"><img alt="[?]"></a><br>'
    )
    model = parse_model_page(page(row("Theme:", themes), row("Specialty:", specialty)))
    assert model.themes == (
        "Sports",
        "Roller Derby",
        "Roller Skating",
        "Licensed Theme",
    )
    assert model.specialties == ("Bingo Machine", "One Ball Game")


def test_notes_keep_their_paragraphs_and_drop_their_glossary_links():
    value = (
        'Pat McMahon was the artist for both <a class="glossarylink" '
        'href="/glossary.php#Backglass">backglass</a> and playfield.<br><br>'
        "Production Start Date: Apr-4-1990<br>Production End Date: Jun-21-1990<P>"
    )
    model = parse_model_page(page(row("Notes:", value)))
    assert model.notes == (
        "Pat McMahon was the artist for both backglass and playfield.\n\n"
        "Production Start Date: Apr-4-1990\nProduction End Date: Jun-21-1990"
    )


def test_slogans_and_citations_come_back_one_per_line():
    slogans = '"Stay on Track with another Williams Winner!"<br>"Let the Good Times Roll!"<br>'
    photos = (
        "Chicago Tribune May 21, 1990<br>"
        '<a href="redirect.pl?http://www.amazon.com/gp/product/0764341073/">'
        "The Pinball Compendium 1982 to Present</a>, page 124<br>"
    )
    model = parse_model_page(
        page(row("Marketing Slogans:", slogans), row("Photos in:", photos))
    )
    assert model.marketing_slogans == (
        '"Stay on Track with another Williams Winner!"',
        '"Let the Good Times Roll!"',
    )
    assert model.photos_in[0].text == "Chicago Tribune May 21, 1990"
    assert model.photos_in[0].url is None
    assert model.photos_in[1].text == "The Pinball Compendium 1982 to Present, page 124"
    assert model.photos_in[1].url == "http://www.amazon.com/gp/product/0764341073/"


def test_an_off_site_link_is_unwrapped_from_ipdbs_redirect():
    value = (
        '<a href="redirect.pl?http://www.ipsnd.net/View.aspx?id=2006">View at IPSND'
        '</a>&nbsp;&nbsp;<font size="-1"><I>(External site)</i></font>'
    )
    model = parse_model_page(page(row("Serial Number Database:", value)))
    assert model.serial_number_database_url == "http://www.ipsnd.net/View.aspx?id=2006"


def test_a_relative_link_is_resolved_against_the_site_root():
    value = '<a href="https://www.ipdb.org/rulesheets/2006/ROLLERGA.HTM">Rulesheet</a>'
    model = parse_model_page(page(row("Rule Sheets:", value)))
    assert (
        model.rule_sheets[0].url == "https://www.ipdb.org/rulesheets/2006/ROLLERGA.HTM"
    )
    assert model.rule_sheets[0].text == "Rulesheet"


def test_mpu_carries_ipdbs_own_id_for_it():
    value = '<a href="search.pl?searchtype=advanced&amp;mpu=9">Williams System 11C</a>'
    model = parse_model_page(page(row("MPU:", value)))
    assert model.mpu is not None
    assert model.mpu.name == "Williams System 11C"
    assert model.mpu.ipdb_id == 9


# --------------------------------------------------------------------------- #
# Document listings and images
# --------------------------------------------------------------------------- #


def test_a_listing_collects_its_continuation_rows():
    model = parse_model_page(
        page(
            file_row(
                "ROMs:", "241 KB", "ZIP", "https://www.ipdb.org/files/a.zip", "Romset"
            ),
            file_row(
                None, "36 KB", "ZIP", "https://www.ipdb.org/files/b.zip", "Game ROM L-2"
            ),
            file_row(
                "Documentation:",
                "5 MB",
                "PDF",
                "https://www.ipdb.org/files/c.pdf",
                "Manual",
            ),
        )
    )
    roms = model.listing(FileSection.ROMS)
    assert [entry.name for entry in roms] == ["Romset", "Game ROM L-2"]
    assert roms[0].size == "241 KB"
    assert roms[0].kind == "ZIP"
    assert roms[0].credit == "Williams Electronic Games"
    assert [entry.name for entry in model.listing(FileSection.DOCUMENTATION)] == [
        "Manual"
    ]


def test_a_listing_ipdb_invents_is_keyed_by_its_label():
    model = parse_model_page(
        page(
            file_row(
                "Schematics:",
                "1 MB",
                "PDF",
                "https://www.ipdb.org/files/s.pdf",
                "Sheet",
            )
        )
    )
    assert [entry.name for entry in model.listing("Schematics")] == ["Sheet"]


def test_images_give_the_full_size_url_dimensions_and_credit():
    grid = (
        '<table border=0><tr><td align=center width="20%">'
        '<a href="showpic.pl?id=2006&amp;picno=8601"><span title="…">'
        '<img src="https://www.ipdb.org/images/2006/tn_image-1.jpg" width=60 height=100 '
        'alt="Image # 8601: Rollergames Full Machine "><br>Full Machine</span></a>'
        '<br><font size="-2">649x1086</font><br>[Steve Nordseth]</td></tr></table>'
    )
    model = parse_model_page(page(row("Images:", grid)))
    (image,) = model.images
    assert image.thumbnail_url == "https://www.ipdb.org/images/2006/tn_image-1.jpg"
    assert image.url == "https://www.ipdb.org/images/2006/image-1.jpg"
    assert image.picno == 8601
    assert image.caption == "Full Machine"
    assert image.description == "Rollergames Full Machine"
    assert (image.width, image.height) == (649, 1086)
    assert image.credit == "Steve Nordseth"


# --------------------------------------------------------------------------- #
# Totality — the point of the rewrite
# --------------------------------------------------------------------------- #


def test_an_unmodeled_label_still_arrives_as_data():
    """The failure this parser exists to prevent: a field on the page that no
    consumer can see because the reader only knew a fixed list of labels."""
    value = (
        'Made of walnut<br>Signed by <a href="redirect.pl?https://example.com/maker">'
        "the maker</a><br>"
    )
    model = parse_model_page(page(row("Cabinet Wood:", value)))
    assert model.unknown_labels == ("Cabinet Wood",)
    field = model.fields["Cabinet Wood"]
    assert field.text == "Made of walnut Signed by the maker"
    assert field.lines == ("Made of walnut", "Signed by the maker")
    assert field.links[0].url == "https://example.com/maker"


def test_a_page_this_parser_fully_understands_reports_nothing_unknown():
    model = parse_model_page(
        page(
            row("Theme:", "Sports"),
            row("Model Number:", "576"),
            row("Source:", "flyer"),
        )
    )
    assert model.unknown_labels == ()


def test_every_modeled_row_is_also_kept_verbatim_in_fields():
    """A parse is a reading; the citation quotes the page. Both must survive."""
    model = parse_model_page(page(row("Production:", "5,250 units")))
    assert model.production is not None
    assert model.production.units == 5250
    assert model.fields["Production"].text == "5,250 units"


def test_bytes_in_cp1252_are_decoded_like_the_browser_does():
    """IPDB declares no charset and emits cp1252 — a name with a curly
    apostrophe must not come back as a replacement character."""
    header = HEADER.replace(">Rollergames<", ">Barrel O\u2019 Fun \u201861<")
    raw = page(header=header).encode("cp1252")
    assert b"\x92" in raw  # the byte a utf-8 decode would choke on
    assert parse_model_page(raw).name == "Barrel O\u2019 Fun \u201861"


def test_rows_wrapped_in_a_tbody_are_still_found():
    """IPDB's own HTML 4.01 omits `tbody`, but a page saved out of a browser
    carries the one the browser inserted when it built the DOM. Looking only
    for a table's direct `tr` children finds no rows in that copy, and the
    parse comes back as a model whose every field is missing."""
    body = HEADER + row("Theme:", "Cards/Gambling")
    html = (
        "<html><body>"
        '<table border=0 align=center width="80%" cellpadding=1 cellspacing=0>'
        f"<tbody>{body}</tbody>"
        "</table></body></html>"
    )
    model = parse_model_page(html)
    assert model.ipdb_id == 2006
    assert model.themes == ("Cards/Gambling",)


def test_the_parse_is_pure():
    html = page(row("Theme:", "Sports - Soccer"), row("Production:", "500 units"))
    assert parse_model_page(html) == parse_model_page(html)


def test_the_module_exports_what_it_documents():
    for name in parse_ipdb.__all__:
        assert hasattr(parse_ipdb, name), name

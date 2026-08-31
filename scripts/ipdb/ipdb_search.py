"""IPDB advanced-search result pages, parsed.

<https://www.ipdb.org/search.pl?searchtype=advanced> filters the live database
and renders the matches as one table. Any filter renders the SAME table, so this
module is the table and the callers are the filters -- `Specialty`, `Type`, or
whatever gets saved next.

The pages are behind a bot wall and are downloaded by hand. That has a
consequence worth stating, because it shapes everything here: they arrive in two
different markups. A page saved as the server sent it keeps IPDB's HTML 4.01 --
bare attributes (`<tr valign=top>`), relative hrefs, no `tbody`. A page saved out
of a browser carries the DOM the browser built -- every attribute quoted, hrefs
absolutised, a `tbody` the original never had. Both are the same table and both
must parse, so nothing here may depend on either spelling.

COLUMNS ARE READ BY HEADER NAME, never by position, because IPDB moves them:
the Specialty searches put `Prod.` after `Specialty` and the Type search puts it
before. Position-reading would not have failed on that -- it would have quietly
read production out of the specialty column and back again. The header row is
the only thing that says which column is which, so it is what gets read, and a
column that goes missing raises rather than arriving empty.

Every cell that is not plain text is parsed here rather than by callers, because
the shapes are the table's rather than any filter's: the `*` that marks a Project
Date, production stated as a word or an approximation, a rating IPDB disclaims,
and the specialty set that must be read from `title` attributes because the cell
text is elided (`Shaker Ball Mac...`).
"""

from __future__ import annotations

import html
import re
from typing import Any

# One parsed model.
Model = dict[str, Any]

# IPDB serves these as windows-1252 and says so in its own meta tag. Decoding as
# UTF-8 would mangle the punctuation in Japanese and European titles.
ENCODING = "windows-1252"

_RECORDS_MATCH = re.compile(r"\((\d+) records? match\)")
_GAMELIST = re.compile(r'<table[^>]*id="gamelist"[^>]*>(.*?)</table>', re.DOTALL)
# `class="oddrow"` is the only attribute spelled the same in both markups.
_ROW = re.compile(r'<tr[^>]*class="(?:odd|even)row"[^>]*>(.*?)</tr>', re.DOTALL)
_HEADER_ROW = re.compile(r"<tr[^>]*>((?:\s*<th.*?</th>\s*)+)</tr>", re.DOTALL)
_HEADER_CELL = re.compile(r"<th[^>]*>(.*?)</th>", re.DOTALL)
_CELL = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL)
# A results row links the model page directly on a large result set, and falls
# back to a fragment anchor on a small one, where IPDB inlines the full records
# below the table. The href may be absolute or relative. The id is the same.
_MODEL_ID = re.compile(r'machine\.cgi\?id=(\d+)|href="#(\d+)"')
_TITLE = re.compile(r'title="([^"]*)"')
# The Specialty cell's spans sometimes carry a class before the title
# (`notapinballspec`), so the attribute cannot be anchored to the tag.
_SPAN_TITLE = re.compile(r'<span[^>]*title="([^"]*)"')
_DATE = re.compile(r"^(\d{4})(?:-(\d{2}))?(\*)?$")
_PRODUCTION = re.compile(r"^(~)?([\d,]+)$")
_RATING_FIRM = re.compile(r"^Rated (\d+(?:\.\d+)?) after (\d+) ratings")
_RATING_PROVISIONAL = re.compile(r"^Too few ratings to count; only (\d+) so far")
_PLAYERS = re.compile(r"^(\d+) Player Game$")

# IPDB's words for a production run it cannot put a number to. Enumerated rather
# than matched loosely, so a word nobody has seen raises instead of being kept as
# a count that is not one.
PRODUCTION_WORDS = frozenset({"none", "few", "unknown"})

# Canonical column name -> the prefix its header starts with. Prefixes because
# IPDB decorates some of them ("Name  (Click to display that game)").
COLUMNS = {
    "date": "Date",
    "name": "Name",
    "manufacturer": "MFG",
    "type": "Type",
    "specialty": "Specialty",
    "production": "Prod.",
    "players": "Pl.",
    "model": "Model",
    "photos": "Pics",
    "rating": "Rating",
}


# The advanced-search form, echoed back on pages saved as the server sent them.
# `<select name="specialty">` carries every Specialty IPDB offers with its own
# numeric id, and marks the one this page searched for. Not every filter echoes:
# the Type search returns no such select at all, so callers must tolerate None.
_SPECIALTY_SELECT = re.compile(
    r'<select[^>]*name="specialty"[^>]*>(.*?)</select>', re.DOTALL
)
_OPTION = re.compile(r"<option([^>]*)>(.*?)</option>", re.DOTALL)
_OPTION_VALUE = re.compile(r'value="(\d+)"')

# The dropdown's do-not-filter option, which is not a Specialty.
ANY_SPECIALTY_ID = "0"

SEARCH_URL = (
    "https://www.ipdb.org/search.pl?specialty={id}&sortby=name&searchtype=advanced"
)


class ParseError(Exception):
    """A page did not have the shape this parser requires."""


def _text(fragment: str) -> str:
    """Tags stripped, entities resolved, whitespace collapsed."""
    without_tags = re.sub(r"<[^>]+>", " ", fragment)
    return re.sub(r"\s+", " ", html.unescape(without_tags).replace("\xa0", " ")).strip()


def _title_of(fragment: str) -> str | None:
    """The first `title=` attribute, which is IPDB's unabbreviated wording."""
    match = _TITLE.search(fragment)
    return html.unescape(match.group(1)).strip() if match else None


def _column_index(table: str, source: str) -> dict[str, int]:
    """Map each canonical column name to its position in THIS page's header row.

    The header is the contract. IPDB has already shipped two orderings of this
    table, so a page's own header is the only thing that says where production
    ends and specialty begins.
    """
    header = _HEADER_ROW.search(table)
    if header is None:
        raise ParseError(f"{source}: results table has no header row")

    headers = [_text(cell) for cell in _HEADER_CELL.findall(header.group(1))]
    index: dict[str, int] = {}
    for name, prefix in COLUMNS.items():
        matches = [i for i, actual in enumerate(headers) if actual.startswith(prefix)]
        if not matches:
            raise ParseError(
                f"{source}: no {prefix!r} column in {headers} -- IPDB changed the "
                "results table and this page cannot be read"
            )
        if len(matches) > 1:
            raise ParseError(
                f"{source}: {prefix!r} matches {len(matches)} columns in {headers}"
            )
        index[name] = matches[0]
    return index


def _parse_date(cell: str) -> tuple[str | None, int | None, int | None, bool]:
    """IPDB's date column: `1970-12`, `1957`, `????`, any of them `*`-suffixed.

    The star means the date is a Project Date rather than a Date Of Manufacture,
    which IPDB explains in a tooltip beside it. That distinction is the reason to
    take this column at all -- it is the same signal `ipdb_stg.archive_models`
    goes to the model page for -- so it is kept rather than stripped.

    Year and month stay separate integers. A DATE would pad a year-only value to
    January 1st and read as a day IPDB never stated.
    """
    # Drop the explanatory tooltip, which is prose rather than a value.
    cell = re.sub(
        r'<span class="date-tooltiptext">.*?</span>', "", cell, flags=re.DOTALL
    )
    text = _text(cell)
    if not text or text == "????":
        return None, None, None, False

    match = _DATE.match(text)
    if match is None:
        raise ParseError(f"unrecognised date {text!r}")
    year, month, star = match.groups()
    return text, int(year), int(month) if month else None, star is not None


def _parse_production(cell: str) -> tuple[str | None, int | None, bool]:
    """The production column, which states a count, an approximation, or a word.

    `1,256` is a count; `~200` is IPDB's own approximation; `none`, `few` and
    `unknown` are words standing where a count would be. The verbatim text is
    kept beside the integer because only the text distinguishes "IPDB says none
    were built" from "IPDB does not say" -- both of which have no integer, and
    `unknown` is IPDB saying the second out loud.
    """
    text = _text(cell) or None
    if text is None:
        return None, None, False
    match = _PRODUCTION.match(text)
    if match is None:
        if text in PRODUCTION_WORDS:
            return text, None, False
        raise ParseError(f"unrecognised production {text!r}")
    approximate, digits = match.groups()
    return text, int(digits.replace(",", "")), approximate is not None


def _parse_rating(cell: str) -> tuple[float | None, int | None, bool]:
    """The rating column, whose tooltip says how many ratings stand behind it.

    IPDB greys out and disclaims a score with too few ratings to count. That
    score is returned with `provisional` set rather than dropped: it is what IPDB
    displays, and the flag is what stops a reader treating it as settled.
    """
    tooltip = _title_of(cell)
    if tooltip is None:
        return None, None, False
    firm = _RATING_FIRM.match(tooltip)
    if firm is not None:
        return float(firm.group(1)), int(firm.group(2)), False
    provisional = _RATING_PROVISIONAL.match(tooltip)
    if provisional is not None:
        score = _text(cell)
        return (float(score) if score else None), int(provisional.group(1)), True
    raise ParseError(f"unrecognised rating tooltip {tooltip!r}")


def parse_row(row: str, index: dict[str, int], source: str) -> Model:
    """One results row into one model record."""
    cells: list[str] = _CELL.findall(row)
    if len(cells) <= max(index.values()):
        raise ParseError(f"{source}: row has {len(cells)} cells, fewer than the header")

    def cell(name: str) -> str:
        return cells[index[name]]

    identifier = _MODEL_ID.search(cell("name"))
    if identifier is None:
        raise ParseError(f"{source}: row links no model id")

    date_text, year, month, is_project_date = _parse_date(cell("date"))
    production_text, production_units, production_approximate = _parse_production(
        cell("production")
    )
    rating_score, rating_ratings, rating_provisional = _parse_rating(cell("rating"))

    players_text = _title_of(cell("players"))
    players = None
    if players_text is not None:
        match = _PLAYERS.match(players_text)
        if match is None:
            raise ParseError(f"{source}: unrecognised players {players_text!r}")
        players = int(match.group(1))

    photos = _text(cell("photos"))

    return {
        "ipdb_id": int(identifier.group(1) or identifier.group(2)),
        "name": _text(cell("name")) or None,
        "date_text": date_text,
        "date_year": year,
        "date_month": month,
        "date_is_project_date": is_project_date,
        # The cell abbreviates the company and the tooltip spells it out.
        "manufacturer": _text(cell("manufacturer")) or None,
        "manufacturer_full": _title_of(cell("manufacturer")),
        "type_code": _text(cell("type")) or None,
        "type_text": _title_of(cell("type")),
        "production_text": production_text,
        "production_units": production_units,
        "production_approximate": production_approximate,
        "players": players,
        "model_number": _text(cell("model")) or None,
        "n_photos": int(photos) if photos else None,
        "rating_score": rating_score,
        "rating_ratings": rating_ratings,
        "rating_provisional": rating_provisional,
        # Read from the titles, never the cell text, which IPDB elides.
        "specialties": sorted(
            {
                html.unescape(name).strip()
                for name in _SPAN_TITLE.findall(cell("specialty"))
            }
        ),
    }


def parse_results(page: str, source: str) -> list[Model]:
    """Every model on one saved search page.

    The page's own `(N records match)` is checked against the rows read. A
    half-saved download or a result set IPDB decided to paginate disagrees here
    rather than shipping short -- which matters because every caller is claiming
    its download is COMPLETE for whatever it filtered on, and a short page reads
    exactly like a filter that matched fewer models.
    """
    declared = _RECORDS_MATCH.search(page)
    if declared is None:
        raise ParseError(f"{source}: no '(N records match)' count")

    table = _GAMELIST.search(page)
    if table is None:
        raise ParseError(f"{source}: no results table")

    index = _column_index(table.group(1), source)
    rows = [parse_row(row, index, source) for row in _ROW.findall(table.group(1))]

    if len(rows) != int(declared.group(1)):
        raise ParseError(
            f"{source}: {len(rows)} rows parsed but the page declares "
            f"{declared.group(1)} records match"
        )
    return rows


def parse_specialty_filter(
    page: str, source: str
) -> tuple[dict[str, int], str | None] | None:
    """IPDB's Specialty list and the one this page searched for, if it says.

    Returns every Specialty in the dropdown as `{name: id}` alongside the name of
    the selected one, or None where the page carries no such dropdown. Both come
    off the same echoed form, so a page that states its filter also states the
    full list it was chosen from -- which is what lets the download check its own
    completeness rather than being taken on trust.
    """
    select = _SPECIALTY_SELECT.search(page)
    if select is None:
        return None

    terms: dict[str, int] = {}
    selected: str | None = None
    for attributes, label in _OPTION.findall(select.group(1)):
        value = _OPTION_VALUE.search(attributes)
        if value is None or value.group(1) == ANY_SPECIALTY_ID:
            continue
        name = _text(label)
        if name in terms:
            raise ParseError(
                f"{source}: Specialty {name!r} listed twice in the dropdown"
            )
        terms[name] = int(value.group(1))
        if "selected" in attributes:
            if selected is not None:
                raise ParseError(f"{source}: two Specialties marked selected")
            selected = name

    if not terms:
        raise ParseError(f"{source}: the Specialty dropdown is empty")
    return terms, selected

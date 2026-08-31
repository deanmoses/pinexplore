#!/usr/bin/env python3
"""Turn an IPDB machine page into structured fields.

One pure function: ``parse_model_page(html) -> IpdbModel``.  No I/O, no network, no cache.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, NamedTuple, cast
from urllib.parse import parse_qs, urljoin, urlsplit

import lxml.html
from lxml import etree

if TYPE_CHECKING:
    from collections.abc import Mapping

    from lxml.html import HtmlElement

__all__ = [
    "IPDB_BASE",
    "Citation",
    "Credit",
    "DatePrecision",
    "Field",
    "FileEntry",
    "FileSection",
    "ImageEntry",
    "IpdbDate",
    "IpdbModel",
    "IpdbParseError",
    "IpdbType",
    "Link",
    "Manufacturer",
    "Mpu",
    "NotAModelPageError",
    "Person",
    "Production",
    "ProductionQualifier",
    "Rating",
    "absolute_url",
    "parse_date",
    "parse_model_page",
]

# Every relative href on a machine page resolves against the site root.
IPDB_BASE: Final = "https://www.ipdb.org/"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class IpdbParseError(Exception):
    """The document could not be read as an IPDB machine page."""


class NotAModelPageError(IpdbParseError):
    """The document has no ``IPD No.`` header, so it is not a model's page.

    Asking IPDB for an id it doesn't have serves the site's front page rather
    than a 404. This keeps a soft 404 from being read as
    a model whose every field is missing.
    """


# --------------------------------------------------------------------------- #
# Value types
# --------------------------------------------------------------------------- #


class DatePrecision(StrEnum):
    """How much of a date IPDB actually stated."""

    YEAR = "year"
    MONTH = "month"
    DAY = "day"


class IpdbDate(NamedTuple):
    """A date IPDB printed, at whatever precision it printed it. ``text`` is
    verbatim ("April 04, 1990", "October, 1991", "1979"); the parts are the
    reading of it."""

    text: str
    year: int
    month: int | None = None
    day: int | None = None

    @property
    def precision(self) -> DatePrecision:
        if self.day is not None:
            return DatePrecision.DAY
        if self.month is not None:
            return DatePrecision.MONTH
        return DatePrecision.YEAR

    @property
    def iso(self) -> str:
        """As much of an ISO-8601 date as is known. Never padded: a day IPDB
        didn't state must not become the 1st."""
        if self.month is not None and self.day is not None:
            return f"{self.year:04d}-{self.month:02d}-{self.day:02d}"
        if self.month is not None:
            return f"{self.year:04d}-{self.month:02d}"
        return f"{self.year:04d}"


class Link(NamedTuple):
    """An anchor: its text, and its absolute target with ``redirect.pl?``
    unwrapped."""

    text: str
    url: str


class Field(NamedTuple):
    """One labeled row, read without knowing what the label means: the value on
    one line, split on ``<br>`` (how IPDB writes every list it has), and
    every anchor in it. An unmodeled label still arrives as data."""

    label: str
    text: str
    lines: tuple[str, ...] = ()
    links: tuple[Link, ...] = ()


class Person(NamedTuple):
    """One credited person. ``url`` is IPDB's people search for that exact name —
    the nearest thing the site has to a person id, which is what separates
    two catalog people who share a name."""

    name: str
    url: str | None = None


class Citation(NamedTuple):
    """One line of a bibliographic list (``Photos in``): the line as printed,
    plus any link it carried."""

    text: str
    url: str | None = None


class Manufacturer(NamedTuple):
    """One cell — ``D. Gottlieb & Company, a Columbia Pictures Industries Company
    (1976-1983)[Trade Name: Gottlieb]`` — split into its parts. ``text``
    keeps the whole thing, so a wrong split stays auditable."""

    text: str
    name: str
    ipdb_id: int | None = None
    trade_name: str | None = None
    location: str | None = None
    first_year: int | None = None
    last_year: int | None = None


class Mpu(NamedTuple):
    """The MPU (game control board family), with IPDB's own id for it."""

    name: str
    ipdb_id: int | None = None


class IpdbType(StrEnum):
    """IPDB's technology classification. Only codes seen in the wild are members;
    an unrecognized one leaves ``type_code`` None while ``type_text`` still
    carries what the page said."""

    ELECTRO_MECHANICAL = "EM"
    SOLID_STATE = "SS"
    PURE_MECHANICAL = "PM"

    @classmethod
    def from_code(cls, code: str) -> IpdbType | None:
        try:
            return cls(code)
        except ValueError:
            return None


class ProductionQualifier(StrEnum):
    """How sure IPDB is of a production quantity."""

    CONFIRMED = "confirmed"
    APPROXIMATE = "approximate"


class Production(NamedTuple):
    """A quantity, a status, or both. ``status`` is the row's italic verbatim when
    it isn't one of the two qualifiers, so ``Never Produced`` — a fact
    about the model, not a missing number — survives, and so does a status
    IPDB has yet to print."""

    text: str
    units: int | None = None
    qualifier: ProductionQualifier | None = None
    status: str | None = None
    never_produced: bool = False


class Rating(NamedTuple):
    """IPDB's player rating; None when nobody has rated the model. ``provisional``
    is its "Needs More Ratings!" — a score it prints but disowns."""

    text: str
    score: Decimal | None = None
    ratings: int | None = None
    comments: int | None = None
    provisional: bool = False


class FileSection(StrEnum):
    """The document listings, by row label. ``StrEnum`` members *are* strings, so
    ``documents`` stays keyed by plain label — keeping a listing IPDB adds
    later — and is still read as ``documents[FileSection.ROMS]``."""

    ROMS = "ROMs"
    DOCUMENTATION = "Documentation"
    SERVICE_BULLETINS = "Service Bulletins"
    FILES = "Files"
    MULTIMEDIA_FILES = "Multimedia Files"


class FileEntry(NamedTuple):
    """One row of a document listing. ``size`` and ``kind`` stay IPDB's display
    strings ("5 MB", "PDF"): the sizes are rounded, so bytes re-derived
    from them would be invented precision."""

    name: str
    url: str
    kind: str | None = None
    size: str | None = None
    credit: str | None = None


class ImageEntry(NamedTuple):
    """One thumbnail in the image grid. ``url`` is the full-size image and
    ``width``/``height`` its dimensions, so the grid alone says what the
    catalog would be downloading."""

    thumbnail_url: str
    url: str
    picno: int | None = None
    caption: str | None = None
    description: str | None = None
    page_url: str | None = None
    width: int | None = None
    height: int | None = None
    credit: str | None = None


class Credit(StrEnum):
    """The "… by" rows, by label. Credits are detected by the label's "… by" shape
    rather than by this list, so a role IPDB adds is still read as people;
    these are the readable way into ``credits``."""

    CONCEPT = "Concept by"
    DESIGN = "Design by"
    ART = "Art by"
    DOTS_ANIMATION = "Dots/Animation by"
    ANIMATION = "Animation by"
    MECHANICS = "Mechanics by"
    MUSIC = "Music by"
    SOUND = "Sound by"
    SOFTWARE = "Software by"


_NO_PEOPLE: Final[Mapping[str, tuple[Person, ...]]] = MappingProxyType({})
_NO_DOCUMENTS: Final[Mapping[str, tuple[FileEntry, ...]]] = MappingProxyType({})
_NO_FIELDS: Final[Mapping[str, Field]] = MappingProxyType({})


class IpdbModel(NamedTuple):
    """Everything one machine page states about one model. Only ``ipdb_id`` and
    ``name`` are guaranteed; every other field is absent on some real model
    — a never-produced design may carry a project date, an artist, nothing
    else."""

    ipdb_id: int
    name: str
    players: int | None = None
    # Production date if there is one, else project date. Kept apart from both
    # because on a sparse capture it is sometimes the only one present.
    header_date: IpdbDate | None = None
    manufacture_date: IpdbDate | None = None
    project_date: IpdbDate | None = None
    manufacturer: Manufacturer | None = None
    model_number: str | None = None
    common_abbreviations: tuple[str, ...] = ()
    mpu: Mpu | None = None
    type_code: IpdbType | None = None
    type_text: str | None = None
    production: Production | None = None
    rating: Rating | None = None
    themes: tuple[str, ...] = ()
    specialties: tuple[str, ...] = ()
    notable_features: str | None = None
    toys: str | None = None
    # A pointer at somebody else's catalog of eggs, not prose.
    easter_eggs: Citation | None = None
    notes: str | None = None
    marketing_slogans: tuple[str, ...] = ()
    photos_in: tuple[Citation, ...] = ()
    source: str | None = None
    # Keyed by row label so a role IPDB invents tomorrow is carried today;
    # index with a `Credit` member for the known ones. Same for `documents`.
    credits: Mapping[str, tuple[Person, ...]] = _NO_PEOPLE
    rule_sheets: tuple[Link, ...] = ()
    additional_media: tuple[Link, ...] = ()
    serial_number_database_url: str | None = None
    owners_list_url: str | None = None
    documents: Mapping[str, tuple[FileEntry, ...]] = _NO_DOCUMENTS
    images: tuple[ImageEntry, ...] = ()
    # Every labeled row, modeled or not — the backstop that makes this lossless.
    fields: Mapping[str, Field] = _NO_FIELDS
    # Labels with no typed home above; their content is still in `fields`, so a
    # non-empty tuple is the signal that IPDB has something new to model.
    unknown_labels: tuple[str, ...] = ()

    @property
    def date(self) -> IpdbDate | None:
        """The model's effective date: production if IPDB states one, else
        project. The same choice IPDB's own header line makes."""
        return self.manufacture_date or self.project_date or self.header_date

    def credited(self, role: str) -> tuple[Person, ...]:
        """The people credited in ``role`` (a ``Credit`` member or a raw
        label), or an empty tuple."""
        return self.credits.get(role, ())

    def listing(self, section: str) -> tuple[FileEntry, ...]:
        """The documents in ``section`` (a ``FileSection`` member or a raw
        label), or an empty tuple."""
        return self.documents.get(section, ())


# --------------------------------------------------------------------------- #
# Patterns — for parsing text within a single HTML node
# --------------------------------------------------------------------------- #

_MONTHS: Final[Mapping[str, int]] = MappingProxyType(
    {
        name: number
        for number, full in enumerate(
            (
                "january",
                "february",
                "march",
                "april",
                "may",
                "june",
                "july",
                "august",
                "september",
                "october",
                "november",
                "december",
            ),
            start=1,
        )
        for name in (full, full[:3])
    }
)

_DATE_DAY_RE: Final = re.compile(r"^([A-Za-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})$")
_DATE_MONTH_RE: Final = re.compile(r"^([A-Za-z]+)\.?,?\s+(\d{4})$")
_DATE_YEAR_RE: Final = re.compile(r"^(\d{4})$")

_IPD_NO_RE: Final = re.compile(r"IPD\s*No\.\s*\d+")
_PLAYERS_RE: Final = re.compile(r"^(\d+)\s+players?$", re.IGNORECASE)
# "0" is IPDB's unknown start, "now" its still-operating end.
_YEARS_RE: Final = re.compile(r"\(\s*(\d{4}|0)\s*(?:-\s*(\d{4}|now)?\s*)?\)")

# Type-code fallback when the help link is gone. Unanchored and read
# last-match-first, so text trailing the code cannot hide it.
_TYPE_CODE_RE: Final = re.compile(r"\(([A-Za-z]{2,3})\)")
_UNITS_RE: Final = re.compile(r"([\d,]+)\s+units?\b", re.IGNORECASE)
_NEVER_PRODUCED_RE: Final = re.compile(r"\bnever\s+produced\b", re.IGNORECASE)

_SCORE_RE: Final = re.compile(r"(\d+(?:\.\d+)?)\s*/\s*10\b")
_RATINGS_RE: Final = re.compile(r"(\d+)\s+ratings?\b", re.IGNORECASE)
_COMMENTS_RE: Final = re.compile(r"(\d+)\s+comments?\b", re.IGNORECASE)
_NO_RATINGS_RE: Final = re.compile(r"no ratings on file", re.IGNORECASE)
_PROVISIONAL_RE: Final = re.compile(r"needs more ratings", re.IGNORECASE)

_THEME_SPLIT_RE: Final = re.compile(r"\s+-\s+")
_DIMENSIONS_RE: Final = re.compile(r"^(\d+)\s*x\s*(\d+)$")
_BRACKETED_RE: Final = re.compile(r"^\[(.+)\]$")
_IMAGE_ALT_RE: Final = re.compile(r"^Image\s*#\s*(\d+)\s*:\s*(.*)$")
_BLANK_LINES_RE: Final = re.compile(r"\n{3,}")

# A five-cell row is a document listing; four is the floor this reader needs.
_LISTING_CELLS: Final = 5


# --------------------------------------------------------------------------- #
# Text helpers
# --------------------------------------------------------------------------- #


class _Segment(NamedTuple):
    """One ``<br>``-delimited run of a cell: its text and the links in it."""

    text: str
    links: tuple[Link, ...]


def _soft(text: str) -> str:
    """Whitespace runs to one space, ends left alone. For text about to be
    concatenated: trimming an anchor first would weld it to the next word
    ("…7.5/10" + "(58 ratings)")."""
    return re.sub(r"\s+", " ", text.replace("\xa0", " "))


def _collapse(text: str) -> str:
    """``_soft`` plus trimmed ends. IPDB glues names with ``&nbsp;``
    ("Steve&nbsp;Ritchie"), which is what makes one comparable with the
    catalog's."""
    return _soft(text).strip()


def _flat(element: HtmlElement) -> str:
    """An element's text, collapsed onto one line."""
    return _collapse(element.text_content())


def _find(element: HtmlElement, path: str) -> list[HtmlElement]:
    """``xpath`` narrowed to what every call here wants: a list of elements."""
    return cast("list[HtmlElement]", element.xpath(path))


def _segments(element: HtmlElement) -> tuple[_Segment, ...]:
    """Split an element's content on ``<br>``, keeping each run's links. IPDB uses
    ``<br>`` both for list items and (doubled) for paragraphs. Anchors are
    atomic, so a ``<br>`` inside one is a space, not a new item."""
    texts: list[list[str]] = [[]]
    links: list[list[Link]] = [[]]

    def walk(node: HtmlElement) -> None:
        if node.text:
            texts[-1].append(node.text)
        for child in node:
            tag = child.tag
            if not isinstance(tag, str):
                pass  # a comment or processing instruction: no text of its own
            elif tag == "br":
                texts.append([])
                links.append([])
            elif tag == "a":
                text = _soft(child.text_content())
                href = child.get("href")
                texts[-1].append(text)
                if href:
                    links[-1].append(Link(text=text.strip(), url=absolute_url(href)))
            else:
                walk(child)
            if child.tail:
                texts[-1].append(child.tail)

    walk(element)
    return tuple(
        _Segment(text=_collapse("".join(parts)), links=tuple(hrefs))
        for parts, hrefs in zip(texts, links, strict=True)
    )


def _text_without(element: HtmlElement, skip: set[HtmlElement]) -> str:
    """An element's text with the ``skip`` subtrees left out — what remains of a
    cell once the facts already read are removed. Their tails stay: IPDB
    writes the manufacturer's years after the ``<font>`` holding its city."""
    parts: list[str] = []

    def walk(node: HtmlElement) -> None:
        if node.text:
            parts.append(node.text)
        for child in node:
            if isinstance(child.tag, str) and child not in skip:
                walk(child)
            if child.tail:
                parts.append(child.tail)

    walk(element)
    return _collapse("".join(parts))


def _rich_text(element: HtmlElement) -> str:
    """A prose cell as plain text: ``<br>`` a line break, ``<br><br>`` a
    paragraph break, glossary and cross-reference links flattened to their
    words. Runs of blank lines collapse to one so the shape stays predictable.
    """
    text = "\n".join(segment.text for segment in _segments(element))
    return _BLANK_LINES_RE.sub("\n\n", text).strip()


def absolute_url(href: str) -> str:
    """Resolve an href, unwrapping IPDB's ``redirect.pl?`` click-counting hop so a
    citation gets the real target."""
    url = urljoin(IPDB_BASE, href.strip())
    marker = "redirect.pl?"
    position = url.find(marker)
    if position != -1:
        target = url[position + len(marker) :]
        if target.startswith(("http://", "https://")):
            return target
    return url


def _query_int(url: str, key: str) -> int | None:
    """One integer query parameter of a URL, or None."""
    values = parse_qs(urlsplit(url).query).get(key)
    if not values:
        return None
    try:
        return int(values[0])
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Value readers. IPDB's HTML is presentational, but that presentation is the
# only structure the page has and it is load-bearing: the trade name is the
# <i>, the city the <font size="-1">, the type code a parameter of the [?]
# help link. Read the markup; pattern-match only what it leaves as bare text.
# --------------------------------------------------------------------------- #


def parse_date(text: str) -> IpdbDate | None:
    """Read one of IPDB's three date shapes — ``April 04, 1990``, ``October,
    1991``, ``1979`` — or None, which leaves the raw string in ``fields``
    rather than inventing a precision."""
    text = _collapse(text)
    if not text:
        return None
    if match := _DATE_DAY_RE.match(text):
        month = _MONTHS.get(match.group(1).lower())
        if month is not None:
            return IpdbDate(
                text=text,
                year=int(match.group(3)),
                month=month,
                day=int(match.group(2)),
            )
    if match := _DATE_MONTH_RE.match(text):
        month = _MONTHS.get(match.group(1).lower())
        if month is not None:
            return IpdbDate(text=text, year=int(match.group(2)), month=month)
    if match := _DATE_YEAR_RE.match(text):
        return IpdbDate(text=text, year=int(match.group(1)))
    return None


def _read_manufacturer(cell: HtmlElement, field: Field) -> Manufacturer | None:
    """Four facts in one anchor, three with an element of their own: the trade
    name in ``<i>``, the city in ``<font size="-1">``, the legal name as
    the anchor's own text. Only the years are bare text."""
    if not field.text:
        return None
    anchors = _find(cell, ".//a[@href]")
    scope = anchors[0] if anchors else cell

    italics = _find(scope, ".//i")
    fonts = _find(scope, ".//font[@size='-1']")

    trade_name: str | None = None
    if italics and "Trade Name" in (label := _flat(italics[0])):
        # IPDB closes the italic before the bracket: `<i>[Trade Name: Bally</i>]`.
        trade_name = label.split(":", 1)[1].strip().rstrip("]").strip() or None

    location: str | None = None
    if fonts and (place := _flat(fonts[0])).lower().startswith("of "):
        location = place[3:].strip().rstrip(",") or None

    rest = _text_without(scope, {*italics[:1], *fonts[:1]})
    first_year: int | None = None
    last_year: int | None = None
    if match := _YEARS_RE.search(rest):
        # Neither "0" nor "now" is a year, so neither is stated as one.
        if match.group(1) != "0":
            first_year = int(match.group(1))
        if (end := match.group(2)) and end != "now":
            last_year = int(end)
        rest = rest[: match.start()] + rest[match.end() :]

    # The bracket closing the trade name is the italic's tail, so it survives.
    name = _collapse(rest.replace("]", " ")).rstrip(",").strip()
    return Manufacturer(
        text=field.text,
        name=name or field.text,
        ipdb_id=next(
            (
                _query_int(link.url, "mfgid")
                for link in field.links
                if "mfgid=" in link.url
            ),
            None,
        ),
        trade_name=trade_name,
        location=location,
        first_year=first_year,
        last_year=last_year,
    )


def _read_type_code(cell: HtmlElement, text: str | None) -> IpdbType | None:
    """The technology code, from the ``[?]`` help link the row ends in:
    ``showhelp.pl?item=type&highlight=SS#SS``. The parenthesis in the text
    beside it is the fallback, for a capture that lost the link."""
    for link in _links_in(cell):
        if "showhelp" not in link.url:
            continue
        highlight = parse_qs(urlsplit(link.url).query).get("highlight")
        if highlight and (found := IpdbType.from_code(highlight[0].upper())):
            return found
    for code in reversed(_TYPE_CODE_RE.findall(text or "")):
        if found := IpdbType.from_code(code.upper()):
            return found
    return None


def _read_mpu(field: Field) -> Mpu | None:
    if not field.text:
        return None
    for link in field.links:
        if "mpu=" in link.url:
            return Mpu(name=link.text, ipdb_id=_query_int(link.url, "mpu"))
    return Mpu(name=field.text)


def _read_production(cell: HtmlElement, field: Field) -> Production | None:
    """A count, a status, or a count plus a confidence — and the italic says
    which: ``5,000 units (<i>approximate</i>)`` against
    ``<i>Never Produced</i>``. Only the count is bare text."""
    if not field.text:
        return None
    units: int | None = None
    if match := _UNITS_RE.search(field.text):
        units = int(match.group(1).replace(",", ""))

    qualifier: ProductionQualifier | None = None
    status: str | None = None
    italics = _find(cell, ".//i")
    if italics and (italic := _flat(italics[0])):
        try:
            qualifier = ProductionQualifier(italic.lower())
        except ValueError:
            status = italic

    return Production(
        text=field.text,
        units=units,
        qualifier=qualifier,
        status=status,
        never_produced=_NEVER_PRODUCED_RE.search(status or "") is not None,
    )


def _read_rating(field: Field) -> Rating | None:
    if not field.text or _NO_RATINGS_RE.search(field.text):
        return None
    score: Decimal | None = None
    if match := _SCORE_RE.search(field.text):
        try:
            score = Decimal(match.group(1))
        except InvalidOperation:  # pragma: no cover - the group is digits and a dot
            score = None
    ratings = _RATINGS_RE.search(field.text)
    comments = _COMMENTS_RE.search(field.text)
    return Rating(
        text=field.text,
        score=score,
        ratings=int(ratings.group(1)) if ratings else None,
        comments=int(comments.group(1)) if comments else None,
        provisional=_PROVISIONAL_RE.search(field.text) is not None,
    )


def _read_people(field: Field) -> tuple[Person, ...]:
    """The people in a "… by" cell. Each is normally a link into IPDB's people
    search, which is the canonical spelling; a cell without them falls back
    to splitting on commas."""
    people = tuple(
        Person(name=link.text, url=link.url)
        for link in field.links
        if "ppl=" in link.url
    )
    if people:
        return people
    return tuple(
        Person(name=part.strip()) for part in field.text.split(",") if part.strip()
    )


def _split(text: str, separator: str | re.Pattern[str]) -> tuple[str, ...]:
    """The non-empty parts of a value IPDB writes as one separated string."""
    parts = (
        separator.split(text)
        if isinstance(separator, re.Pattern)
        else text.split(separator)
    )
    return tuple(part.strip() for part in parts if part.strip())


def _first_url(field: Field) -> str | None:
    """The target of a row that is one link (``Owners List URL``)."""
    return field.links[0].url if field.links else None


def _read_citations(cell: HtmlElement) -> tuple[Citation, ...]:
    return tuple(
        Citation(text=segment.text, url=segment.links[0].url if segment.links else None)
        for segment in _segments(cell)
        if segment.text
    )


def _read_file_row(cells: list[HtmlElement]) -> FileEntry | None:
    """One listing row: ``[label|blank] [size] [kind] [link] [credit]``. A row
    whose file is withheld for copyright has no anchor and is not an entry."""
    links = _links_in(cells[3])
    if not links:
        return None
    credit = _flat(cells[4])
    if match := _BRACKETED_RE.match(credit):
        credit = match.group(1).strip()
    return FileEntry(
        name=links[0].text,
        url=links[0].url,
        kind=_flat(cells[2]) or None,
        size=_flat(cells[1]) or None,
        credit=credit or None,
    )


def _read_images(cell: HtmlElement) -> tuple[ImageEntry, ...]:
    """The thumbnail grid: one entry per tile that actually holds an image."""
    entries: list[ImageEntry] = []
    for tile in _find(cell, ".//td"):
        images = _find(tile, ".//img[@src]")
        source = images[0].get("src") if images else None
        if not source:
            continue
        thumbnail = absolute_url(source)

        page_url: str | None = None
        picno: int | None = None
        for link in _links_in(tile):
            if "showpic" in link.url:
                page_url = link.url
                picno = _query_int(link.url, "picno")
                break

        # The dimensions have a <font size="-2"> of their own, so they are
        # addressable rather than recognized among the tile's other lines.
        width: int | None = None
        height: int | None = None
        for small in _find(tile, ".//font[@size='-2']"):
            if match := _DIMENSIONS_RE.match(_flat(small)):
                width, height = int(match.group(1)), int(match.group(2))
                break

        caption: str | None = None
        credit: str | None = None
        for index, segment in enumerate(_segments(tile)):
            if not segment.text or _DIMENSIONS_RE.match(segment.text):
                continue
            if match := _BRACKETED_RE.match(segment.text):
                credit = match.group(1).strip()
            elif index == 0:
                caption = segment.text

        description: str | None = None
        alt = _collapse(images[0].get("alt") or "")
        if alt:
            match = _IMAGE_ALT_RE.match(alt)
            description = match.group(2).strip() if match else alt
            if picno is None and match:
                picno = int(match.group(1))

        entries.append(
            ImageEntry(
                thumbnail_url=thumbnail,
                # Full size is the same path without the `tn_` prefix.
                url=thumbnail.replace("/tn_", "/", 1),
                picno=picno,
                caption=caption,
                description=description or None,
                page_url=page_url,
                width=width,
                height=height,
                credit=credit,
            )
        )
    return tuple(entries)


def _links_in(element: HtmlElement) -> tuple[Link, ...]:
    return tuple(
        Link(text=_flat(link), url=absolute_url(href))
        for link in _find(element, ".//a[@href]")
        if (href := link.get("href"))
    )


# --------------------------------------------------------------------------- #
# Page structure — one table, one row per field:
#
#   <tr><td colspan=5>    header: name / IPD No. N / date / n Players
#   <tr><td><b>Label: </b></td><td colspan=4>value</td>
#   <tr><td><b>ROMs: </b></td><td>size</td><td>kind</td><td>link</td><td>credit</td>
#   <tr><td>&nbsp;</td>...  a listing's continuation rows carry no label
#   <tr><td><b>Images: </b></td><td> ...nested grid of thumbnails... </td>
#
# Rows are read by shape — two cells a field, five a listing — not by label, so
# a row IPDB adds still lands somewhere.
# --------------------------------------------------------------------------- #


def _decode(html: bytes | str) -> str:
    """Bytes to text. IPDB declares no charset and emits cp1252 (the curly quotes
    in ``Barrel O' Fun '61``), so: utf-8, then cp1252, which cannot fail."""
    if isinstance(html, str):
        return html
    try:
        return html.decode("utf-8")
    except UnicodeDecodeError:
        return html.decode("cp1252", errors="replace")


def _label_of(cell: HtmlElement) -> str | None:
    """The row label in a first cell (``<b>Design by: </b>``), or None. The
    trailing colon is required — it is what separates a field label from
    the page's other bold text, above all the model name in the header."""
    bolds = _find(cell, ".//b")
    if not bolds:
        return None
    text = _flat(bolds[0])
    if not text.endswith(":"):
        return None
    return text[:-1].strip() or None


def _cell_of(element: HtmlElement) -> HtmlElement:
    """The nearest enclosing table cell, or the element itself if it is loose."""
    cells: list[HtmlElement] = list(element.iterancestors("td"))
    return cells[0] if cells else element


def _header_cell(document: HtmlElement) -> HtmlElement:
    """The cell holding ``<name> / IPD No. <id> / <date> / <n> Players``, found by
    the id self-link whose cell states "IPD No." — a page links other
    models from its notes, and those are not the header."""
    for link in _find(document, "//a[@href][contains(@href, 'machine.cgi?id=')]"):
        cell = _cell_of(link)
        if _IPD_NO_RE.search(_flat(cell)):
            return cell
    raise NotAModelPageError("no 'IPD No.' header on this page")


# A table's own rows, whether or not a `tbody` sits between. IPDB's HTML 4.01
# omits `tbody`, but a page saved out of a browser carries the one the browser
# inserted when it built the DOM, and `./tr` alone finds nothing in that copy.
_ROWS = "./tr|./tbody/tr"


def _model_table(header: HtmlElement) -> HtmlElement:
    """The table holding the field rows. Found by walking out from the header to
    the innermost ancestor with labeled rows as direct children, so a
    capture that wraps the page in extra layout tables still works."""
    ancestors: list[HtmlElement] = list(header.iterancestors("table"))
    for table in ancestors:
        for row in _find(table, _ROWS):
            cells = _find(row, "./td")
            if len(cells) > 1 and _label_of(cells[0]):
                return table
    if not ancestors:
        raise NotAModelPageError("the header line is not inside a table")
    return ancestors[-1]


class _Header(NamedTuple):
    ipdb_id: int
    name: str
    players: int | None
    date: IpdbDate | None


def _read_header(cell: HtmlElement) -> _Header:
    """Read the header cell: id, name, and the italic ``/ date / n Players``."""
    ipdb_id: int | None = None
    for link in _links_in(cell):
        if "machine.cgi" in link.url:
            ipdb_id = _query_int(link.url, "id")
            break
    if ipdb_id is None:
        raise NotAModelPageError("the header line carries no id")

    names = _find(cell, ".//b") or _find(cell, f".//a[@name='{ipdb_id}']")
    name = _flat(names[0]) if names else ""
    if not name:
        raise NotAModelPageError("the header line carries no name")

    players: int | None = None
    date: IpdbDate | None = None
    for italic in _find(cell, ".//i"):
        for part in _flat(italic).split("/"):
            part = part.strip()
            if not part:
                continue
            if match := _PLAYERS_RE.match(part):
                players = int(match.group(1))
            elif date is None:
                date = parse_date(part)
    return _Header(ipdb_id=ipdb_id, name=name, players=players, date=date)


def _read_field(label: str, cells: list[HtmlElement]) -> Field:
    """A labeled row read without knowing what the label means."""
    value = cells[1]
    segments = _segments(value)
    if len(cells) > 2:  # a listing row: the value is spread across the cells
        text = _collapse(" ".join(_flat(cell) for cell in cells[1:]))
    else:
        text = _collapse(" ".join(segment.text for segment in segments))
    return Field(
        label=label,
        text=text,
        lines=tuple(segment.text for segment in segments if segment.text),
        links=tuple(link for segment in segments for link in segment.links),
    )


# --------------------------------------------------------------------------- #
# The parse
# --------------------------------------------------------------------------- #


def parse_model_page(html: bytes | str) -> IpdbModel:
    """Parse an IPDB machine page (``machine.cgi?id=N``). Raises
    ``NotAModelPageError`` when there is no ``IPD No.`` header,
    ``IpdbParseError`` when the bytes aren't HTML."""
    try:
        document = lxml.html.document_fromstring(_decode(html))
    except (etree.ParserError, ValueError) as error:
        raise IpdbParseError(f"unparseable HTML: {error}") from error
    # Generated markup restates the id and the image paths; strip it so it
    # cannot reach the text of the cell holding it (the image grid).
    etree.strip_elements(document, "script", "style", with_tail=False)

    header_cell = _header_cell(document)
    header = _read_header(header_cell)
    model = IpdbModel(
        ipdb_id=header.ipdb_id,
        name=header.name,
        players=header.players,
        header_date=header.date,
    )

    fields: dict[str, Field] = {}
    unknown: list[str] = []
    credits: dict[str, tuple[Person, ...]] = {}
    documents: dict[str, list[FileEntry]] = {}
    listing: str | None = None

    for row in _find(_model_table(header_cell), _ROWS):
        cells = _find(row, "./td")
        if len(cells) < 2:
            continue  # the header row: one cell spanning the table
        label = _label_of(cells[0])

        # A document listing spans rows: the labeled first row, then
        # continuation rows whose first cell is empty.
        if len(cells) >= _LISTING_CELLS:
            if label is not None:
                listing = label
            if listing is not None:
                entry = _read_file_row(cells)
                if entry is not None:
                    documents.setdefault(listing, []).append(entry)
                if label is not None:
                    fields.setdefault(label, _read_field(label, cells))
                continue
        if label is None:
            continue
        listing = None

        value = cells[1]
        field = _read_field(label, cells)
        fields.setdefault(label, field)

        # Any "… by" row is people, listed or not — that is how a credit role
        # IPDB adds later still arrives as people rather than as a raw string.
        if label.casefold().endswith(" by"):
            credits[label] = _read_people(field)
            continue

        # `_replace` is checked by the type checker against the field it names,
        # so a reader wired to the wrong field is an error here, not a silent
        # corruption downstream.
        match label:
            case "Average Fun Rating":
                model = model._replace(rating=_read_rating(field))
            case "Manufacturer":
                model = model._replace(manufacturer=_read_manufacturer(value, field))
            case "Date Of Manufacture":
                model = model._replace(manufacture_date=parse_date(field.text))
            case "Project Date":
                model = model._replace(project_date=parse_date(field.text))
            case "Model Number":
                model = model._replace(model_number=field.text or None)
            case "Common Abbreviations":
                model = model._replace(common_abbreviations=_split(field.text, ","))
            case "MPU":
                model = model._replace(mpu=_read_mpu(field))
            case "Type":
                model = model._replace(
                    type_text=field.text or None,
                    type_code=_read_type_code(value, field.text),
                )
            case "Production":
                model = model._replace(production=_read_production(value, field))
            case "Serial Number Database":
                model = model._replace(serial_number_database_url=_first_url(field))
            case "Theme":
                model = model._replace(themes=_split(field.text, _THEME_SPLIT_RE))
            case "Specialty":
                model = model._replace(specialties=field.lines)
            case "Notable Features":
                model = model._replace(notable_features=_rich_text(value) or None)
            case "Toys":
                model = model._replace(toys=_rich_text(value) or None)
            case "Easter Eggs":
                model = model._replace(
                    easter_eggs=Citation(text=field.text, url=_first_url(field))
                    if field.text
                    else None
                )
            case "Notes":
                model = model._replace(notes=_rich_text(value) or None)
            case "Marketing Slogans":
                model = model._replace(marketing_slogans=field.lines)
            case "Photos in":
                model = model._replace(photos_in=_read_citations(value))
            case "Source":
                model = model._replace(source=field.text or None)
            case "Rule Sheets":
                model = model._replace(rule_sheets=field.links)
            case "Additional Media":
                model = model._replace(additional_media=field.links)
            case "Owners List URL":
                model = model._replace(owners_list_url=_first_url(field))
            case "Images":
                model = model._replace(images=_read_images(value))
            case _:
                if label not in unknown:
                    unknown.append(label)

    return model._replace(
        credits=MappingProxyType(credits),
        documents=MappingProxyType(
            {label: tuple(entries) for label, entries in documents.items()}
        ),
        fields=MappingProxyType(fields),
        unknown_labels=tuple(unknown),
    )

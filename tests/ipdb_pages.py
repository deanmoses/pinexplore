"""Saved IPDB advanced-search pages, built to order.

Not a test module: the search parser and the search extract both need pages to
read, and they must agree on what a page looks like. The two column orders and
the two markups here are the ones IPDB and a browser-save actually produce, so a
fixture that drifts from them stops testing anything.
"""

from __future__ import annotations

# The two orderings IPDB has actually served.
SPECIALTY_ORDER = [
    "Date",
    "Name",
    "MFG",
    "Type",
    "Specialty",
    "Prod.",
    "Pl.",
    "Model",
    "Pics",
    "Rating",
]
TYPE_ORDER = [
    "Date",
    "Name",
    "MFG",
    "Type",
    "Prod.",
    "Specialty",
    "Pl.",
    "Model",
    "Pics",
    "Rating",
]

# A short stand-in for IPDB's 27, enough to have one selected and others not.
SPECIALTIES = {"Bingo Machine": 3, "Widebody": 14, "Zipper Flippers": 25}

CELLS = {
    "Date": "1970-12 ",
    "Name": '<a class="linkid" href="{href}">4 Queens</a>',
    "MFG": '<span title="Bally Manufacturing Corporation">Bally</span>',
    "Type": '<span title="Electro-Mechanical Game">EM</span>',
    "Specialty": '&nbsp;<span title="Zipper Flippers">Zipper Flippers</span>',
    "Prod.": "1,256",
    "Pl.": '<span title="1 Player Game">1p</span>',
    "Model": "890",
    "Pics": "12",
    "Rating": '<span title="Rated 7.3 after 14 ratings">7.3</span>',
}


def dropdown(selected: str | None) -> str:
    """The Specialty select, echoed back with the searched term marked.

    A page that does not filter by Specialty carries no such select at all --
    IPDB's Type search is the real example -- so callers pass nothing for it.
    """
    options = ['<option value="0">Any Specialty&nbsp;</option>']
    for name, identifier in sorted(SPECIALTIES.items()):
        mark = ' selected="selected"' if name == selected else ""
        options.append(f'<option{mark} value="{identifier}">{name}\n</option>')
    return '<select name="specialty">' + "".join(options) + "</select>"


def page(
    order: list[str],
    *,
    browser_saved: bool = True,
    rows: int = 1,
    ipdb_id: int = 936,
    specialties: str | None = None,
    selected: str | None = None,
    with_form: bool = True,
) -> str:
    """One results page in either markup, with columns in the given order."""
    if browser_saved:
        tr = '<tr valign="top" class="oddrow">'
        td, th = '<td nowrap="nowrap" class="normal">', "<th>"
        href = f"https://ipdb.org/machine.cgi?id={ipdb_id}"
        open_body, close_body = "<tbody>", "</tbody>"
    else:
        tr = '<tr valign=top class="oddrow">'
        td, th = "<td nowrap class=normal>", "<th nowrap align=center>"
        href = f"machine.cgi?id={ipdb_id}"
        open_body, close_body = "", ""

    cells = dict(CELLS)
    if specialties is not None:
        cells["Specialty"] = specialties

    header = "".join(f"{th}<b>{name}</b></th>" for name in order)
    body = ""
    for _ in range(rows):
        body += (
            tr
            + "".join(f"{td}{cells[name].format(href=href)}</td>" for name in order)
            + "</tr>"
        )
    form = f"<form>{dropdown(selected)}</form>" if with_form else ""
    return (
        f"{form}<b>({rows} records match)</b>"
        f'<table class="sortable" id="gamelist">{open_body}'
        f"<tr>{header}</tr>{body}{close_body}</table>"
    )


def specialty_cell(*names: str, elide: bool = False) -> str:
    """The Specialty cell, which lists a model's WHOLE set on every page."""
    spans = []
    for name in names:
        # IPDB tags the Not A Pinball span with a class, so `title` is not the
        # first attribute on every span.
        css = ' class="notapinballspec"' if name == "Not A Pinball" else ""
        shown = name[:15] + "..." if elide else name
        spans.append(f'<span{css} title="{name}">{shown}</span>')
    return "&nbsp;" + ",<br>&nbsp;".join(spans)

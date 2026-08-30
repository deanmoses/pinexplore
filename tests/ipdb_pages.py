"""Saved IPDB advanced-search pages, built to order.

Not a test module: the search parser and the search extract both need pages
to read, and they must agree on what a page looks like. The two column orders
and the two markups here are the ones IPDB and a browser-save actually
produce, so a fixture that drifts from them stops testing anything.
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


def page(order: list[str], *, browser_saved: bool, rows: int = 1) -> str:
    """One results page in either markup, with columns in the given order."""
    if browser_saved:
        tr, td, th = (
            '<tr valign="top" class="oddrow">',
            '<td nowrap="nowrap" class="normal">',
            "<th>",
        )
        href, open_body, close_body = (
            "https://ipdb.org/machine.cgi?id=936",
            "<tbody>",
            "</tbody>",
        )
    else:
        tr, td, th = (
            '<tr valign=top class="oddrow">',
            "<td nowrap class=normal>",
            "<th nowrap align=center>",
        )
        href, open_body, close_body = "machine.cgi?id=936", "", ""

    header = "".join(f"{th}<b>{name}</b></th>" for name in order)
    body = ""
    for _ in range(rows):
        body += (
            tr
            + "".join(f"{td}{CELLS[name].format(href=href)}</td>" for name in order)
            + "</tr>"
        )
    return (
        f"<b>({rows} records match)</b>"
        f'<table class="sortable" id="gamelist">{open_body}'
        f"<tr>{header}</tr>{body}{close_body}</table>"
    )

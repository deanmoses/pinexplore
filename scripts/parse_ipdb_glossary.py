#!/usr/bin/env python3
"""Parse the IPDB glossary HTML into structured JSON.

Reads the saved HTML from ingest_sources/glossary_ipdb/ and extracts each
glossary term with its definition, cross-references, and linked games.

Usage:
    python scripts/parse_ipdb_glossary.py [--src FILE] [--dest FILE]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

# Import slugify from the pindata sister project
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "pindata" / "lib"))
from slugify import slugify

DEFAULT_SRC = "ingest_sources/glossary_ipdb/Pinball Glossary.html"
DEFAULT_DEST = "ingest_sources/glossary_ipdb/ipdb_glossary.json"


class _GlossaryParser(HTMLParser):
    """State-machine parser that walks <dt>/<dd> pairs in the glossary."""

    def __init__(self) -> None:
        super().__init__()
        self.entries: list[dict] = []

        # State tracking
        self._in_dt = False
        self._in_dd = False
        self._current_anchor: str | None = None
        self._current_term: str | None = None
        self._dd_parts: list[str] = []
        self._dd_links: list[dict] = []  # pagelink cross-refs
        self._dd_games: list[dict] = []  # agamelink game refs

        # Link accumulation
        self._in_pagelink = False
        self._in_gamelink = False
        self._link_href: str | None = None
        self._link_text_parts: list[str] = []

    # -- helpers --

    def _flush_entry(self) -> None:
        if self._current_term is None:
            return
        definition = _clean_text("".join(self._dd_parts))
        # Deduplicate links by href
        seen_links: set[str] = set()
        xrefs: list[str] = []
        for lnk in self._dd_links:
            if lnk["slug"] not in seen_links:
                seen_links.add(lnk["slug"])
                xrefs.append(lnk["slug"])
        seen_games: set[int] = set()
        games = []
        for g in self._dd_games:
            if g["ipdb_id"] not in seen_games:
                seen_games.add(g["ipdb_id"])
                games.append(g)

        entry: dict = {
            "slug": slugify((self._current_anchor or "").replace("_", " ")),
            "name": self._current_term,
            "definition": definition,
        }
        if xrefs:
            entry["see_also"] = xrefs
        if games:
            entry["games"] = games
        self.entries.append(entry)

        # Reset
        self._current_anchor = None
        self._current_term = None
        self._dd_parts = []
        self._dd_links = []
        self._dd_games = []

    # -- HTMLParser callbacks --

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = dict(attrs)
        if tag == "dt":
            # A new <dt> after a <dd> means the previous entry is complete
            if self._in_dd:
                self._in_dd = False
                self._flush_entry()
            self._in_dt = True
        elif tag == "dd":
            self._in_dt = False
            self._in_dd = True
        elif tag == "a" and self._in_dt and "name" in attr:
            self._current_anchor = attr["name"]
        elif tag == "a" and self._in_dd:
            cls = attr.get("class", "")
            href = attr.get("href", "")
            if "pagelink" in cls:
                self._in_pagelink = True
                self._link_href = href.lstrip("#")
                self._link_text_parts = []
            elif "agamelink" in cls:
                self._in_gamelink = True
                self._link_href = href
                self._link_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "dt":
            self._in_dt = False
        elif tag == "dd":
            self._in_dd = False
            self._flush_entry()
        elif tag == "a":
            if self._in_pagelink:
                text = _clean_text("".join(self._link_text_parts))
                if self._link_href and text:
                    self._dd_links.append({"slug": slugify(self._link_href.replace("_", " "))})
                self._in_pagelink = False
            elif self._in_gamelink:
                text = _clean_text("".join(self._link_text_parts))
                if self._link_href and text:
                    ipdb_id = _extract_ipdb_id(self._link_href)
                    if ipdb_id is not None:
                        game = _parse_game_ref(ipdb_id, text)
                        self._dd_games.append(game)
                self._in_gamelink = False

    def handle_data(self, data: str) -> None:
        if self._in_dt and self._current_anchor:
            # Capture term name from the anchor text inside <dt>
            stripped = data.strip().rstrip("—:").strip()
            if stripped and self._current_term is None:
                self._current_term = stripped
        if self._in_dd:
            self._dd_parts.append(data)
            if self._in_pagelink or self._in_gamelink:
                self._link_text_parts.append(data)

    def handle_entityref(self, name: str) -> None:
        char = {"amp": "&", "lt": "<", "gt": ">", "nbsp": " ", "quot": '"'}.get(
            name, f"&{name};"
        )
        if self._in_dd:
            self._dd_parts.append(char)
            if self._in_pagelink or self._in_gamelink:
                self._link_text_parts.append(char)

    def handle_charref(self, name: str) -> None:
        try:
            char = chr(int(name, 16) if name.startswith("x") else int(name))
        except ValueError:
            char = f"&#{name};"
        if self._in_dd:
            self._dd_parts.append(char)
            if self._in_pagelink or self._in_gamelink:
                self._link_text_parts.append(char)


def _normalize_quotes(s: str) -> str:
    """Replace curly quotes with straight quotes."""
    return s.replace("\u2018", "'").replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"')


# Patterns for game reference strings, tried in order.
# 1. Manufacturer's Year 'Title'
# 2. Manufacturer's 'Title' (no year)
# 3. Year 'Title' (no manufacturer)
# 4. Bare title (no quotes, no manufacturer, no year)
_GAME_PATTERNS = [
    re.compile(r"^(.+?)'s?\s+(\d{4})\s+'(.+)'$"),
    re.compile(r"^(.+?)'s?\s+'(.+)'$"),
    re.compile(r"^(\d{4})\s+'(.+)'$"),
]


def _parse_game_ref(ipdb_id: int, raw_name: str) -> dict:
    """Parse a game reference string into structured fields.

    Returns a dict with ipdb_id and whichever of manufacturer, year,
    title can be extracted.
    """
    name = _normalize_quotes(raw_name)
    result: dict | None = None
    for i, pat in enumerate(_GAME_PATTERNS):
        m = pat.match(name)
        if not m:
            continue
        if i == 0:  # Mfr's Year 'Title'
            mfr, year_s, title = m.group(1), m.group(2), m.group(3)
            result = {"ipdb_id": ipdb_id, "manufacturer": mfr, "year": int(year_s), "title": title}
        elif i == 1:  # Mfr's 'Title'
            mfr, title = m.group(1), m.group(2)
            result = {"ipdb_id": ipdb_id, "manufacturer": mfr, "title": title}
        elif i == 2:  # Year 'Title'
            year_s, title = m.group(1), m.group(2)
            result = {"ipdb_id": ipdb_id, "year": int(year_s), "title": title}
        break

    if result is None:
        # Bare title — strip wrapping quotes if present
        title = name.strip("'\"").strip()
        result = {"ipdb_id": ipdb_id, "title": title}

    # Clean any residual quote artifacts from all string fields
    for key in ("title", "manufacturer"):
        if key in result:
            result[key] = result[key].strip("'\"").strip()

    _validate_game_ref(result, raw_name)
    return result


# Known manufacturers from the IPDB glossary game references.
_KNOWN_MANUFACTURERS = {
    "Ad-Lee Company", "Atari", "Automaticos", "Bally", "Bally Midway",
    "Bensa", "C. F. Eckhart & Company", "Capcom", "Chicago Coin",
    "Como Manufacturing Corp.", "Exhibit", "Genco", "Gottlieb",
    "H. P. Schafer", "Homepin Ltd", "Interflip", "Keeney", "Midway",
    "Pacent", "Playmatic", "Premier", "Rally", "Recel", "Rock-ola",
    "SLEIC", "Sega", "Sega Enterprises", "Smith Manufacturing Company",
    "Soc. Elettrogiochi", "Stern", "Stoner", "United",
    "Unknown Manufacturer", "Williams", "Zaccaria",
}


def _validate_game_ref(ref: dict, raw_name: str) -> None:
    """Validate a parsed game reference. Raises ValueError on problems."""
    # Title must be non-empty
    title = ref.get("title", "")
    if not title or not title.strip():
        raise ValueError(f"Game ref has empty title: ipdb_id={ref['ipdb_id']} raw={raw_name!r}")

    # Year must be a plausible pinball year
    if "year" in ref:
        year = ref["year"]
        if not (1900 <= year <= 2030):
            raise ValueError(
                f"Game ref has implausible year {year}: ipdb_id={ref['ipdb_id']} raw={raw_name!r}"
            )

    # Manufacturer must be in the known set
    if "manufacturer" in ref:
        mfr = ref["manufacturer"]
        if mfr not in _KNOWN_MANUFACTURERS:
            raise ValueError(
                f"Game ref has unknown manufacturer {mfr!r}: ipdb_id={ref['ipdb_id']} raw={raw_name!r}"
            )


def _extract_ipdb_id(url: str) -> int | None:
    """Extract the numeric IPDB machine ID from a URL like '...?id=1234'."""
    m = re.search(r"[?&]id=(\d+)", url)
    return int(m.group(1)) if m else None


def _clean_text(text: str) -> str:
    """Collapse whitespace and strip a cleaned-up plain-text string."""
    text = text.replace("\xa0", " ")  # nbsp
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _backfill_see_also(entries: list[dict]) -> None:
    """Ensure 'See X' definitions have the target in see_also.

    Some "See X" entries lack a pagelink in the HTML, so see_also is
    empty. This fills it in by slugifying the referenced term.
    """
    by_slug = {e["slug"] for e in entries}
    for e in entries:
        d = e.get("definition", "")
        if not d.startswith("See "):
            continue
        existing = set(e.get("see_also", []))
        # Extract all "See Term" references from the definition
        for m in re.finditer(r"See\s+(?:longer explanation under\s+)?['\"]?([^.'\"\u201c\u201d]+)", d):
            ref = m.group(1).strip()
            candidate = slugify(ref.replace("_", " "))
            if candidate in by_slug and candidate not in existing:
                e.setdefault("see_also", []).append(candidate)
                existing.add(candidate)


def _extract_also_called(entries: list[dict]) -> None:
    """Extract 'Also called X' aliases from definitions."""
    for e in entries:
        for m in re.finditer(
            r"[Aa]lso called (?:a |the |an )?"
            r'["\u201c]?([A-Z][^."\u201d]+?)["\u201d.]',
            e.get("definition", ""),
        ):
            alias = m.group(1).strip().rstrip(",")
            if "," in alias or len(alias) > 40:
                continue
            e.setdefault("aliases", []).append(alias)

    # Deduplicate (case-insensitive) and sort aliases
    for e in entries:
        if "aliases" in e:
            seen: dict[str, str] = {}
            for a in e["aliases"]:
                key = a.lower()
                if key not in seen:
                    seen[key] = a
            e["aliases"] = sorted(seen.values())


def parse_glossary(src: Path) -> list[dict]:
    """Parse the glossary HTML and return a list of term dicts."""
    html = src.read_text(encoding="utf-8", errors="replace")
    parser = _GlossaryParser()
    parser.feed(html)
    # Flush any trailing entry
    parser._flush_entry()
    entries = parser.entries
    _backfill_see_also(entries)
    _extract_also_called(entries)
    return entries


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse IPDB glossary HTML to JSON")
    ap.add_argument("--src", default=DEFAULT_SRC, help="Source HTML file")
    ap.add_argument("--dest", default=DEFAULT_DEST, help="Output JSON file")
    args = ap.parse_args()

    src = Path(args.src)
    if not src.exists():
        print(f"Source file not found: {src}", file=sys.stderr)
        sys.exit(1)

    entries = parse_glossary(src)
    dest = Path(args.dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        json.dumps(entries, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(entries)} glossary entries to {dest}")


if __name__ == "__main__":
    main()

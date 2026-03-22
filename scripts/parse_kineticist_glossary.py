#!/usr/bin/env python3
"""Parse the Kineticist pinball glossary HTML into structured JSON.

Reads the saved HTML from ingest_sources/glossary_kineticist/ and extracts
each glossary term with its definition.

Usage:
    python scripts/parse_kineticist_glossary.py [--src FILE] [--dest FILE]
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

DEFAULT_SRC = "ingest_sources/glossary_kineticist/glossary_kineticist"
DEFAULT_DEST = "ingest_sources/glossary_kineticist/kineticist_glossary.json"


class _GlossaryParser(HTMLParser):
    """State-machine parser that walks <h2>/<p> pairs in the glossary."""

    def __init__(self) -> None:
        super().__init__()
        self.entries: list[dict] = []

        # State tracking
        self._in_h2 = False
        self._in_p = False
        self._current_slug: str | None = None
        self._current_name_parts: list[str] = []
        self._p_parts: list[str] = []
        self._p_depth = 0  # track nested tags inside <p>

    # -- helpers --

    def _flush_entry(self) -> None:
        if self._current_slug is None:
            return
        raw_name = _clean_text("".join(self._current_name_parts))
        definition = _clean_text("".join(self._p_parts))
        if not raw_name or not definition:
            return
        slug_name, alias = _extract_alias(raw_name)
        entry: dict = {
            "slug": slugify(slug_name),
            "name": raw_name,
            "definition": definition,
        }
        if alias:
            entry["alias"] = alias
        self.entries.append(entry)
        self._current_slug = None
        self._current_name_parts = []
        self._p_parts = []

    # -- HTMLParser callbacks --

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = dict(attrs)
        if tag == "h2" and "id" in attr:
            # New term — flush any previous entry
            self._flush_entry()
            self._current_slug = attr["id"]
            self._in_h2 = True
            self._current_name_parts = []
        elif tag == "p" and self._current_slug is not None:
            if not self._in_p:
                self._in_p = True
                self._p_depth = 0
            else:
                self._p_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag == "h2":
            self._in_h2 = False
        elif tag == "p" and self._in_p:
            if self._p_depth > 0:
                self._p_depth -= 1
            else:
                self._in_p = False

    def handle_data(self, data: str) -> None:
        if self._in_h2:
            self._current_name_parts.append(data)
        elif self._in_p and self._current_slug is not None:
            self._p_parts.append(data)

    def handle_entityref(self, name: str) -> None:
        char = {"amp": "&", "lt": "<", "gt": ">", "nbsp": " ", "quot": '"'}.get(
            name, f"&{name};"
        )
        if self._in_p and self._current_slug is not None:
            self._p_parts.append(char)

    def handle_charref(self, name: str) -> None:
        try:
            char = chr(int(name, 16) if name.startswith("x") else int(name))
        except ValueError:
            char = f"&#{name};"
        if self._in_p and self._current_slug is not None:
            self._p_parts.append(char)


def _extract_alias(name: str) -> tuple[str, str | None]:
    """Extract parenthetical alias from a name.

    Returns (slug_name, alias) where slug_name has the parenthetical
    stripped (for slugification) and alias is the parenthetical content.

    Examples:
        'Solid State (SS)' -> ('Solid State', 'SS')
        'Ruleset (or rule set)' -> ('Ruleset', 'rule set')
    """
    m = re.search(r"\(([^)]+)\)", name)
    if not m:
        return name, None
    alias = m.group(1)
    # Strip leading "or " prefix
    alias = re.sub(r"^or\s+", "", alias)
    slug_name = (name[: m.start()] + name[m.end() :]).strip()
    slug_name = re.sub(r"\s+", " ", slug_name)
    return slug_name, alias


def _clean_text(text: str) -> str:
    """Collapse whitespace and strip a cleaned-up plain-text string."""
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_glossary(src: Path) -> list[dict]:
    """Parse the glossary HTML and return a list of term dicts."""
    html = src.read_text(encoding="utf-8", errors="replace")
    parser = _GlossaryParser()
    parser.feed(html)
    parser._flush_entry()
    return parser.entries


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Parse Kineticist glossary HTML to JSON"
    )
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

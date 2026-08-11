"""Check `ref_document_class_pattern`'s needles against its patterns.

`ipdb_file_class_matches` pairs a filename with a pattern only when one of that
pattern's `required_any` literals appears in the name, then runs the real regex
on what survives. That makes `required_any` a necessary condition, and an
under-narrow one silently drops matches: the build stays green and the class
just stops appearing.

The realistic way to get it wrong is to add or widen a top-level branch and
forget its needle. This finds that, offline, without the corpus: every
alternative a pattern can match must be covered by some needle.

It is a lint, not a proof. It reads the pattern as written rather than as RE2
would compile it, so a needle can still be too narrow *within* a branch — most
plausibly around an optional quantifier, where `rule ?sheet` matches both
"rule sheet" and "rulesheet" but only the first is visible here. Cover those by
declaring a needle per spelling, as that pattern does.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import pytest

PATTERN_VIEW = "ref_document_class_pattern"
REFERENCE_SQL = Path(__file__).resolve().parents[1] / "sql" / "01_reference.sql"


def _load_patterns() -> list[tuple[str, str, list[str]]]:
    """Return (document_class, pattern, required_any) straight from the SQL.

    The view is a bare VALUES list with no table dependencies, so it can be
    created in an empty in-memory database. Only that one statement is run —
    its neighbours in the file are not all self-contained.
    """
    con = duckdb.connect()
    statements = con.extract_statements(REFERENCE_SQL.read_text(encoding="utf-8"))
    for statement in statements:
        if PATTERN_VIEW in statement.query:
            con.execute(statement.query)
            break
    else:  # pragma: no cover - only reachable if the view is renamed
        pytest.fail(f"{PATTERN_VIEW} not found in {REFERENCE_SQL}")
    return con.execute(
        "SELECT document_class, pattern, required_any FROM ref_document_class_pattern"
    ).fetchall()


def _top_level_branches(pattern: str) -> list[str]:
    """Split on `|` at nesting depth zero, leaving grouped alternations intact.

    `parts (list|catalog)|part list` is two alternatives, not three: the first
    `|` is inside a group and describes how one branch varies.
    """
    branches: list[str] = []
    depth = 0
    current: list[str] = []
    escaped = False
    for char in pattern:
        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            current.append(char)
            escaped = True
        elif char == "[":
            depth += 1
            current.append(char)
        elif char == "]":
            depth -= 1
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "|" and depth == 0:
            branches.append("".join(current))
            current = []
        else:
            current.append(char)
    branches.append("".join(current))
    return branches


def _literal_form(branch: str) -> str:
    """A branch reduced to the text a needle can be looked for in.

    Word boundaries and quantifiers carry no characters of their own, so
    dropping them turns `\\brule ?sheet\\b` into `rule sheet` — one of the two
    strings it matches, which is enough to tell whether a needle is aimed at
    this branch at all.
    """
    return re.sub(r"\\b|[?*+]", "", branch)


PATTERNS = _load_patterns()


@pytest.mark.parametrize(
    ("document_class", "pattern", "needles"),
    PATTERNS,
    ids=[f"{cls}:{pat}" for cls, pat, _ in PATTERNS],
)
def test_every_branch_has_a_needle(
    document_class: str, pattern: str, needles: list[str]
) -> None:
    assert needles, f"{document_class} declares no required_any"
    for branch in _top_level_branches(pattern):
        literal = _literal_form(branch)
        assert any(needle in literal for needle in needles), (
            f"{document_class}: branch {branch!r} is matched by no needle in "
            f"{needles}. Anything matching only this branch would be dropped "
            f"before the regex ever runs."
        )


@pytest.mark.parametrize(
    ("document_class", "needles"),
    [(cls, needles) for cls, _, needles in PATTERNS],
    ids=[cls for cls, _, _ in PATTERNS],
)
def test_needles_are_lowercase(document_class: str, needles: list[str]) -> None:
    # The match is against lower(file_name), so an uppercase needle never fires.
    for needle in needles:
        assert needle == needle.lower(), (
            f"{document_class}: {needle!r} is not lowercase"
        )

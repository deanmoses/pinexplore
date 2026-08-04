#!/usr/bin/env python3
"""HTML content handler for the web evidence cache (see docs/WebCache.md).

Everything HTML-specific lives here: resolving the charset of the fetched bytes
(the headerless Shift-JIS case old Japanese pages hit), converting the whole
document body to block-level markdown with markdownify, carrying an allowlist of
``<head>`` metadata into the text, and extracting a conservative date. The
transport and the fetcher reach this only through the ``ContentHandler``
interface — they never mention HTML.

The stored ``text`` is the *whole* document — footers, nav, comments, dropdown
indexes — assembled as YAML-style frontmatter (title + head tags, one
``key: value`` line each between ``---`` delimiters) followed by the page as
markdown, headings at their source levels. Main-content judgment is
deliberately absent: what a boilerplate filter discards is often the evidence
sought (a footer street address, a correction in the comments), and noise in a
recall corpus is far cheaper than a silent gap. The structure exists so a
citation can say where in the page a quote lives ("in the og:description meta
tag", "in the Specifications section") — including places a reader can't see.
"""

from __future__ import annotations

import functools
import re
from typing import TYPE_CHECKING, override

from .base import ContentHandler, ExtractedMeta

if TYPE_CHECKING:
    from bs4 import Tag
    from lxml.html import HtmlElement
    from markdownify import MarkdownConverter

# --------------------------------------------------------------------------- #
# Charset decoding
# --------------------------------------------------------------------------- #

# A page can declare its charset in an HTML <meta> tag two ways: the HTML5
# ``<meta charset="...">`` and the legacy
# ``<meta http-equiv="Content-Type" content="text/html; charset=...">``. Both put
# the value right after a ``charset=``, so one pattern catches them.
_META_CHARSET_RE = re.compile(
    rb"""<meta[^>]+?charset\s*=\s*["']?\s*([a-zA-Z0-9_.:-]+)""",
    re.IGNORECASE,
)

# Windows-authored Japanese pages routinely declare ``Shift_JIS`` but actually use
# cp932 (its superset, with NEC/IBM extension characters like ①, ㈱). Python's
# strict ``shift_jis`` codec mangles those extension bytes, so decode the whole
# family as cp932 — it round-trips genuine Shift_JIS unchanged.
_CHARSET_ALIASES = {
    "shift_jis": "cp932",
    "shift-jis": "cp932",
    "shiftjis": "cp932",
    "sjis": "cp932",
    "x-sjis": "cp932",
}


def _sniff_meta_charset(raw: bytes) -> str | None:
    """Return the charset an HTML page declares in a ``<meta>`` tag, or None.

    Per the HTML spec the declaration must appear in the first 1024 bytes, so we
    only scan that prefix (and bound a stray match deeper in the body). Matches
    both ``<meta charset="shift_jis">`` and the legacy
    ``<meta http-equiv="Content-Type" content="text/html; charset=Shift_JIS">``.
    """
    match = _META_CHARSET_RE.search(raw[:1024])
    if match is None:
        return None
    return match.group(1).decode("ascii", errors="replace")


def _detect_charset(raw: bytes) -> str | None:
    """Statistically detect the charset of undeclared bytes, or None.

    The last resort when neither the HTTP header nor the HTML declares a charset
    (common for old Japanese pages served as Shift-JIS). charset-normalizer is a
    direct dependency.
    """
    from charset_normalizer import from_bytes

    best = from_bytes(raw).best()
    return best.encoding if best is not None else None


def _try_decode(raw: bytes, label: str | None) -> str | None:
    """Decode ``raw`` using charset ``label``, or None if the label is empty or
    unknown to Python's codecs (a junk ``charset=`` shouldn't raise and lose the
    page). The Shift_JIS family is upgraded to its cp932 superset first."""
    if not label:
        return None
    codec = _CHARSET_ALIASES.get(label.strip().lower(), label)
    try:
        return raw.decode(codec, errors="replace")
    except LookupError:
        return None


def _decode_body(raw: bytes, header_charset: str | None) -> str:
    """Decode response bytes to text, resolving the charset in priority order:

    1. the HTTP ``Content-Type`` charset, when the server sent one (authoritative);
    2. a ``<meta>`` charset the HTML declares about itself;
    3. charset-normalizer's statistical detection;
    4. utf-8, as a last resort.

    The motivating bug: old Japanese pages served as Shift-JIS/cp932 with *no*
    charset header — a blind utf-8 decode turned their titles to mojibake. An
    unknown/junk label (e.g. a bogus ``charset=utf-8x-bogus``, which would raise
    ``LookupError`` and escape ``fetch_one``'s except tuple) is skipped rather than
    allowed to lose the page; ``errors="replace"`` throughout so undecodable bytes
    never raise either.
    """
    # Header then <meta>; only if neither yields a usable label do we run the
    # (relatively costly) statistical detection.
    for label in (header_charset, _sniff_meta_charset(raw)):
        decoded = _try_decode(raw, label)
        if decoded is not None:
            return decoded
    detected = _try_decode(raw, _detect_charset(raw))
    return detected if detected is not None else raw.decode("utf-8", errors="replace")


# --------------------------------------------------------------------------- #
# Pre-parse string cleanup
# --------------------------------------------------------------------------- #


# A closed googleoff/googleon pair is the page's own machine-readable "this is
# not content" marker (cookie banners, ad blocks). The conversion below keeps
# the whole document, so a page's own opt-out is the one exclusion it still
# gets to request — and a comment-delimited *range* cannot be expressed as the
# container-token matching the consent strip uses, so it stays a regex on the
# decoded string. Unclosed googleoff markers are left alone (stripping to EOF
# on a malformed page would be worse than a noisy extraction).
_GOOGLEOFF_RE = re.compile(
    r"<!--\s*googleoff:\s*(?:all|index|snippet|anchor)\s*-->.*?"
    r"<!--\s*googleon:\s*(?:all|index|snippet|anchor)\s*-->",
    re.DOTALL | re.IGNORECASE,
)

# lxml refuses str input that carries an XML encoding declaration ("Unicode
# strings with encoding declaration are not supported") — some XHTML pages ship
# one. The bytes are already decoded, so the declaration is spent; drop it.
_XML_DECL_RE = re.compile(r"^\s*<\?xml[^>]*\?>")


# --------------------------------------------------------------------------- #
# Consent-widget stripping
# --------------------------------------------------------------------------- #

# Cookie banners come from a small number of consent platforms with stable
# container classes/ids. Their text is unambiguous junk ("This website uses
# cookies… Accept Rej…") that would otherwise pollute quotes and FTS on dozens
# of pages, and unlike nav or footers it can never be evidence. Matching is on
# class *tokens* (whole token, or token starting with ``<platform>-``/``_``) and
# id *prefixes* — never raw substring, since a substring ``cky`` would match
# ``sticky-header``. The upkeep cost is one entry per new platform; the risk is
# a false positive taking real content, which is exactly why the match stays
# this narrow.
_CONSENT_PLATFORMS = (
    # Observed in the corpus:
    "cookie-law-info",
    "cmplz",
    "complianz",
    "onetrust",
    "cookie-notice",
    "cookie-consent",
    "cookie-banner",
    "cookiebar",
    "cky",
    "iubenda-cs",
    # The common remainder, not yet seen here:
    "cybotcookiebotdialog",
    "didomi",
    "osano",
    "qc-cmp2",
    "borlabs-cookie",
    "usercentrics",
    "termsfeed",
    "moove_gdpr",
    "cookieconsent",
    "cc-window",
    "trustarc",
    "klaro",
    "tarteaucitron",
)


def _is_consent_element(el: HtmlElement) -> bool:
    """True when an element's class tokens or id mark a known consent platform."""
    class_tokens = [t.lower() for t in (el.get("class") or "").split()]
    el_id = (el.get("id") or "").lower()
    for platform in _CONSENT_PLATFORMS:
        if el_id.startswith(platform):
            return True
        for token in class_tokens:
            if token == platform or token.startswith((platform + "-", platform + "_")):
                return True
    return False


def _strip_consent_widgets(tree: HtmlElement) -> None:
    """Drop the outermost element of every recognized consent widget, in place.

    ``drop_tree`` (not ``remove``) so an element's tail text — real content that
    happens to follow the banner in the markup — survives the drop.
    """
    matched = [
        el for el in tree.iter() if isinstance(el.tag, str) and _is_consent_element(el)
    ]
    matched_set = set(matched)
    for el in matched:
        if any(anc in matched_set for anc in el.iterancestors()):
            continue  # an ancestor is already being dropped, and el with it
        el.drop_tree()


# --------------------------------------------------------------------------- #
# Head metadata
# --------------------------------------------------------------------------- #

# The <head> tags carried into the text, in emission order — which is also the
# dedup preference order, most general key first (description before
# og:description before twitter:description; og:title before twitter:title).
# The order matters because 124 pages carry an identical description under 2-3
# keys and the surviving key is what a citation's locator names, so it must not
# depend on which plugin emitted its tag first. Excluded on purpose: viewport,
# generator, theme-color, robots, og:image*/og:url/og:type, twitter:card,
# dimensions — junk on more pages than they help.
_META_ALLOWLIST = (
    "description",
    "og:description",
    "twitter:description",
    "keywords",
    "news_keywords",
    "author",
    "og:title",
    "twitter:title",
    "og:site_name",
    "article:published_time",
    "article:modified_time",
    "article:tag",
    "article:section",
    "article:author",
)


def _collapse_ws(value: str) -> str:
    """Collapse all whitespace runs to single spaces and trim."""
    return " ".join(value.split())


def _head_metadata(tree: HtmlElement) -> tuple[str | None, str | None, list[str]]:
    """Extract the metadata block's lines from a parsed document.

    Returns ``(title_tag, og_title, lines)``: the ``<title>`` text, the first
    ``og:title`` value (both for the title fallback chain), and the
    ``key: value`` lines to emit. One string is emitted once however many tags
    carry it: values are deduped case-insensitively across the whole allowlist,
    with ``title`` most general of all, then allowlist order. Genuinely
    differing values all appear — a page can legitimately carry both a
    ``description:`` and an ``og:description:`` line.
    """
    title_el = tree.find(".//title")
    title_tag = _collapse_ws(title_el.text_content()) if title_el is not None else None

    by_key: dict[str, list[str]] = {}
    for meta in tree.iterfind(".//meta"):
        key = (meta.get("property") or meta.get("name") or "").strip().lower()
        content = _collapse_ws(meta.get("content") or "")
        if key in _META_ALLOWLIST and content:
            by_key.setdefault(key, []).append(content)

    og_values = by_key.get("og:title")
    og_title = og_values[0] if og_values else None

    lines: list[str] = []
    seen: set[str] = set()
    if title_tag:
        lines.append(f"title: {title_tag}")
        seen.add(title_tag.casefold())
    for key in _META_ALLOWLIST:
        for value in by_key.get(key, []):
            if value.casefold() in seen:
                continue
            seen.add(value.casefold())
            lines.append(f"{key}: {value}")
    return title_tag, og_title, lines


# --------------------------------------------------------------------------- #
# Body → markdown
# --------------------------------------------------------------------------- #


@functools.cache
def _converter() -> MarkdownConverter:
    """The configured markdownify converter (built once; handlers are stateless).

    Every option is passed explicitly — never inherited — so a dependency bump
    cannot silently change ``<pre>`` handling or document stripping through a
    shifted default. Verified against the pinned markdownify==1.2.3, whose
    ``DefaultOptions`` surface matches this enumeration exactly.
    """
    import markdownify

    # markdownify's bundled stub types the class but omits every convert_<tag>
    # method, so reaching convert_div for the aliases below needs one ignore.
    _convert_div = markdownify.MarkdownConverter.convert_div  # type: ignore[attr-defined]

    class _EvidenceConverter(markdownify.MarkdownConverter):
        """markdownify tuned for verbatim evidence text.

        ``convert_img`` returns the alt text directly (an image's only quotable
        content) — an override, not an ``![alt](url)`` post-processing regex,
        which alt text containing ``]`` or a URL containing ``)`` defeats.

        markdownify applies block separation only through an active
        ``convert_<tag>`` method; a tag without one passes its text through
        *inline*, which on minified HTML (no source newlines) fuses adjacent
        blocks into run-ons (``Multimorphic, Inc.2109 Couch Dr``). So the block
        containers markdownify has no converter for are aliased to
        ``convert_div``, and ``<option>`` — which has none at all — gets its
        own: each option on its own line, so a minified ``<select>`` holding a
        manufacturer index stays a list instead of fusing. Inline fusion
        (``<span>a</span><span>b</span>`` → ``ab``) is left alone: it matches
        browser rendering, so it is faithful.
        """

        def convert_img(self, el: Tag, text: str, parent_tags: set[str]) -> str:
            return str(el.get("alt") or "")

        def convert_option(self, el: Tag, text: str, parent_tags: set[str]) -> str:
            stripped = text.strip()
            return f"\n{stripped}\n" if stripped else ""

        convert_address = _convert_div
        convert_aside = _convert_div
        convert_details = _convert_div
        convert_figure = _convert_div
        convert_footer = _convert_div
        convert_header = _convert_div
        convert_main = _convert_div
        convert_nav = _convert_div
        convert_summary = _convert_div

    return _EvidenceConverter(
        # Block-level only. Inline tags (a, b, strong, i, em, code) are excluded
        # on purpose: their markers land *inside* sentences where they poison
        # quotable spans, and <a> alone would inflate the corpus ~70% with nav
        # URLs. Block markers sit between spans, invisible to quoting.
        convert=[
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "h6",
            "p",
            "br",
            "hr",
            "blockquote",
            "pre",
            "ul",
            "ol",
            "li",
            "table",
            "thead",
            "tbody",
            "tr",
            "td",
            "th",
            "img",
            "div",
            "section",
            "article",
            "dl",
            "dt",
            "dd",
            "figcaption",
            "caption",
            "option",
            "footer",
            "header",
            "nav",
            "aside",
            "main",
            "address",
            "figure",
            "details",
            "summary",
        ],
        # Same parser as the lxml strip pass, not bs4's html.parser default, so
        # both passes see the same tree.
        bs4_options="lxml",
        heading_style="atx",
        # ~9 in 10 cached tables have no header row markdownify recognizes; at
        # the False default it emits a phantom empty header and demotes the real
        # first row below the separator. True promotes the first row, which on
        # these machine-list tables is the actual header. The cost — a genuinely
        # data-first table gets its row labelled a header — loses no text.
        table_infer_header=True,
        # No escaping: this text is quoted verbatim, not re-rendered.
        escape_asterisks=False,
        escape_underscores=False,
        escape_misc=False,
        bullets="-",
        autolinks=False,
        newline_style="spaces",
        # The rest is markdownify 1.2.3's defaults, restated so they are pinned.
        strip=None,
        strip_document="strip",
        strip_pre="strip",
        wrap=False,
        wrap_width=80,
        keep_inline_images_in=[],
        code_language="",
        code_language_callback=None,
        strong_em_symbol="*",
        sub_symbol="",
        sup_symbol="",
        default_title=False,
    )


def _postprocess(markdown: str) -> str:
    """Minimal cleanup: strip trailing whitespace per line, cap blank runs.

    Deliberately nothing more. markdownify already collapses whitespace runs in
    prose while preserving what indentation *means* — the leading spaces in
    ``<pre>`` fences, the two-space indents of nested lists — so a global
    ``[ \\t]+`` collapse or leading strip would be redundant for prose and
    destructive for structure. The trailing strip does erase
    ``newline_style="spaces"``'s two-space ``<br>`` markers, deliberately: the
    line break itself survives, and markdown *rendering* semantics are not what
    this text is for. No adjacent-duplicate-line dedup either: measured, it
    removes 0.9% of lines and mostly real content (non-adjacent nav copies never
    fire), an arbitrary exception to keeping the document verbatim.
    """
    markdown = "\n".join(line.rstrip() for line in markdown.split("\n"))
    return re.sub(r"\n{3,}", "\n\n", markdown).strip()


# --------------------------------------------------------------------------- #
# Extraction
# --------------------------------------------------------------------------- #


def _parse_tree(html: str) -> HtmlElement | None:
    """Parse to an lxml tree with script/style/svg/etc. and consent widgets
    stripped, or None when the document is empty/unparseable.

    The strip list removes what must never reach the text: code and styling
    (``script``/``style``), drawings whose internal ``<title>/<desc>/<text>``
    would leak (``svg`` — lxml's HTML parser doesn't namespace foreign content,
    so the bare tag also catches ``<svg xmlns=…>``), and fallback text of
    embeds (``iframe``/``canvas``). ``with_tail=False`` keeps each stripped
    element's tail — real content that follows it in the markup. HTML comments
    are markdownify's job (it skips them); if that ever needs to be ours,
    ``etree.strip_elements(tree, etree.Comment)`` makes it so.
    """
    import lxml.html
    from lxml import etree

    html = _XML_DECL_RE.sub("", html)
    if not html.strip():
        return None
    try:
        tree = lxml.html.document_fromstring(html)
    except etree.ParserError, ValueError:
        return None
    etree.strip_elements(
        tree, "script", "style", "svg", "iframe", "canvas", with_tail=False
    )
    _strip_consent_widgets(tree)
    return tree


def _extract_html(html: str) -> ExtractedMeta:
    """Whole-document extraction: frontmatter + markdown body + a real date.

    ``text`` is a YAML-style frontmatter block (title + head allowlist as
    ``key: value`` lines between ``---`` delimiters) followed by the ``<body>``
    subtree as markdown. Frontmatter rather than a labeled pseudo-section keeps
    the document *well-formed*: page headings stay at their source levels (an
    ``<h1>`` is ``#``) with nothing above them to outrank, so any markdown
    reader — not just this repo's helpers — parses the page's real outline.
    Only the body subtree is converted — never the whole document — because
    markdownify treats ``<title>`` as text-bearing, which would emit the
    title's text twice under two locators; its one home is the frontmatter's
    ``title:`` line. The delimiters are always emitted, even when no metadata
    was found, so the shape is positional and constant: ``---`` on line 1,
    closed by the next ``---`` line.

    The stored ``title`` column is ``og:title`` → ``<title>`` → first ``<h1>``,
    verbatim — no site-suffix stripping, because no separator heuristic can tell
    ``… | Pinside.com`` from ``Sirmo : Magic Screen``, where the separator joins
    two halves of one real title.

    ``last_updated`` is a real date stated on the page, or None — never a guess.
    htmldate with ``extensive_search=False`` returns None rather than pad a
    weak year-only signal (a stray "© 2024") up to a fabricated ``YYYY-01-01``,
    which would corrupt the "is this still live" judgment. ``original_date`` is
    left False (htmldate's default) so we get the page's most recent date,
    matching the ``last_updated`` column name and the recency check.
    """
    import htmldate
    import lxml.html

    html = _GOOGLEOFF_RE.sub("", html)
    try:
        last_updated = htmldate.find_date(html, extensive_search=False)
    except Exception:
        last_updated = None

    tree = _parse_tree(html)
    if tree is None:
        return ExtractedMeta(
            title=None, last_updated=last_updated, text=None, body_text=None
        )

    title_tag, og_title, meta_lines = _head_metadata(tree)

    body = tree.find("body")
    subtree = body if body is not None else tree
    body_md = _postprocess(
        _converter().convert(lxml.html.tostring(subtree, encoding="unicode"))
    )

    first_h1 = tree.find(".//h1")
    title = (
        og_title
        or title_tag
        or (_collapse_ws(first_h1.text_content()) if first_h1 is not None else None)
        or None
    )

    text = "\n".join(["---", *meta_lines, "---", "", body_md]).strip()

    return ExtractedMeta(
        title=title, last_updated=last_updated, text=text, body_text=body_md
    )


class HtmlHandler(ContentHandler):
    """Web pages: charset-decoded, converted whole to markdown, render-eligible.

    Has no magic signature (HTML is recognized by its content type, not a fixed
    prefix) and is the only renderable type — a thin extraction means a JS-only
    page a headless render may rescue. Thinness is judged on ``body_text``, the
    body alone: a JS-only page ships rich ``og:`` tags precisely because
    crawlers don't run JS, so the assembled ``text`` reads fat where the page
    is empty.
    """

    mime_types = frozenset({"text/html", "application/xhtml+xml"})
    canonical_mime = "text/html"
    signature: bytes | None = None
    extension = "html"
    text_source = "html"
    renderable = True

    @override
    def decode(self, raw: bytes, header_charset: str | None) -> str | None:
        return _decode_body(raw, header_charset)

    @override
    def extract(self, raw: bytes, text: str | None, url: str) -> ExtractedMeta:
        assert text is not None  # HTML always carries decoded text (or a render's)
        return _extract_html(text)

    @override
    def thin_warning(
        self, url: str, *, rendered: bool, render_attempted: bool
    ) -> str | None:
        if rendered:
            return f"WARNING: still thin after render: {url}"
        if not render_attempted:
            # A render might rescue it; suggest it only when we didn't already try.
            # --force too, since this page is now fresh and would otherwise skip.
            return (
                f"WARNING: thin content, likely JS-only: {url} "
                "(retry with --force --render after "
                "`uv run playwright install chromium`)"
            )
        # A render was attempted and failed — render already logged why.
        return None

"""Tests for the content-type handler registry and its handlers (no network).

The registry routing (handler_for / sniff / the extractable + sniffable sets),
plus each handler's per-type behavior: the HTML handler's charset decoding and
conservative date extraction, the PDF handler's text/title/date pull, and
the image handler's OCR (the Vision backend itself lives in test_web_ocr).
"""

from __future__ import annotations

import content_types as ct
import web_ocr
from content_types.html import HtmlHandler, _sniff_meta_charset
from content_types.image import JpegHandler, PngHandler
from content_types.pdf import PdfHandler

# --------------------------------------------------------------------------- #
# Registry — routing by content type and by magic-byte signature
# --------------------------------------------------------------------------- #


def test_handler_for_routes_known_types():
    assert isinstance(ct.handler_for("text/html"), HtmlHandler)
    assert isinstance(ct.handler_for("application/xhtml+xml"), HtmlHandler)
    assert isinstance(ct.handler_for("application/pdf"), PdfHandler)
    assert isinstance(ct.handler_for("image/jpeg"), JpegHandler)
    assert isinstance(ct.handler_for("image/png"), PngHandler)


def test_handler_for_unknown_type_is_none():
    assert ct.handler_for("application/zip") is None


def test_extractable_set_is_the_union_of_handler_mime_types():
    assert {
        "text/html",
        "application/xhtml+xml",
        "application/pdf",
        "text/vtt",
        "image/jpeg",
        "image/jpg",
        "image/png",
    } == ct.EXTRACTABLE_CONTENT_TYPES


def test_sniff_matches_pdf_signature_whatever_the_header():
    # The %PDF- signature identifies a PDF served as octet-stream / mislabeled.
    assert isinstance(ct.sniff(b"%PDF-1.4\nstuff"), PdfHandler)


def test_sniff_matches_image_signatures():
    # JPEG (SOI + marker) and PNG magic, so an image handed over as
    # octet-stream — or with no content type at all, the manual-import case —
    # still routes to the right handler and blob extension.
    assert isinstance(ct.sniff(b"\xff\xd8\xff\xe0\x00\x10JFIF"), JpegHandler)
    assert isinstance(ct.sniff(b"\x89PNG\r\n\x1a\n\x00"), PngHandler)


def test_sniff_returns_none_for_non_signature_bytes():
    # HTML carries no signature; a genuine binary download matches nothing.
    assert ct.sniff(b"<html>hi</html>") is None
    assert ct.sniff(b"PK\x03\x04zip") is None


def test_pdf_handler_is_not_renderable_html_is():
    assert ct.handler_for("text/html").renderable is True
    assert ct.handler_for("application/pdf").renderable is False


def test_image_handler_is_not_renderable():
    # A browser can't pull text out of a JPEG either — OCR is the only path.
    assert ct.handler_for("image/jpeg").renderable is False


def test_handler_extensions():
    assert ct.handler_for("text/html").extension == "html"
    assert ct.handler_for("application/pdf").extension == "pdf"
    assert ct.handler_for("image/jpeg").extension == "jpg"
    assert ct.handler_for("image/png").extension == "png"


def test_extension_for_resolves_stored_content_types():
    # extension_for is the bridge from a pages row to its blob: a stored
    # content_type maps to the blob's extension; an unknown type returns None
    # rather than guessing (a row never holds one — http_get canonicalizes first).
    assert ct.extension_for("text/html") == "html"
    assert ct.extension_for("application/pdf") == "pdf"
    assert ct.extension_for("image/jpeg") == "jpg"
    assert ct.extension_for("application/octet-stream") is None


def test_image_alias_mime_canonicalizes_to_one_extension():
    # Servers do send image/jpg; it must not become a `.jpeg`/`.jpg` split, and
    # the canonical label a sniff stamps is the real one.
    assert ct.extension_for("image/jpg") == "jpg"
    assert ct.handler_for("image/jpg").canonical_mime == "image/jpeg"


# --------------------------------------------------------------------------- #
# text_source — how a row's text was derived (evidence-quality provenance)
# --------------------------------------------------------------------------- #


def test_each_handler_declares_how_its_text_was_derived():
    # Stored on the page row so a consumer can weigh a quote by its extraction
    # path. An image declares None: its words are machine-read, land in
    # ocr_text, and no text layer is derived — stamping a label would
    # assert otherwise.
    assert ct.handler_for("text/html").text_source == "html"
    assert ct.handler_for("application/pdf").text_source == "pdf"
    assert ct.handler_for("text/vtt").text_source == "vtt"
    assert ct.handler_for("image/jpeg").text_source is None
    assert ct.handler_for("image/png").text_source is None


def test_every_registered_handler_declares_a_text_source():
    # None is a declaration (no text layer); "" is the unset default the
    # registry rejects at import.
    for handler in ct.HANDLERS:
        assert handler.text_source != "", (
            f"{type(handler).__name__} declares no text_source"
        )


def test_sniffed_canonical_mime_round_trips_to_a_handler():
    # http_get stamps content_type = sniffed.canonical_mime; web_fetch then re-looks
    # the handler up by it. Lock that round-trip: the stamped label must be one a
    # handler claims (the registry validates this, but guard it from the outside too).
    for handler in ct.HANDLERS:
        if handler.signature is not None:
            assert handler.canonical_mime in ct.EXTRACTABLE_CONTENT_TYPES
            assert ct.handler_for(handler.canonical_mime) is handler


# --------------------------------------------------------------------------- #
# thin_warning — the per-type no-text message (user-facing strings + branching)
# --------------------------------------------------------------------------- #


def test_html_thin_warning_suggests_render_when_none_attempted():
    msg = HtmlHandler().thin_warning("http://x", rendered=False, render_attempted=False)
    assert msg is not None
    assert "likely JS-only" in msg
    assert "--render" in msg


def test_html_thin_warning_distinct_after_a_render():
    msg = HtmlHandler().thin_warning("http://x", rendered=True, render_attempted=True)
    assert msg == "WARNING: still thin after render: http://x"


def test_html_thin_warning_quiet_when_render_attempted_and_failed():
    # A render was tried and returned None (render already logged why) — stay quiet.
    assert (
        HtmlHandler().thin_warning("http://x", rendered=False, render_attempted=True)
        is None
    )


def test_image_thin_warning_names_ocr():
    msg = ct.handler_for("image/jpeg").thin_warning(
        "http://x/f.jpg", rendered=False, render_attempted=False
    )
    assert msg is not None
    assert "OCR" in msg


def test_pdf_thin_warning_is_scanned_message_regardless_of_render_flags():
    # A PDF never renders, so the flags don't change its message.
    pdf = PdfHandler()
    for rendered, attempted in [(False, False), (False, True)]:
        msg = pdf.thin_warning(
            "http://x", rendered=rendered, render_attempted=attempted
        )
        assert msg == "WARNING: PDF extracted to little/no text (scanned?): http://x"


# --------------------------------------------------------------------------- #
# HTML handler — charset fallback (a bogus label must not crash the batch)
# --------------------------------------------------------------------------- #


def _decode(raw: bytes, charset: str | None) -> str | None:
    return HtmlHandler().decode(raw, charset)


def test_decode_honors_valid_charset():
    assert _decode("café".encode("latin-1"), "latin-1") == "café"


def test_decode_falls_back_to_utf8_on_unknown_charset():
    # A page advertising a junk charset label would make bytes.decode raise
    # LookupError — which escapes the fetch_one except tuple and kills the batch.
    # Fall back to utf-8 instead of losing the page.
    assert _decode(b"hi", "utf-8x-bogus") == "hi"


def test_decode_never_raises_on_bad_bytes():
    # utf-8 fallback still uses errors="replace", so undecodable bytes don't raise.
    assert isinstance(_decode(b"\xff\xfe bad", "totally-not-a-charset"), str)


# --------------------------------------------------------------------------- #
# HTML handler — charset sniffing for headerless Shift-JIS pages
# --------------------------------------------------------------------------- #
#
# Real-world failure: old Japanese pages (ampress.co.jp, showayuen) are served as
# Shift-JIS/cp932 with no Content-Type charset header, so a blind utf-8 decode
# yields mojibake. Resolve the charset from the page's own <meta> / detection.

JP = "会社概要"  # "Company Overview" — the showayuen kaisha_gaiyou page's title


def test_decode_sniffs_meta_http_equiv_charset_when_header_absent():
    # Legacy <meta http-equiv="Content-Type" ... charset=Shift_JIS>, no HTTP header.
    html = (
        '<html><head><meta http-equiv="Content-Type" '
        f'content="text/html; charset=Shift_JIS"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode("cp932")
    assert JP in _decode(html, None)


def test_decode_sniffs_html5_meta_charset_when_header_absent():
    html = (
        f'<html><head><meta charset="shift_jis"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode("cp932")
    assert JP in _decode(html, None)


def test_decode_detects_charset_when_no_header_and_no_meta():
    # No header, no meta declaration: fall back to charset-normalizer detection.
    body = (
        f"<html><body><h1>{JP}</h1>" + "日本語の本文。" * 40 + "</body></html>"
    ).encode("cp932")
    assert JP in _decode(body, None)


def test_decode_header_charset_wins_over_meta():
    # The HTTP header is authoritative; a contradicting meta must not override it.
    html = (
        f'<html><head><meta charset="shift_jis"><title>{JP}</title>'
        "</head><body>x</body></html>"
    ).encode()
    assert JP in _decode(html, "utf-8")


def test_decode_shift_jis_meta_decoded_as_cp932_superset():
    # Pages declaring Shift_JIS routinely use cp932 extension chars (①, etc.) that
    # the strict shift_jis codec can't decode. Treat the whole family as cp932.
    html = (
        f'<html><head><meta charset="Shift_JIS"></head><body>①{JP}</body></html>'
    ).encode("cp932")
    out = _decode(html, None)
    assert "①" in out
    assert JP in out


def test_sniff_meta_charset_html5_form():
    assert _sniff_meta_charset(b'<meta charset="utf-8">') == "utf-8"


def test_sniff_meta_charset_http_equiv_form():
    raw = b'<meta http-equiv="Content-Type" content="text/html; charset=Shift_JIS">'
    assert _sniff_meta_charset(raw) == "Shift_JIS"


def test_sniff_meta_charset_none_when_absent():
    assert _sniff_meta_charset(b"<html><body>no meta</body></html>") is None


def test_sniff_meta_charset_only_scans_head():
    # The HTML spec puts the declaration in the first 1024 bytes; ignore late strays.
    raw = b"x" * 1100 + b'<meta charset="shift_jis">'
    assert _sniff_meta_charset(raw) is None


# --------------------------------------------------------------------------- #
# HTML handler — conservative date extraction (no padded guess)
# --------------------------------------------------------------------------- #


def _extract_html(html: str, url: str = "http://x") -> ct.ExtractedMeta:
    return HtmlHandler().extract(b"", html, url)


def test_extract_date_null_when_only_weak_year_signal():
    html = (
        '<html><head><meta name="date" content="2024"></head>'
        "<body><article><p>Defunct maker.</p>"
        "<footer>© 2024 Acme</footer></article></body></html>"
    )
    assert _extract_html(html).last_updated is None


def test_extract_date_is_most_recent_real_date():
    html = (
        "<html><head>"
        '<meta property="article:published_time" content="2023-06-15">'
        '<meta property="article:modified_time" content="2024-08-01">'
        "</head><body><article><p>y</p></article></body></html>"
    )
    assert _extract_html(html).last_updated == "2024-08-01"


# --------------------------------------------------------------------------- #
# HTML handler — googleoff blocks (cookie banners) never win extraction
# --------------------------------------------------------------------------- #


def test_extract_keeps_image_caption_lists_without_image_markers():
    # Some evidence pages (tilt.it maker pages) are a thin caption list beside
    # a photo gallery — the page's only body text. Those paragraphs survive the
    # whole-document conversion, and the convert_img override emits alt text
    # alone, so no ![alt](url) markers land in the evidence text.
    gallery = "".join(
        f'<a href="http://x/g/{i}.jpg"><img src="http://x/g/t{i}.jpg" alt="www.x"/></a>'
        for i in range(6)
    )
    html = (
        "<html><body><article>"
        '<div class="entry-content"><h1>P.C.</h1>'
        f"<p>Marte (“Electra Pool”)<br/>{gallery}</p></div>"
        "</article></body></html>"
    )
    text = _extract_html(html).text or ""
    assert "Marte" in text
    assert "![" not in text


def test_extract_skips_googleoff_blocks():
    # A googleoff-delimited cookie-consent block beside a short machine list
    # (seen on tilt.it maker pages using the WP Cookie Law Info plugin). This
    # page is caught twice over — the googleoff regex and the consent-container
    # match both apply; the unclassed-googleoff test below isolates the regex.
    banner = (
        "<!--googleoff: all-->"
        '<div id="cookie-law-info-bar" data-nosnippet="true">'
        + "<p>This website uses cookies to improve your experience. "
        "We assume you are ok with this, but you can opt out if you wish. "
        "Necessary cookies are absolutely essential for the website to function "
        "properly and are stored on your browser. "
        * 20
        + "</p></div><!--googleon: all-->"
    )
    html = (
        "<html><body>" + banner + '<article><div class="entry-content"><h1>LORI</h1>'
        "<p>Jungle Life<br>Count-Down<br>KO<br>Space Orbit</p>"
        "</div></article></body></html>"
    )
    text = _extract_html(html).text or ""
    assert "Jungle Life" in text
    assert "uses cookies" not in text


# --------------------------------------------------------------------------- #
# HTML handler — whole-document markdown conversion
# --------------------------------------------------------------------------- #


def test_assembled_text_is_frontmatter_then_wellformed_markdown():
    html = (
        "<html><head><title>T</title></head><body>"
        "<h1>Top</h1><p>x</p><h3>Deep</h3><p>y</p></body></html>"
    )
    text = _extract_html(html).text or ""
    lines = text.split("\n")
    # YAML-style frontmatter: --- on line 1, key lines, a closing ---.
    assert lines[0] == "---"
    assert lines[1] == "title: T"
    assert "---" in lines[1:]
    # Page headings keep their source ATX levels — an <h1> is #, not demoted —
    # and nothing above them outranks them: the document is well-formed markdown.
    assert "# Top" in lines
    assert "### Deep" in lines


def test_frontmatter_delimiters_present_even_without_metadata():
    # The shape is positional and constant, so parsers need no fallback case.
    text = _extract_html("<html><body><p>Only prose.</p></body></html>").text or ""
    lines = text.split("\n")
    assert lines[0] == "---"
    assert lines[1] == "---"
    assert "Only prose." in text


def test_footer_and_comment_text_present():
    # The motivating cases: a footer street address and page comments are
    # exactly what a main-content extractor discards and this pipeline keeps.
    html = (
        "<html><body><article><p>Article.</p></article>"
        '<div id="comments"><p>The machine list is wrong; Nordamatic made 12.</p></div>'
        "<footer><p>Multimorphic, Inc. 2109 Couch Dr, McKinney</p></footer>"
        "</body></html>"
    )
    text = _extract_html(html).text or ""
    assert "2109 Couch Dr" in text
    assert "Nordamatic made 12" in text


def test_no_inline_markers_in_output():
    html = (
        "<html><body><p>A <b>bold</b> and <strong>strong</strong> and "
        '<i>italic</i> and <em>em</em> claim with <a href="http://x/">a link</a>.'
        "</p></body></html>"
    )
    text = _extract_html(html).text or ""
    assert "A bold and strong and italic and em claim with a link." in text
    assert "**" not in text
    assert "](" not in text


def test_image_alt_with_brackets_and_parens_survives():
    # The case the old ![alt](url) post-processing regex missed completely.
    html = '<html><body><p><img alt="a ] bracket" src="http://x/y_(1).png"></p></body></html>'
    text = _extract_html(html).text or ""
    assert "a ] bracket" in text
    assert "y_(1).png" not in text


def test_table_without_thead_promotes_first_row_no_phantom_header():
    html = (
        "<html><body><table>"
        "<tr><td>nombre</td><td>año</td><td>mes</td><td>tipo</td><td>JUG.</td></tr>"
        "<tr><td>Cavalier</td><td>1979</td><td></td><td>Electromecánica</td><td>4</td></tr>"
        "</table></body></html>"
    )
    text = _extract_html(html).text or ""
    assert "| nombre | año | mes | tipo | JUG. |" in text
    # The real first row is the header — no phantom empty header row above it.
    assert "|  |  |" not in text
    # Empty cells keep their pipes: the gap says "no month recorded".
    assert "| Cavalier | 1979 |  | Electromecánica | 4 |" in text


def test_select_noscript_template_text_present():
    html = (
        "<html><body>"
        "<select><option>Alben (19)</option><option>CIDELSA (7)</option></select>"
        "<noscript>Enable JS for maps.</noscript>"
        "<template><p>Template prose.</p></template>"
        "</body></html>"
    )
    text = _extract_html(html).text or ""
    assert "Alben (19)" in text
    assert "CIDELSA (7)" in text
    assert "Enable JS for maps." in text
    assert "Template prose." in text


def test_minified_html_does_not_fuse_blocks():
    # Zero source whitespace, the layout of an optimizer/SSR page. Without the
    # block-container converters, adjacent blocks fuse into unquotable run-ons
    # ("Multimorphic, Inc.2109 Couch Dr").
    html = (
        "<html><body>"
        "<footer><div>Multimorphic, Inc.</div><div>2109 Couch Dr</div></footer>"
        "<dl><dt>Players</dt><dd>4</dd><dt>Year</dt><dd>1979</dd></dl>"
        "<select><option>Alben (19)</option><option>Allied (1)</option></select>"
        "<p><span>in</span><span>line</span> spans stay fused</p>"
        "</body></html>"
    )
    text = _extract_html(html).text or ""
    assert "Multimorphic, Inc." in text
    assert "2109 Couch Dr" in text
    assert "Inc.2109" not in text
    assert "Players4" not in text
    assert "Alben (19)Allied" not in text
    # Inline elements fusing IS faithful — a browser renders "inline" too.
    assert "inline spans stay fused" in text


def test_repeated_adjacent_lines_survive():
    # No dedup step: a line repeated twice in a row appears twice.
    html = "<html><body><p>Alice Goes to Wonderland</p><p>Alice Goes to Wonderland</p></body></html>"
    text = _extract_html(html).text or ""
    assert text.count("Alice Goes to Wonderland") == 2


def test_pre_and_nested_list_indentation_survive_trailing_strip():
    html = (
        "<html><body>"
        "<pre>def f():\n    return x</pre>"
        "<ul><li>outer<ul><li>inner item</li></ul></li></ul>"
        "<p>break<br>here</p>"
        "</body></html>"
    )
    text = _extract_html(html).text or ""
    # <pre> indentation intact (no global whitespace collapse)...
    assert "    return x" in text
    # ...nested list still nested under its parent bullet...
    assert "- outer" in text
    nested = next(ln for ln in text.split("\n") if "inner item" in ln)
    assert nested != nested.lstrip(), "nested bullet lost its indentation"
    # ...but trailing whitespace (incl. two-space <br> markers) is stripped.
    assert not any(ln != ln.rstrip() for ln in text.split("\n"))
    assert "break\nhere" in text


def test_exclusion_matrix_nothing_nontextual_leaks():
    # Built with the real configured options (bs4_options="lxml",
    # table_infer_header=True, the convert_img subclass) — the whole matrix of
    # non-text sources, none of which may reach the stored text.
    html = """<html><head><title>Matrix</title>
<style>.CSSJUNK { color: red; }</style>
<script type="application/ld+json">{"@type":"Organization","name":"LDJUNK"}</script>
</head><body>
<p style="COLORJUNK" onclick="CLICKJUNK()" data-x="DATAJUNK">Visible prose.</p>
<script>var SCRIPTJUNK = 1;</script>
<svg><title>SVGTITLEJUNK</title><desc>SVGDESCJUNK</desc><text>SVGTEXTJUNK</text></svg>
<svg xmlns="http://www.w3.org/2000/svg"><text>NSSVGJUNK</text></svg>
<noscript><style>.NOSCRIPTCSSJUNK{}</style>Enable JS please.</noscript>
<template><script>var TEMPLATESCRIPTJUNK;</script><p>Template prose.</p></template>
<!-- COMMENTJUNK -->
<canvas>CANVASJUNK</canvas>
<iframe>IFRAMEJUNK</iframe>
<pre><code>  fenced code sample</code></pre>
</body></html>"""
    text = _extract_html(html).text or ""
    for junk in (
        "CSSJUNK",
        "LDJUNK",
        "COLORJUNK",
        "CLICKJUNK",
        "DATAJUNK",
        "SCRIPTJUNK",
        "SVGTITLEJUNK",
        "SVGDESCJUNK",
        "SVGTEXTJUNK",
        "NSSVGJUNK",
        "NOSCRIPTCSSJUNK",
        "TEMPLATESCRIPTJUNK",
        "COMMENTJUNK",
        "CANVASJUNK",
        "IFRAMEJUNK",
    ):
        assert junk not in text, f"{junk} leaked into extracted text"
    assert "Visible prose." in text
    assert "Enable JS please." in text
    assert "Template prose." in text
    assert "fenced code sample" in text


def test_consent_widget_stripped_token_match_only():
    # The consent container goes; the sticky-header beside it — whose class
    # merely *contains* "cky" — survives, because matching is on class tokens
    # and id prefixes, never raw substrings.
    html = (
        "<html><body>"
        '<div class="sticky-header">Real Site Nav</div>'
        '<div class="cky-consent-container"><p>We use cookies. Accept Reject</p></div>'
        "<p>Real content.</p>"
        "</body></html>"
    )
    text = _extract_html(html).text or ""
    assert "Real Site Nav" in text
    assert "Real content." in text
    assert "We use cookies" not in text


def test_unclassed_googleoff_block_removed():
    # The googleoff regex on its own — the block carries no consent-platform
    # class, so only the page's own machine-readable marker removes it.
    html = (
        "<html><body><p>Keep me.</p>"
        "<!--googleoff: index--><div><p>UNMARKEDJUNK</p></div><!--googleon: index-->"
        "<p>And me.</p></body></html>"
    )
    text = _extract_html(html).text or ""
    assert "Keep me." in text
    assert "And me." in text
    assert "UNMARKEDJUNK" not in text


# --------------------------------------------------------------------------- #
# HTML handler — head metadata block, title chain, body_text
# --------------------------------------------------------------------------- #


def test_meta_dedup_prefers_most_general_key():
    html = (
        "<html><head>"
        '<meta property="og:description" content="One description.">'
        '<meta name="description" content="One description.">'
        '<meta name="twitter:description" content="One description.">'
        "</head><body><p>x</p></body></html>"
    )
    text = _extract_html(html).text or ""
    assert "description: One description." in text
    assert "og:description:" not in text
    assert "twitter:description:" not in text


def test_meta_case_distinct_values_all_emitted():
    # The quote gate collapses whitespace but never case, so a case difference
    # is a genuine difference — both variants must be quotable.
    html = (
        "<html><head><title>ACME PINBALL</title>"
        '<meta property="og:title" content="Acme Pinball"></head>'
        "<body><p>x</p></body></html>"
    )
    text = _extract_html(html).text or ""
    assert "title: ACME PINBALL" in text
    assert "og:title: Acme Pinball" in text


def test_contentless_page_stores_no_text():
    # A parseable document with no metadata and an empty body must not store
    # bare "---\n---" scaffolding: truthy-but-empty text would defeat the
    # importer's text-is-mandatory gate and the backfill's never-blank guard.
    meta = _extract_html("<html><body></body></html>")
    assert meta.text is None
    assert meta.body_text == ""  # a real, thin body — not a missing one


def test_meta_differing_values_all_emitted():
    html = (
        "<html><head>"
        '<meta name="description" content="Short one.">'
        '<meta property="og:description" content="A longer, different one.">'
        "</head><body><p>x</p></body></html>"
    )
    text = _extract_html(html).text or ""
    assert "description: Short one." in text
    assert "og:description: A longer, different one." in text


def test_meta_excluded_keys_do_not_appear():
    html = (
        "<html><head>"
        '<meta name="viewport" content="width=device-width">'
        '<meta name="generator" content="WordPress 6.4">'
        '<meta name="robots" content="index, follow">'
        '<meta property="og:image" content="http://x/img.png">'
        "</head><body><p>x</p></body></html>"
    )
    text = _extract_html(html).text or ""
    for junk in ("viewport", "WordPress", "robots", "img.png"):
        assert junk not in text


def test_title_prefers_og_title():
    html = (
        "<html><head><title>Tag Title</title>"
        '<meta property="og:title" content="OG Title"></head>'
        "<body><p>x</p></body></html>"
    )
    assert _extract_html(html).title == "OG Title"


def test_title_falls_back_to_title_tag_verbatim_with_suffix():
    # No suffix stripping — the tag is stored intact.
    html = (
        "<html><head><title>Wizard Pinball Machine | Pinside Game Archive</title>"
        "</head><body><p>x</p></body></html>"
    )
    assert _extract_html(html).title == "Wizard Pinball Machine | Pinside Game Archive"


def test_title_falls_back_to_first_h1():
    html = "<html><body><h1>Company Information</h1><p>x</p></body></html>"
    assert _extract_html(html).title == "Company Information"


def test_title_string_appears_exactly_once_in_text():
    # The body-subtree rule: <title> lives in the metadata block only, never
    # duplicated into the body by converting the whole document.
    html = (
        "<html><head><title>UNIQUETITLETOKEN</title></head>"
        "<body><p>Body prose.</p></body></html>"
    )
    text = _extract_html(html).text or ""
    assert text.count("UNIQUETITLETOKEN") == 1


def test_body_text_is_body_only_and_empty_for_js_shell():
    # A JS-only page ships rich og: tags precisely because crawlers don't run
    # JS — so the assembled text is fat while the body is empty. body_text is
    # the thin-detection probe and must measure the body alone.
    html = (
        "<html><head><title>Fat Meta</title>"
        '<meta property="og:description" content="'
        + "A very rich page description. " * 20
        + '"></head><body><div id="root"></div></body></html>'
    )
    meta = _extract_html(html)
    assert meta.body_text == ""
    assert len(meta.text or "") > 200


def test_body_text_none_when_unparseable():
    meta = _extract_html("")
    assert meta.text is None
    assert meta.body_text is None


# --------------------------------------------------------------------------- #
# PDF handler — text from the poppler backend, title/date from the Info dict
# --------------------------------------------------------------------------- #


def _extract_pdf(raw: bytes) -> ct.ExtractedMeta:
    return PdfHandler().extract(raw, None, "http://x")


def test_extract_pdf_pulls_text_title_and_date(make_pdf):
    raw = make_pdf(
        title="Test Rulesheet",
        moddate="D:20240115093000-08'00'",
        creationdate="D:20200101000000Z",
    )
    meta = _extract_pdf(raw)
    assert "Hello PDF evidence" in (meta.text or "")
    assert meta.title == "Test Rulesheet"
    # ModDate preferred over the older CreationDate (most-recent-date semantics).
    assert meta.last_updated == "2024-01-15"


def test_extract_pdf_falls_back_to_creationdate(make_pdf):
    raw = make_pdf(creationdate="D:20200101000000Z")  # no ModDate
    assert _extract_pdf(raw).last_updated == "2020-01-01"


def test_extract_pdf_no_metadata_yields_none_title_and_date(make_pdf):
    meta = _extract_pdf(make_pdf())  # no Info-dict fields
    assert meta.title is None
    assert meta.last_updated is None
    assert "Hello PDF evidence" in (meta.text or "")


def test_extract_pdf_malformed_reports_unavailable_not_raises():
    # A broken/garbage PDF must not crash a batch — but "we couldn't read it" is
    # not "it has no text", so it reports unavailable rather than an empty
    # result. That is what stops a failed re-read from blanking text the
    # byte-identical blob once supported.
    meta = _extract_pdf(b"%PDF-1.4 not really a pdf")
    assert meta.text is None
    assert meta.unavailable is True


def test_extract_pdf_scanned_image_only_has_no_text(make_pdf):
    # An image-only PDF (here: empty content) really does have no text layer.
    # Unlike the malformed case above this is a *finding*: unavailable stays
    # False, so the row is written and the thin-content warning surfaces it.
    meta = _extract_pdf(make_pdf(text=""))
    assert meta.text is None
    assert meta.unavailable is False


def test_extract_pdf_keeps_metadata_when_the_text_backend_is_missing(
    make_pdf, monkeypatch
):
    # Text and metadata come from different backends precisely so one can't take
    # the other down: with poppler absent the document still declares its title.
    import web_pdftext

    def _unavailable(_raw: bytes) -> str | None:
        raise web_pdftext.PdfTextUnavailableError("no poppler here")

    monkeypatch.setattr(web_pdftext, "pdf_text", _unavailable)
    meta = _extract_pdf(
        make_pdf(title="Sonic Compare Flyer", moddate="D:20260529000000Z")
    )
    assert meta.title == "Sonic Compare Flyer"
    assert meta.last_updated == "2026-05-29"
    assert meta.text is None
    assert meta.unavailable is True


def test_extract_pdf_keeps_text_when_the_info_dict_is_unreadable(monkeypatch):
    # The other direction: a PDF whose metadata pypdf chokes on still yields its
    # full text, instead of one malformed Info field costing the whole document.
    import content_types.pdf as pdf_mod
    import web_pdftext

    monkeypatch.setattr(pdf_mod, "_pdf_meta", lambda _raw: (None, None))
    monkeypatch.setattr(web_pdftext, "pdf_text", lambda _raw: "9 Stand-Up Targets")
    meta = _extract_pdf(b"%PDF-1.4 whatever")
    assert meta.text == "9 Stand-Up Targets"
    assert meta.title is None
    assert meta.unavailable is False


# --------------------------------------------------------------------------- #
# Image handler — text via OCR (the Vision backend is stubbed; see test_web_ocr)
# --------------------------------------------------------------------------- #


def _extract_jpeg(monkeypatch, ocr_result):
    monkeypatch.setattr(web_ocr, "ocr_text", lambda raw: ocr_result)
    return JpegHandler().extract(b"\xff\xd8\xff fake jpeg", None, "http://x/f.jpg")


def test_extract_image_puts_ocr_in_the_machine_read_tier(monkeypatch):
    # OCR'd words are findable but are not a text layer: they land in ocr_text
    # and the extracted text stays None.
    meta = _extract_jpeg(monkeypatch, "MECATRONICS INDUSTRIA E COMERCIO LTDA.")
    assert meta.text is None
    assert meta.ocr_text == "MECATRONICS INDUSTRIA E COMERCIO LTDA."


def test_extract_image_has_no_title_or_date(monkeypatch):
    # An image carries no metadata we'd trust: OCR'd text is not a title, and an
    # EXIF timestamp is when the *photo/scan* was taken, not the document's date.
    # Both stay None rather than being guessed; the importer can supply them.
    meta = _extract_jpeg(monkeypatch, "some words")
    assert meta.title is None
    assert meta.last_updated is None


def test_extract_image_with_no_recognized_text_is_empty(monkeypatch):
    # A photo with no legible text: no text, no crash — the blob is still stored
    # and the caller's thin-content warning fires. A fact about the *document*,
    # so `unavailable` stays False and the result may overwrite a stale row.
    meta = _extract_jpeg(monkeypatch, None)
    assert meta.text is None
    assert meta.unavailable is False


def test_extract_image_survives_missing_ocr_backend(monkeypatch):
    # No pyobjc / not macOS: warn and keep going rather than killing a batch that
    # happens to include an image URL — but flag it `unavailable`, a fact about
    # this *host*. Without that the caller can't tell it from "this image has no
    # text" and would blank a row another host had already OCR'd.
    def _boom(raw: bytes) -> str | None:
        raise web_ocr.OcrUnavailableError("no Vision here")

    monkeypatch.setattr(web_ocr, "ocr_text", _boom)
    meta = JpegHandler().extract(b"\xff\xd8\xff", None, "http://x/f.jpg")
    assert meta.text is None
    assert meta.unavailable is True


def test_extract_image_makes_no_claim_when_the_ocr_request_fails(monkeypatch, capsys):
    # Vision errors cover both a bad file and a bad moment (timeout, OOM), and
    # Apple's error codes don't separate them reliably. So a failed request is
    # "no result", never "this document has no text" — otherwise a transient
    # failure blanks text the identical bytes still support.
    def _boom(raw: bytes) -> str | None:
        raise web_ocr.OcrFailedError("not a decodable image")

    monkeypatch.setattr(web_ocr, "ocr_text", _boom)
    meta = JpegHandler().extract(b"\xff\xd8\xff", None, "http://x/f.jpg")
    assert meta.text is None
    assert meta.unavailable is True
    assert "ocr failed" in capsys.readouterr().err.lower()

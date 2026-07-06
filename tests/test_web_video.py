"""Tests for the video-transcript path — all offline (yt-dlp stubbed).

Covers the pure pieces (video URL recognition/canonicalization, VTT transcript
extraction, caption-track selection) and the fetch_one orchestration for video
URLs (page row keyed on the canonical watch URL, .vtt blob, the no-captions
audit-only path, and the freshness skip). The yt-dlp call itself is stubbed —
these tests never touch the network.
"""

from __future__ import annotations

import pytest
import web_cache as wc
import web_fetch
import web_video
from content_types import extension_for, handler_for
from content_types.vtt import transcript_text


@pytest.fixture(autouse=True)
def _no_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    # Never sleep in tests (the real per-domain limiter would on repeat fetches).
    monkeypatch.setattr(web_fetch, "_rate_limit", lambda domain: None)


# --------------------------------------------------------------------------- #
# URL recognition / canonicalization
# --------------------------------------------------------------------------- #


def test_watch_url_canonicalizes_to_itself():
    assert (
        web_video.canonical_video_url("https://www.youtube.com/watch?v=O-2BXTXLXIY")
        == "https://www.youtube.com/watch?v=O-2BXTXLXIY"
    )


def test_youtu_be_and_shorts_and_live_canonicalize_to_watch():
    for url in (
        "https://youtu.be/O-2BXTXLXIY",
        "https://www.youtube.com/shorts/O-2BXTXLXIY",
        "https://www.youtube.com/live/O-2BXTXLXIY",
        "https://m.youtube.com/watch?v=O-2BXTXLXIY",
    ):
        assert (
            web_video.canonical_video_url(url)
            == "https://www.youtube.com/watch?v=O-2BXTXLXIY"
        )


def test_non_video_urls_are_not_claimed():
    for url in (
        "https://example.com/watch?v=abc",
        "https://www.youtube.com/@channel",
        "https://www.youtube.com/playlist?list=PL123",
        "https://www.youtube.com/watch",  # no id
    ):
        assert web_video.canonical_video_url(url) is None


# --------------------------------------------------------------------------- #
# VTT transcript extraction
# --------------------------------------------------------------------------- #

ROLLING_VTT = """WEBVTT
Kind: captions
Language: en

00:00:00.320 --> 00:00:02.879 align:start position:0%

and<00:00:00.560><c> the</c><00:00:00.799><c> winner</c><00:00:01.199><c> is</c>

00:00:02.879 --> 00:00:05.190 align:start position:0%
and the winner is
Elf<00:00:03.360><c> by</c><00:00:03.679><c> Bob</c><00:00:04.000><c> Nies</c>

00:00:05.190 --> 00:00:07.000 align:start position:0%
Elf by Bob Nies
"""


def test_transcript_strips_header_timestamps_and_tags():
    assert transcript_text(ROLLING_VTT) == "and the winner is\nElf by Bob Nies"


def test_transcript_dedupes_rolling_caption_repeats():
    # Each auto-caption line appears in two consecutive cues; only one survives.
    text = transcript_text(ROLLING_VTT)
    assert text is not None
    assert text.count("Elf by Bob Nies") == 1


def test_transcript_of_cueless_vtt_is_none():
    assert transcript_text("WEBVTT\nKind: captions\nLanguage: en\n") is None


def test_vtt_handler_is_registered():
    handler = handler_for("text/vtt")
    assert handler is not None
    assert extension_for("text/vtt") == "vtt"
    meta = handler.extract(ROLLING_VTT.encode(), ROLLING_VTT, "u")
    assert meta.text == "and the winner is\nElf by Bob Nies"


# --------------------------------------------------------------------------- #
# Caption-track selection
# --------------------------------------------------------------------------- #


def _track(lang: str) -> list[dict[str, str]]:
    return [{"ext": "vtt", "url": f"https://captions.test/{lang}.vtt"}]


def test_manual_subtitles_beat_automatic_captions():
    picked = web_video.pick_track(
        subtitles={"en": _track("man-en")},
        automatic_captions={"en": _track("auto-en")},
        video_language="en",
    )
    assert picked is not None
    assert picked.url == "https://captions.test/man-en.vtt"
    assert picked.kind == "subtitles"


def test_english_variant_preferred_among_manual_subtitles():
    picked = web_video.pick_track(
        subtitles={"de": _track("de"), "en-GB": _track("en-GB")},
        automatic_captions={},
        video_language="de",
    )
    assert picked is not None
    assert picked.lang == "en-GB"


def test_auto_captions_prefer_original_language_track():
    # Auto captions offer dozens of machine-translated languages; the original
    # spoken track (lang or lang-orig) is the trustworthy one.
    picked = web_video.pick_track(
        subtitles={},
        automatic_captions={
            "en": _track("translated-en"),
            "ja-orig": _track("orig"),
            "ja": _track("ja"),
        },
        video_language="ja",
    )
    assert picked is not None
    assert picked.lang == "ja-orig"
    assert picked.kind == "automatic_captions"


def test_no_tracks_returns_none():
    assert web_video.pick_track(subtitles={}, automatic_captions={}) is None


# --------------------------------------------------------------------------- #
# fetch_one orchestration for video URLs
# --------------------------------------------------------------------------- #

WATCH = "https://www.youtube.com/watch?v=O-2BXTXLXIY"


def _stub_video(monkeypatch, *, vtt: bytes | None, fail: bool = False):
    def _fetch(url: str) -> web_video.VideoResp | None:
        if fail:
            return None
        return web_video.VideoResp(
            title="2022 TWIPY Awards Show Full Results",
            upload_date="2023-03-26",
            vtt=vtt,
            caption_note="automatic_captions:en",
        )

    monkeypatch.setattr(web_video, "fetch_video", _fetch)


def _fetch_rows(con):
    return con.execute(
        "SELECT url, http_status, content_sha FROM fetches ORDER BY id"
    ).fetchall()


def test_video_with_captions_caches_transcript(cache, monkeypatch):
    con = cache
    _stub_video(monkeypatch, vtt=ROLLING_VTT.encode())
    web_fetch.fetch_one(
        con, "https://youtu.be/O-2BXTXLXIY", query="twipy", force=False, max_age_days=30
    )
    page = wc.get(WATCH, con=con)
    assert page is not None
    assert page["content_type"] == "text/vtt"
    assert page["title"] == "2022 TWIPY Awards Show Full Results"
    assert page["last_updated"] == "2023-03-26"
    assert page["text"] == "and the winner is\nElf by Bob Nies"
    blob = wc.blob_path(page["content_sha"], ext="vtt")
    assert blob.read_bytes() == ROLLING_VTT.encode()


def test_video_without_captions_logs_audit_row_only(cache, monkeypatch, capsys):
    con = cache
    _stub_video(monkeypatch, vtt=None)
    web_fetch.fetch_one(con, WATCH, query=None, force=False, max_age_days=30)
    assert wc.get(WATCH, con=con) is None
    assert len(_fetch_rows(con)) == 1
    assert "no captions" in capsys.readouterr().err


def test_video_extraction_failure_logs_audit_row(cache, monkeypatch, capsys):
    con = cache
    _stub_video(monkeypatch, vtt=None, fail=True)
    web_fetch.fetch_one(con, WATCH, query=None, force=False, max_age_days=30)
    assert wc.get(WATCH, con=con) is None
    rows = _fetch_rows(con)
    assert len(rows) == 1
    assert rows[0][1] is None  # no status — the extraction failed
    assert "FAILED" in capsys.readouterr().err


def test_fresh_video_skips_refetch(cache, monkeypatch, capsys):
    con = cache
    _stub_video(monkeypatch, vtt=ROLLING_VTT.encode())
    web_fetch.fetch_one(con, WATCH, query=None, force=False, max_age_days=30)
    # A second fetch inside the window — even via a differently-shaped URL —
    # must skip without calling yt-dlp again.
    _stub_video(monkeypatch, vtt=None, fail=True)  # would fail if called
    web_fetch.fetch_one(
        con, "https://youtu.be/O-2BXTXLXIY", query=None, force=False, max_age_days=30
    )
    assert "skip (fresh" in capsys.readouterr().out
    assert len(_fetch_rows(con)) == 1

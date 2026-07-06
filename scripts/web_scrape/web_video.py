#!/usr/bin/env python3
"""Video-transcript transport for the web evidence cache (see docs/WebCache.md).

A YouTube URL isn't a document a plain GET can read — the watch page is a JS
shell and the evidence is spoken. This module is the transport for that case:
yt-dlp pulls the video's metadata and its best caption track, and the fetcher
stores the raw ``.vtt`` as the blob with the parsed transcript as the page text
(``content_types/vtt``), keyed on the canonical watch URL. From there the
transcript is searchable and quotable like any cached page.

Track choice: manual subtitles beat auto-captions (a human wrote them), and
within a kind an English track beats others — except among auto-captions, where
the original spoken language (``xx-orig``/``xx``) beats YouTube's machine
translations. Videos with no captions at all (common for livestream archives)
yield a loud warning and an audit row, not a page: there is no transcript to
quote, and the video description often links the written source instead.
"""

from __future__ import annotations

import sys
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Mapping

WATCH_URL_TEMPLATE = "https://www.youtube.com/watch?v={id}"

# Hosts that serve YouTube videos, post-normalize_url (lowercased hostname).
_YOUTUBE_HOSTS = frozenset(
    {"www.youtube.com", "youtube.com", "m.youtube.com", "music.youtube.com"}
)
# Path prefixes that embed the video id as their next segment.
_PATH_ID_PREFIXES = ("/shorts/", "/live/", "/embed/", "/v/")

_CAPTION_TIMEOUT_SECONDS = 30

# A caption track's format entries as yt-dlp reports them ({"ext", "url", ...}).
type CaptionFormats = list[Mapping[str, str]]


class CaptionTrack(NamedTuple):
    """One chosen caption track: where it came from and where to download it."""

    kind: str  # "subtitles" (manual) or "automatic_captions"
    lang: str
    url: str


class VideoResp(NamedTuple):
    """What the video transport hands the fetcher for one video."""

    title: str | None
    upload_date: str | None  # YYYY-MM-DD, the video's publish date
    vtt: bytes | None  # the raw caption track, or None when the video has none
    caption_note: str | None  # e.g. "automatic_captions:en", for the fetch log


def canonical_video_url(url: str) -> str | None:
    """The canonical watch URL for a YouTube video URL, or None if not one.

    All the shapes a video arrives as — ``watch?v=``, ``youtu.be/``,
    ``/shorts/``, ``/live/``, ``/embed/`` — collapse to one cache key, so the
    same video dedups to one row however it was cited.
    """
    parts = urllib.parse.urlsplit(url)
    host = (parts.hostname or "").lower()
    if host == "youtu.be":
        vid = parts.path.lstrip("/").split("/")[0]
        return WATCH_URL_TEMPLATE.format(id=vid) if vid else None
    if host not in _YOUTUBE_HOSTS:
        return None
    if parts.path == "/watch":
        vid = urllib.parse.parse_qs(parts.query).get("v", [""])[0]
        return WATCH_URL_TEMPLATE.format(id=vid) if vid else None
    for prefix in _PATH_ID_PREFIXES:
        if parts.path.startswith(prefix):
            vid = parts.path.removeprefix(prefix).split("/")[0]
            return WATCH_URL_TEMPLATE.format(id=vid) if vid else None
    return None


def _preferred_langs(available: list[str], video_language: str | None) -> list[str]:
    """``available`` sorted best-first: original language, then English, then rest."""

    def rank(lang: str) -> tuple[int, str]:
        if video_language and lang in (f"{video_language}-orig", video_language):
            score = 0 if lang.endswith("-orig") else 1
        elif lang == "en" or lang.startswith("en-"):
            score = 2
        else:
            score = 3
        return (score, lang)  # lang breaks ties deterministically

    return sorted(available, key=rank)


def pick_track(
    *,
    subtitles: Mapping[str, CaptionFormats],
    automatic_captions: Mapping[str, CaptionFormats],
    video_language: str | None = None,
) -> CaptionTrack | None:
    """The best caption track of a video, or None when it has none.

    Manual subtitles always beat auto-captions. Within manual subtitles English
    is preferred (the catalog's working language); within auto-captions the
    original spoken language is — every other language there is a machine
    translation of it, one lossy step further from the evidence.
    """
    for kind, tracks in (
        ("subtitles", subtitles),
        ("automatic_captions", automatic_captions),
    ):
        if kind == "subtitles":
            langs = sorted(
                tracks,
                key=lambda lang: (
                    0 if lang == "en" or lang.startswith("en-") else 1,
                    lang,
                ),
            )
        else:
            langs = _preferred_langs(list(tracks), video_language)
        for lang in langs:
            vtt_url = next(
                (f["url"] for f in tracks[lang] if f.get("ext") == "vtt"), None
            )
            if vtt_url:
                return CaptionTrack(kind=kind, lang=lang, url=vtt_url)
    return None


def _download_vtt(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=_CAPTION_TIMEOUT_SECONDS) as resp:
        return resp.read()  # type: ignore[no-any-return]


def fetch_video(url: str) -> VideoResp | None:
    """Metadata + best caption track for a video, or None when extraction fails.

    ``vtt=None`` with a non-None result means the video genuinely has no
    captions (extraction succeeded; there is just nothing to transcribe).
    """
    from yt_dlp import YoutubeDL
    from yt_dlp.utils import DownloadError, ExtractorError

    opts = {"skip_download": True, "quiet": True, "no_warnings": True}
    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except (DownloadError, ExtractorError) as exc:
        print(f"yt-dlp failed: {url} ({exc})", file=sys.stderr)
        return None
    if info is None:
        return None

    upload = info.get("upload_date")  # yt-dlp gives YYYYMMDD
    upload_iso = (
        f"{upload[:4]}-{upload[4:6]}-{upload[6:]}"
        if isinstance(upload, str) and len(upload) == 8
        else None
    )
    track = pick_track(
        subtitles=info.get("subtitles") or {},
        automatic_captions=info.get("automatic_captions") or {},
        video_language=info.get("language"),
    )
    vtt: bytes | None = None
    note: str | None = None
    if track is not None:
        try:
            vtt = _download_vtt(track.url)
            note = f"{track.kind}:{track.lang}"
        except OSError as exc:
            print(f"caption download failed: {url} ({exc})", file=sys.stderr)
            return None
    return VideoResp(
        title=info.get("title"), upload_date=upload_iso, vtt=vtt, caption_note=note
    )

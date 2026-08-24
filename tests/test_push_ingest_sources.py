"""Tests for the push script's file selection and upload decisions.

`make push` is the only writer to the R2 ingest-source bucket, and a wrong skip
decision there stays invisible until a later `make pull` misses a file. These
cover what decides correctness: which local files are eligible
(`_collect_files`), how the bucket listing and committed manifest are read, and
what the two together mean for a given file (`_upload_reason`).

No network — `_upload_reason` is pure, and the listing and manifest reads take
the S3 client as an argument, so a stub covers them.
"""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import types
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_push():
    """Import scripts/cloud_store/push_ingest_sources.py (not a package)."""
    path = REPO_ROOT / "scripts" / "cloud_store" / "push_ingest_sources.py"
    spec = importlib.util.spec_from_file_location("push_ingest_sources", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


push = _load_push()


class _NoSuchKeyError(Exception):
    pass


class _DeniedError(Exception):
    """Any non-404 error — must never be mistaken for "the manifest isn't there"."""


class _StubS3:
    """Stands in for the S3 client's paginator plus get_object."""

    exceptions = types.SimpleNamespace(NoSuchKey=_NoSuchKeyError)

    def __init__(
        self, keys: dict[str, int], manifest: bytes | Exception | None = None
    ) -> None:
        self.keys = keys
        self.manifest = manifest  # bytes, None (absent), or an exception to raise

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        items = [{"Key": k, "Size": v} for k, v in self.keys.items()]
        # Two pages, to prove pagination is followed rather than first-page-only.
        pages = [{"Contents": items[:1]}, {"Contents": items[1:]}]
        return types.SimpleNamespace(paginate=lambda **_: pages)

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        if isinstance(self.manifest, Exception):
            raise self.manifest
        if self.manifest is None:
            raise _NoSuchKeyError(Key)
        return {"Body": io.BytesIO(self.manifest)}


def _entry(rel: str, data: bytes) -> dict[str, Any]:
    return {"path": rel, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def _manifest(*entries: dict[str, Any]) -> bytes:
    return json.dumps(list(entries)).encode()


# --- _human -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("size", "expected"),
    [
        (0, "0 B"),
        (999, "999 B"),
        (1000, "1.0 KB"),
        (22_644_762, "22.6 MB"),
        (190_457_021, "190.5 MB"),
        (1_500_000_000, "1.5 GB"),
        (2_500_000_000_000, "2.5 TB"),
    ],
)
def test_format_decimal_bytes_matches_object_store_reporting(size, expected):
    """Object stores report decimal MB, so matching them avoids a confusing 21.6."""
    assert push._format_decimal_bytes(size) == expected


# --- _elide -----------------------------------------------------------------


def test_elide_leaves_short_text_alone():
    assert push._elide("web/cache.sqlite", 44) == "web/cache.sqlite"


def test_elide_keeps_both_ends_and_respects_width():
    path = "web/raw/" + "a1b2c3d4" * 8 + ".pdf"
    out = push._elide(path, 44)
    assert len(out) == 44
    assert out.startswith("web/raw/a1b2")
    assert out.endswith(".pdf")
    assert "…" in out


@pytest.mark.parametrize("width", range(0, 8))
def test_elide_never_exceeds_width(width):
    """text[-0:] is the whole string; a naive tail slice silently grows the output."""
    assert len(push._elide("web/raw/deadbeef.pdf", width)) <= width


# --- _upload_reason ---------------------------------------------------------

SAMPLE = _entry("web/cache.sqlite", b"a" * 2048)


def test_upload_reason_none_when_listing_and_manifest_both_agree():
    assert push._upload_reason(SAMPLE, SAMPLE["size"], SAMPLE["sha256"]) is None


def test_upload_reason_new_when_not_in_the_listing():
    """Absence is judged by the listing, so an out-of-band delete is restored."""
    assert push._upload_reason(SAMPLE, None, SAMPLE["sha256"]) == "new"


def test_upload_reason_reports_size_difference():
    assert push._upload_reason(SAMPLE, 1500, SAMPLE["sha256"]) == (
        "size differs (remote 1.5 KB)"
    )


def test_upload_reason_uploads_when_manifest_does_not_describe_it():
    """Present in the bucket but unlisted — a prior push died before committing."""
    assert push._upload_reason(SAMPLE, SAMPLE["size"], None) == "not in manifest"


def test_upload_reason_reports_content_difference():
    """Same size, different bytes — the case a size-only check cannot see."""
    other = hashlib.sha256(b"b" * 2048).hexdigest()
    assert push._upload_reason(SAMPLE, SAMPLE["size"], other) == "content differs"


# --- _list_remote / _fetch_remote_manifest ----------------------------------


def test_list_remote_follows_pagination():
    s3 = _StubS3({"a.json": 10, "b.json": 20, "web/raw/c.html": 30})
    assert push._list_remote(s3, "bucket") == {
        "a.json": 10,
        "b.json": 20,
        "web/raw/c.html": 30,
    }


def test_fetch_remote_manifest_maps_path_to_hash():
    one = _entry("a.json", b"aa")
    two = _entry("web/cache.sqlite", b"bb")
    s3 = _StubS3({}, manifest=_manifest(one, two))
    assert push._fetch_remote_manifest(s3, "bucket") == {
        "a.json": one["sha256"],
        "web/cache.sqlite": two["sha256"],
    }


def test_fetch_remote_manifest_none_when_absent():
    """A first push against an empty bucket, not an error."""
    assert push._fetch_remote_manifest(_StubS3({}), "bucket") is None


@pytest.mark.parametrize("body", [b"not json at all", b"{}", b'[{"path": "a.json"}]'])
def test_fetch_remote_manifest_degrades_on_malformed_body(body, capsys):
    """Unusable, so the caller re-uploads rather than trusting a bad record."""
    assert push._fetch_remote_manifest(_StubS3({}, manifest=body), "bucket") is None
    assert "unreadable" in capsys.readouterr().out


def test_fetch_remote_manifest_propagates_other_errors():
    """A 403 silently read as "no manifest" would plan a full re-upload."""
    s3 = _StubS3({}, manifest=_DeniedError("403"))
    with pytest.raises(_DeniedError):
        push._fetch_remote_manifest(s3, "bucket")


# --- _compare ---------------------------------------------------------------


def _tree(n: int) -> list[dict[str, Any]]:
    return [_entry(f"f{i}.json", str(i).encode() * 50) for i in range(n)]


def test_compare_plans_nothing_when_listing_and_manifest_agree():
    entries = _tree(5)
    s3 = _StubS3({e["path"]: e["size"] for e in entries}, manifest=_manifest(*entries))
    planned, orphans = push._compare(s3, "bucket", entries)
    assert planned == []
    assert orphans == []


@pytest.mark.parametrize("manifest", [None, b"{}", b"garbage"])
def test_compare_reuploads_everything_without_a_trustworthy_manifest(manifest):
    """Skipping same-sized objects would publish hashes nobody verified.

    `make pull` recomputes sha256 and exits 1 on a mismatch, so an unverifiable
    hash is worse than the bandwidth.
    """
    entries = _tree(5)
    s3 = _StubS3({e["path"]: e["size"] for e in entries}, manifest=manifest)
    planned, _ = push._compare(s3, "bucket", entries)
    assert [p.reason for p in planned] == ["not in manifest"] * 5


def test_compare_reuploads_an_object_deleted_out_of_band():
    """The listing, not the manifest, decides whether an object is really there."""
    entries = _tree(3)
    listing = {e["path"]: e["size"] for e in entries}
    del listing[entries[1]["path"]]
    s3 = _StubS3(listing, manifest=_manifest(*entries))
    planned, _ = push._compare(s3, "bucket", entries)
    assert [(p.entry["path"], p.reason) for p in planned] == [
        (entries[1]["path"], "new")
    ]


def test_compare_reports_orphans_still_present_in_the_bucket():
    entries = _tree(3)
    gone = _entry("web/raw/abandoned.html", b"x")
    listing = {e["path"]: e["size"] for e in [*entries, gone]}
    s3 = _StubS3(listing, manifest=_manifest(*entries, gone))
    _, orphans = push._compare(s3, "bucket", entries)
    assert orphans == ["web/raw/abandoned.html"]


# --- _collect_files ---------------------------------------------------------


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """An ingest_sources-shaped tree containing one of everything excluded."""
    (tmp_path / "web" / "raw").mkdir(parents=True)
    (tmp_path / "pindata").mkdir()
    (tmp_path / ".hidden_dir").mkdir()

    (tmp_path / "opdb_changelog.json").write_bytes(b"real")
    (tmp_path / "web" / "cache.sqlite").write_bytes(b"real")
    (tmp_path / "web" / "raw" / "abc.html").write_bytes(b"real")

    (tmp_path / "manifest.json").write_bytes(b"skip")
    (tmp_path / ".DS_Store").write_bytes(b"skip")
    (tmp_path / ".env.local").write_bytes(b"skip")
    (tmp_path / "web" / "cache.sqlite-wal").write_bytes(b"skip")
    (tmp_path / "web" / "cache.sqlite-shm").write_bytes(b"skip")
    (tmp_path / "web" / "cache.sqlite-journal").write_bytes(b"skip")
    (tmp_path / "pindata" / "titles.json").write_bytes(b"skip")
    (tmp_path / ".hidden_dir" / "x.json").write_bytes(b"skip")
    return tmp_path


def test_collect_files_selects_only_transportable_files(tree):
    paths = [e["path"] for e in push._collect_files(tree)]
    assert paths == ["opdb_changelog.json", "web/cache.sqlite", "web/raw/abc.html"]


def test_collect_files_is_sorted_for_stable_manifests(tree):
    """The manifest is committed to R2 every push; unstable order churns it."""
    paths = [e["path"] for e in push._collect_files(tree)]
    assert paths == sorted(paths)


def test_collect_files_records_size_and_hash(tree):
    entry = next(
        e for e in push._collect_files(tree) if e["path"] == "web/cache.sqlite"
    )
    assert entry["size"] == 4
    assert entry["sha256"] == hashlib.sha256(b"real").hexdigest()

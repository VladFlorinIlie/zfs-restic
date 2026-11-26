import json
from pathlib import Path

from zfs_restic.jobs import summarize
from zfs_restic.restic import parse_ls_output
from zfs_restic.utils import human_bytes, parse_time
from zfs_restic.zfs import mount_point_for


def test_parse_time_handles_restic_nanoseconds():
    dt = parse_time("2026-11-25T03:00:12.123456789+02:00")
    assert (dt.year, dt.microsecond, dt.utcoffset().total_seconds()) == (2026, 123456, 7200)
    assert parse_time("2026-11-25T03:00:12Z").tzinfo is not None


def test_human_bytes():
    assert human_bytes(None) == "?"
    assert human_bytes(512) == "512 B"
    assert human_bytes(1536) == "1.5 KiB"
    assert human_bytes(5 * 1024 ** 4) == "5.0 TiB"


def test_mount_point_matches_v1_naming():
    # v1: Path(f"/mnt/restic_backup_{dataset.replace('/', '_')}") -- must never change.
    assert mount_point_for(Path("/mnt"), "tank/media/photos") == Path("/mnt/restic_backup_tank_media_photos")


def test_parse_ls_output_returns_direct_children_only():
    lines = [
        {"message_type": "snapshot", "struct_type": "snapshot", "id": "x"},
        {"struct_type": "node", "name": "d", "type": "dir", "path": "/m/d"},
        {"struct_type": "node", "name": "b.txt", "type": "file", "path": "/m/d/b.txt", "size": 3},
        {"struct_type": "node", "name": "A", "type": "dir", "path": "/m/d/A"},
        {"struct_type": "node", "name": "deep", "type": "file", "path": "/m/d/A/deep", "size": 1},
    ]
    out = parse_ls_output("\n".join(json.dumps(l) for l in lines), "/m/d/")
    assert [e["name"] for e in out] == ["A", "b.txt"]


def test_summarize_outcomes():
    ok = {"dataset": "a", "outcome": "success", "data_added": 10}
    bad = {"dataset": "b", "outcome": "failure", "error": "boom"}
    assert summarize([ok], [])[0] == "success"
    assert summarize([ok, bad], [])[0] == "partial"
    assert summarize([ok], ["prune failed"])[0] == "partial"
    assert summarize([bad], [])[0] == "failure"
    assert summarize([], ["no repo"])[0] == "failure"
    assert "b: boom" in summarize([ok, bad], [])[1]

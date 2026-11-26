"""ZFS snapshot and mount operations."""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Set

from .shell import Logger, capture, run_checked, stream

SNAPSHOT_PREFIX = "restic-backup-"
_OWN_SNAPSHOT_RE = re.compile(
    rf"^(?P<dataset>[^@]+)@{SNAPSHOT_PREFIX}\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}-\d{{2}}$")


def mount_point_for(mount_root: Path, dataset: str) -> Path:
    """Where a dataset's snapshot is mounted for backup.

    DO NOT CHANGE: this path is recorded in every restic snapshot. Parent lookup and
    `forget --group-by paths` (retention) depend on it staying identical to v1.
    """
    return mount_root / f"restic_backup_{dataset.replace('/', '_')}"


def new_snapshot_name(dataset: str) -> str:
    return f"{dataset}@{SNAPSHOT_PREFIX}{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"


def snapshot(full_name: str, log: Logger):
    # Non-recursive on purpose: only the dataset itself gets mounted and backed up.
    run_checked(["zfs", "snapshot", full_name], log)


def destroy(full_name: str, log: Logger) -> bool:
    rc, _ = stream(["zfs", "destroy", full_name], log)
    return rc == 0


def mount(full_name: str, mount_point: Path, log: Logger):
    run_checked(["mount", "-t", "zfs", full_name, str(mount_point)], log)


def unmount(mount_point: Path, log: Logger) -> bool:
    rc, _ = stream(["umount", str(mount_point)], log)
    return rc == 0


def mounted_paths(proc_mounts: Path = Path("/proc/mounts")) -> Set[str]:
    try:
        lines = proc_mounts.read_text().splitlines()
    except OSError:
        return set()
    return {parts[1] for parts in (line.split() for line in lines) if len(parts) > 1}


def leftover_snapshots(datasets: Iterable[str]) -> List[str]:
    """Snapshots created by this tool (by exact name pattern) on the given datasets or their children."""
    datasets = list(datasets)
    out = capture(["zfs", "list", "-H", "-t", "snapshot", "-o", "name"], timeout=120)
    found = []
    for name in out.split():
        m = _OWN_SNAPSHOT_RE.match(name)
        if m and any(m["dataset"] == ds or m["dataset"].startswith(ds + "/") for ds in datasets):
            found.append(name)
    return found


def list_filesystems() -> List[str]:
    out = capture(["zfs", "list", "-H", "-o", "name", "-t", "filesystem"], timeout=120)
    return [l.strip() for l in out.splitlines() if l.strip()]

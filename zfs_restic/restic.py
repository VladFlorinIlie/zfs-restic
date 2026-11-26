"""restic operations."""
from __future__ import annotations

import json
import os
import subprocess
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .shell import CommandError, Logger, capture, run_checked
from .utils import human_bytes, parse_time

MAX_LOGGED_FILE_ERRORS = 100

# restic exit codes
EXIT_OK = 0
EXIT_INCOMPLETE = 3  # snapshot created, but some source files could not be read


@dataclass
class BackupResult:
    returncode: int
    summary: Optional[Dict[str, Any]]
    file_errors: int = 0
    stderr_tail: List[str] = field(default_factory=list)


class Restic:
    def __init__(self, progress_fps: str = "0.5"):
        self.progress_fps = progress_fps

    # --- write / maintenance operations (used by jobs) ---
    def preflight(self, log: Logger):
        run_checked(["restic", "cat", "config"], log, quiet=True)
        # Removes only *stale* locks (dead process / older than 30 min), e.g. after a container crash.
        run_checked(["restic", "unlock"], log)

    def latest_snapshot_id(self, tag: str) -> Optional[str]:
        snaps = json.loads(capture(["restic", "snapshots", "--no-lock", "--tag", tag, "--json"]))
        if not snaps:
            return None
        return max(snaps, key=lambda s: parse_time(s["time"]))["short_id"]

    def backup(self, path: Path, tags: List[str], parent: Optional[str], log: Logger,
               on_progress: Callable[[Dict[str, Any]], None]) -> BackupResult:
        cmd = ["restic", "backup", "--json"]
        for tag in tags:
            cmd += ["--tag", tag]
        if parent:
            cmd += ["--parent", parent]
        cmd.append(str(path))
        log(f"$ {' '.join(cmd)}")

        env = {**os.environ, "RESTIC_PROGRESS_FPS": self.progress_fps}
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, bufsize=1)
        assert proc.stdout is not None and proc.stderr is not None
        stderr_lines: List[str] = []
        reader = threading.Thread(target=lambda: stderr_lines.extend(proc.stderr), daemon=True)
        reader.start()

        summary = None
        for line in proc.stdout:
            try:
                msg = json.loads(line)
            except ValueError:
                log(line.rstrip())
                continue
            kind = msg.get("message_type")
            if kind == "status":
                on_progress({
                    "percent": msg.get("percent_done"), "bytes_done": msg.get("bytes_done"),
                    "total_bytes": msg.get("total_bytes"), "files_done": msg.get("files_done"),
                    "total_files": msg.get("total_files"), "eta_seconds": msg.get("seconds_remaining"),
                    "elapsed_seconds": msg.get("seconds_elapsed"),
                    "current_files": msg.get("current_files") or [], "error_count": msg.get("error_count"),
                })
            elif kind == "summary":
                summary = msg
        rc = proc.wait()
        reader.join(timeout=10)

        file_errors = 0
        for line in stderr_lines:
            try:
                msg = json.loads(line)
                err = msg.get("error")
                text = err.get("message") if isinstance(err, dict) else str(err)
                item = f" ({msg['item']})" if msg.get("item") else ""
                file_errors += 1
                if file_errors <= MAX_LOGGED_FILE_ERRORS:
                    log(f"restic error: {text}{item}")
            except (ValueError, AttributeError):
                if line.strip():
                    log(line.rstrip())
        if file_errors > MAX_LOGGED_FILE_ERRORS:
            log(f"... {file_errors - MAX_LOGGED_FILE_ERRORS} more restic errors not shown")

        if summary:
            log(f"Snapshot {str(summary.get('snapshot_id', '?'))[:8]}: "
                f"{summary.get('files_new', 0)} new / {summary.get('files_changed', 0)} changed / "
                f"{summary.get('files_unmodified', 0)} unmodified files, "
                f"{human_bytes(summary.get('data_added'))} added, "
                f"{human_bytes(summary.get('total_bytes_processed'))} processed")
        elif rc != EXIT_OK:
            raise CommandError(cmd, rc, "".join(stderr_lines[-3:]))
        return BackupResult(rc, summary, file_errors, [l.rstrip() for l in stderr_lines[-3:]])

    def forget(self, retention_args: List[str], log: Logger):
        run_checked(["restic", "forget", "--prune", "--group-by", "paths"] + retention_args, log)

    def check(self, read_data_subset: Optional[str], log: Logger):
        cmd = ["restic", "check"]
        if read_data_subset:
            cmd += ["--read-data-subset", read_data_subset]
        run_checked(cmd, log)

    # --- read-only queries (used by the UI); --no-lock so they never block or wait on a job ---
    def snapshots(self) -> List[Dict[str, Any]]:
        snaps = json.loads(capture(["restic", "snapshots", "--no-lock", "--json"]))
        snaps.sort(key=lambda s: parse_time(s["time"]), reverse=True)
        return snaps

    def ls(self, snapshot_id: str, path: str) -> List[Dict[str, Any]]:
        """Direct children of `path` inside a snapshot."""
        out = capture(["restic", "ls", "--no-lock", "--json", snapshot_id, path], timeout=600)
        return parse_ls_output(out, path)

    @staticmethod
    def dump_command(snapshot_id: str, path: str, as_zip: bool) -> List[str]:
        return ["restic", "dump", "--no-lock"] + (["--archive", "zip"] if as_zip else []) + [snapshot_id, path]


def parse_ls_output(output: str, path: str) -> List[Dict[str, Any]]:
    norm = path.rstrip("/") or "/"
    entries = []
    for line in output.splitlines():
        try:
            node = json.loads(line)
        except ValueError:
            continue
        if (node.get("struct_type") or node.get("message_type")) != "node":
            continue
        node_path = node.get("path", "")
        if node_path == norm or (node_path.rsplit("/", 1)[0] or "/") != norm:
            continue
        entries.append({k: node.get(k) for k in ("name", "type", "path", "size", "mtime")})
    entries.sort(key=lambda e: (e["type"] != "dir", e["name"].lower()))
    return entries

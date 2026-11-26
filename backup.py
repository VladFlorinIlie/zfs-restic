from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
import yaml

from db import Database

CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", "/config/config.yml"))
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
MOUNT_ROOT = Path(os.environ.get("MOUNT_ROOT", "/mnt"))
HISTORY_LIMIT = int(os.environ.get("HISTORY_LIMIT", "500"))
MAX_AGE_HOURS = float(os.environ.get("MAX_AGE_HOURS", "36"))
SNAPSHOT_CACHE_TTL = int(os.environ.get("SNAPSHOT_CACHE_TTL", "600"))
HTTP_TIMEOUT = 15

SNAPSHOT_PREFIX = "restic-backup-"
LEFTOVER_RE = re.compile(rf"^(?P<dataset>[^@]+)@{SNAPSHOT_PREFIX}\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}}-\d{{2}}$")
LIVE_LOG_LINES = 2000
MAX_LOGGED_FILE_ERRORS = 100


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_time(value: str) -> datetime:
    """Parse restic's RFC3339 timestamps (nanosecond precision) into aware datetimes."""
    value = re.sub(r"(\.\d{6})\d+", r"\1", value.replace("Z", "+00:00"))
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def human_bytes(n: Optional[float]) -> str:
    if n is None:
        return "?"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n} B"


def mount_point_for(dataset: str) -> Path:
    # Must stay identical to the original naming: restic parent lookup and
    # `forget --group-by paths` both key off this path.
    return MOUNT_ROOT / f"restic_backup_{dataset.replace('/', '_')}"


class CommandError(Exception):
    def __init__(self, cmd: List[str], returncode: int, output: str = ""):
        self.cmd, self.returncode, self.output = cmd, returncode, output
        tail = output.strip().splitlines()[-3:]
        super().__init__(f"`{' '.join(cmd[:3])}` exited with {returncode}" + (f": {' | '.join(tail)}" if tail else ""))


# --- Run context: log lines go to stdout, the live status and the database ---

class Run:
    def __init__(self, manager: "BackupManager", run_id: int):
        self.manager, self.id = manager, run_id

    def log(self, message: str):
        for raw in str(message).rstrip("\n").splitlines() or [""]:
            line = f"[{datetime.now().strftime('%H:%M:%S')}] {raw}"
            print(line, flush=True)
            self.manager.live["log"].append(line)
            try:
                self.manager.db.append_log(self.id, line + "\n")
            except Exception as e:  # never let logging kill a backup
                print(f"!!! could not persist log line: {e}", flush=True)

    def task(self, text: str):
        self.manager.live["current_task"] = text
        self.log(f"== {text}")


def stream(cmd: List[str], log: Callable[[str], None], quiet: bool = False,
           env: Optional[Dict[str, str]] = None) -> Tuple[int, str]:
    """Run a command, streaming merged stdout/stderr line by line into the log."""
    log(f"$ {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            env=env or os.environ.copy(), bufsize=1)
    lines = []
    for line in proc.stdout:
        lines.append(line)
        if not quiet:
            log(line.rstrip())
    return proc.wait(), "".join(lines)


def run_checked(cmd: List[str], log: Callable[[str], None], quiet: bool = False) -> str:
    rc, out = stream(cmd, log, quiet=quiet)
    if rc != 0:
        if quiet:
            log(out.rstrip())
        raise CommandError(cmd, rc, out)
    return out


def capture(cmd: List[str], timeout: int = 300) -> str:
    """Run a command and return stdout; raises CommandError on failure."""
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if res.returncode != 0:
        raise CommandError(cmd, res.returncode, res.stderr or res.stdout)
    return res.stdout


# --- Notifications ---

def notify_gotify(title: str, message: str, priority: int, log: Callable[[str], None]):
    url, token = os.environ.get("GOTIFY_URL"), os.environ.get("GOTIFY_TOKEN")
    if not url or not token:
        return
    try:
        requests.post(url, params={"token": token}, timeout=HTTP_TIMEOUT,
                      json={"title": title, "message": message, "priority": priority}).raise_for_status()
        log(f"Sent Gotify notification: {title}")
    except Exception as e:
        log(f"!!! Could not send Gotify notification: {e}")


def ping(url_env: str, log: Callable[[str], None], body: str = ""):
    url = os.environ.get(url_env)
    if not url:
        return
    try:
        requests.post(url, data=body.encode()[:10000], timeout=HTTP_TIMEOUT).raise_for_status()
        log(f"Pinged {url_env}")
    except Exception as e:
        log(f"!!! Could not ping {url_env}: {e}")


# --- Manager ---

class BackupManager:
    def __init__(self):
        self.db = Database(DATA_DIR / "zfs-restic.db")
        n = self.db.mark_interrupted(now_iso())
        if n:
            print(f"Marked {n} run(s) left 'running' by a previous process as interrupted.", flush=True)
        self.db.trim(HISTORY_LIMIT)
        self.lock = threading.Lock()
        self.live: Dict[str, Any] = {
            "live_status": "idle", "job": None, "run_id": None, "trigger": None,
            "current_task": "N/A", "last_run_start_time": None,
            "dataset_index": None, "dataset_total": None, "progress": None,
            "log": deque(maxlen=LIVE_LOG_LINES),
        }
        self._snap_cache: Tuple[float, Optional[List[Dict[str, Any]]]] = (0.0, None)
        self._snap_lock = threading.Lock()

    # --- config ---
    @staticmethod
    def load_config() -> Dict[str, Any]:
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}

    def configured_datasets(self) -> List[str]:
        try:
            return list(self.load_config().get("datasets") or [])
        except Exception:
            return []

    # --- status ---
    def get_status(self, log_lines: int = 300) -> Dict[str, Any]:
        status = {k: v for k, v in self.live.items() if k != "log"}
        status["log"] = list(self.live["log"])[-log_lines:] if log_lines else []
        last = self.db.last_finished_run("backup")
        status["last_completed_run"] = (
            {"outcome": last["outcome"], "finish_time": last["finished_at"], "details": last["details"],
             "run_id": last["id"]}
            if last else {"outcome": "N/A", "finish_time": None, "details": "No backup has completed yet."}
        )
        return status

    # --- job control ---
    def start_job(self, kind: str, trigger: str = "api", **kwargs) -> Optional[int]:
        """Start a backup/check in the background. Returns the run id, or None if a job is already running."""
        if not self.lock.acquire(blocking=False):
            return None
        try:
            started = now_iso()
            run_id = self.db.create_run(kind, trigger, started)
            self.live.update(live_status="running", job=kind, run_id=run_id, trigger=trigger,
                             current_task="Starting", last_run_start_time=started,
                             dataset_index=None, dataset_total=None, progress=None)
            self.live["log"].clear()
            target = self._backup if kind == "backup" else self._check
            threading.Thread(target=self._job_wrapper, args=(target, Run(self, run_id), kwargs),
                             daemon=True, name=f"{kind}-{run_id}").start()
            return run_id
        except Exception:
            self.lock.release()
            raise

    # Back-compat with the original API
    def start_backup_job(self) -> bool:
        return self.start_job("backup") is not None

    def _job_wrapper(self, target, run: Run, kwargs):
        try:
            outcome, details = target(run, **kwargs)
        except Exception as e:  # last-resort guard; targets handle their own errors
            outcome, details = "failure", f"Unexpected error: {e}"
            run.log(f"!!! {details}")
        try:
            self.db.finish_run(run.id, outcome, details, now_iso())
            self.db.trim(HISTORY_LIMIT)
        except Exception as e:
            print(f"!!! could not persist run result: {e}", flush=True)
        run.log(f"Finished with outcome: {outcome}")
        self.invalidate_snapshots()
        self.live.update(live_status="idle", job=None, current_task="N/A", progress=None,
                         dataset_index=None, dataset_total=None)
        self.lock.release()

    # --- restic helpers ---
    def _restic_preflight(self, run: Run):
        run.task("Checking repository access")
        run_checked(["restic", "cat", "config"], run.log, quiet=True)
        # Removes only *stale* locks (dead process / older than 30 min) -- e.g. after a container crash.
        run_checked(["restic", "unlock"], run.log)

    def _cleanup_leftovers(self, run: Run, datasets: List[str]):
        """Remove ZFS snapshots/mounts left behind by a run that was killed mid-way."""
        mounts = Path("/proc/mounts").read_text().split("\n") if Path("/proc/mounts").exists() else []
        mounted = {line.split()[1] for line in mounts if len(line.split()) > 1}
        for ds in datasets:
            mp = mount_point_for(ds)
            if str(mp) in mounted:
                run.log(f"Found leftover mount {mp}, unmounting")
                stream(["umount", str(mp)], run.log)
        try:
            out = capture(["zfs", "list", "-H", "-t", "snapshot", "-o", "name"], timeout=120)
        except Exception as e:
            run.log(f"Could not list ZFS snapshots for leftover cleanup: {e}")
            return
        for name in out.split():
            m = LEFTOVER_RE.match(name)
            if m and any(m["dataset"] == ds or m["dataset"].startswith(ds + "/") for ds in datasets):
                run.log(f"Destroying leftover snapshot {name}")
                stream(["zfs", "destroy", name], run.log)

    def _find_parent(self, dataset: str, run: Run) -> Optional[str]:
        try:
            snaps = json.loads(capture(["restic", "snapshots", "--no-lock", "--tag", dataset, "--json"]))
            if snaps:
                return max(snaps, key=lambda s: parse_time(s["time"]))["short_id"]
        except Exception as e:
            run.log(f"Could not find parent snapshot for '{dataset}' ({e}); restic will pick one itself.")
        return None

    def _restic_backup(self, run: Run, path: Path, tags: List[str], parent: Optional[str]) -> Tuple[int, Optional[dict]]:
        cmd = ["restic", "backup", "--json"]
        for t in tags:
            cmd += ["--tag", t]
        if parent:
            cmd += ["--parent", parent]
            run.log(f"Using parent snapshot {parent}")
        cmd.append(str(path))
        run.log(f"$ {' '.join(cmd)}")

        env = {**os.environ, "RESTIC_PROGRESS_FPS": os.environ.get("RESTIC_PROGRESS_FPS", "0.5")}
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, bufsize=1)
        stderr_lines: List[str] = []
        reader = threading.Thread(target=lambda: stderr_lines.extend(proc.stderr), daemon=True)
        reader.start()

        summary, file_errors = None, 0
        for line in proc.stdout:
            try:
                msg = json.loads(line)
            except ValueError:
                run.log(line.rstrip())
                continue
            mt = msg.get("message_type")
            if mt == "status":
                self.live["progress"] = {
                    "percent": msg.get("percent_done"), "bytes_done": msg.get("bytes_done"),
                    "total_bytes": msg.get("total_bytes"), "files_done": msg.get("files_done"),
                    "total_files": msg.get("total_files"), "eta_seconds": msg.get("seconds_remaining"),
                    "elapsed_seconds": msg.get("seconds_elapsed"), "current_files": msg.get("current_files") or [],
                    "error_count": msg.get("error_count"),
                }
            elif mt == "summary":
                summary = msg
        rc = proc.wait()
        reader.join(timeout=10)

        for line in stderr_lines:
            try:
                msg = json.loads(line)
                err = msg.get("error")
                text = err.get("message") if isinstance(err, dict) else str(err)
                item = f" ({msg.get('item')})" if msg.get("item") else ""
                file_errors += 1
                if file_errors <= MAX_LOGGED_FILE_ERRORS:
                    run.log(f"restic error: {text}{item}")
            except (ValueError, AttributeError):
                if line.strip():
                    run.log(line.rstrip())
        if file_errors > MAX_LOGGED_FILE_ERRORS:
            run.log(f"... {file_errors - MAX_LOGGED_FILE_ERRORS} more restic errors not shown")
        if summary:
            run.log(
                f"Snapshot {summary.get('snapshot_id', '?')[:8]}: "
                f"{summary.get('files_new', 0)} new / {summary.get('files_changed', 0)} changed / "
                f"{summary.get('files_unmodified', 0)} unmodified files, "
                f"{human_bytes(summary.get('data_added'))} added, "
                f"{human_bytes(summary.get('total_bytes_processed'))} processed")
        if rc != 0 and not summary:
            raise CommandError(cmd, rc, "".join(stderr_lines[-3:]))
        return rc, summary

    # --- backup job ---
    def _backup_dataset(self, run: Run, dataset: str) -> Dict[str, Any]:
        result: Dict[str, Any] = {"dataset": dataset, "started_at": now_iso()}
        snapshot_name = f"{SNAPSHOT_PREFIX}{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        full_snapshot = f"{dataset}@{snapshot_name}"
        mount_point = mount_point_for(dataset)
        created = mounted = False
        try:
            mount_point.mkdir(parents=True, exist_ok=True)
            # Non-recursive on purpose: only the dataset itself is mounted and backed up,
            # so recursive child snapshots were never part of the backup anyway.
            run_checked(["zfs", "snapshot", full_snapshot], run.log)
            created = True
            run_checked(["mount", "-t", "zfs", full_snapshot, str(mount_point)], run.log)
            mounted = True
            parent = self._find_parent(dataset, run)
            rc, summary = self._restic_backup(run, mount_point, [dataset, full_snapshot], parent)
            if summary:
                result.update(snapshot_id=summary.get("snapshot_id"), files_new=summary.get("files_new"),
                              files_changed=summary.get("files_changed"),
                              files_unmodified=summary.get("files_unmodified"),
                              data_added=summary.get("data_added"),
                              total_bytes=summary.get("total_bytes_processed"))
            if rc == 0:
                result["outcome"] = "success"
            elif rc == 3:
                result.update(outcome="partial", error="Snapshot created, but some files could not be read")
            else:
                result.update(outcome="failure", error=f"restic backup exited with {rc}")
        except Exception as e:
            result.update(outcome="failure", error=str(e))
            run.log(f"!!! {dataset}: {e}")
        finally:
            self.live["progress"] = None
            if mounted:
                rc, _ = stream(["umount", str(mount_point)], run.log)
                if rc != 0:
                    run.log(f"Cleanup warning: could not unmount {mount_point}")
            if created:
                rc, _ = stream(["zfs", "destroy", full_snapshot], run.log)
                if rc != 0:
                    run.log(f"Cleanup warning: could not destroy {full_snapshot} (will retry next run)")
            try:
                mount_point.rmdir()
            except OSError as e:
                run.log(f"Cleanup warning: could not remove {mount_point}: {e}")
        result["finished_at"] = now_iso()
        return result

    def _backup(self, run: Run) -> Tuple[str, str]:
        ping("HEALTHCHECK_START_URL", run.log)
        results: List[Dict[str, Any]] = []
        extra_problems: List[str] = []
        try:
            run.task("Reading config")
            config = self.load_config()
            datasets = list(config.get("datasets") or [])
            if not datasets:
                raise ValueError(f"No datasets configured in {CONFIG_PATH}")
            self._restic_preflight(run)
            run.task("Cleaning up leftovers from interrupted runs")
            self._cleanup_leftovers(run, datasets)

            self.live["dataset_total"] = len(datasets)
            for i, dataset in enumerate(datasets, 1):
                self.live["dataset_index"] = i
                run.task(f"Backing up {dataset} ({i}/{len(datasets)})")
                result = self._backup_dataset(run, dataset)
                results.append(result)
                self.db.add_dataset_result(run.id, result)

            retention = config.get("retention") or {}
            if retention and any(r["outcome"] != "failure" for r in results):
                run.task("Applying retention policy (forget + prune)")
                args = [a for k, v in retention.items() for a in (f"--{k}", str(v))]
                try:
                    run_checked(["restic", "forget", "--prune", "--group-by", "paths"] + args, run.log)
                except Exception as e:
                    extra_problems.append(f"Retention/prune failed: {e}")
                    run.log(f"!!! {extra_problems[-1]}")
            elif retention:
                run.log("Skipping retention: no dataset was backed up successfully.")
        except Exception as e:
            extra_problems.append(str(e))
            run.log(f"!!! {e}")

        ok = [r for r in results if r["outcome"] == "success"]
        failed = [r for r in results if r["outcome"] == "failure"]
        if results and len(ok) == len(results) and not extra_problems:
            outcome = "success"
        elif not results or len(failed) == len(results):
            outcome = "failure"
        else:
            outcome = "partial"

        icon = {"success": "✅", "partial": "⚠️", "failure": "❌"}
        lines = [f"{icon[r['outcome']]} {r['dataset']}: " + " — ".join(
                     ([f"+{human_bytes(r['data_added'])}"] if r.get("data_added") is not None else [])
                     + ([r["error"]] if r.get("error") else [])) for r in results]
        lines += [f"❌ {p}" for p in extra_problems]
        details = "\n".join(lines) or "Nothing was backed up."
        title = {"success": "✅ Backup succeeded", "partial": "⚠️ Backup partially failed",
                 "failure": "❌ Backup FAILED"}[outcome]

        if outcome != "success" or os.environ.get("GOTIFY_NOTIFY_SUCCESS", "true").lower() != "false":
            notify_gotify(title, details, {"success": 3, "partial": 7, "failure": 8}[outcome], run.log)
        ping("HEALTHCHECK_URL" if outcome == "success" else "HEALTHCHECK_FAIL_URL", run.log, details)
        return outcome, details

    # --- check job ---
    def _check(self, run: Run, read_data_subset: Optional[str] = None) -> Tuple[str, str]:
        try:
            if read_data_subset is None:
                read_data_subset = (self.load_config().get("check") or {}).get("read_data_subset")
            self._restic_preflight(run)
            cmd = ["restic", "check"]
            if read_data_subset:
                cmd += ["--read-data-subset", str(read_data_subset)]
            run.task("Verifying repository" + (f" (reading {read_data_subset} of data)" if read_data_subset else ""))
            run_checked(cmd, run.log)
            outcome, details = "success", "Repository check passed" + (
                f" (read {read_data_subset} of pack data)" if read_data_subset else " (metadata only)")
        except Exception as e:
            outcome, details = "failure", f"Repository check failed: {e}"
            run.log(f"!!! {details}")
        if outcome != "success" or os.environ.get("GOTIFY_NOTIFY_SUCCESS", "true").lower() != "false":
            notify_gotify("✅ Repository check passed" if outcome == "success" else "❌ Repository check FAILED",
                          details, 3 if outcome == "success" else 8, run.log)
        return outcome, details

    # --- read-only repository queries (used by the UI) ---
    def invalidate_snapshots(self):
        self._snap_cache = (0.0, None)

    def get_current_snapshots(self, max_age: int = SNAPSHOT_CACHE_TTL) -> List[Dict[str, Any]]:
        with self._snap_lock:
            ts, cached = self._snap_cache
            if cached is not None and time.time() - ts < max_age:
                return cached
            snaps = json.loads(capture(["restic", "snapshots", "--no-lock", "--json"]))
            snaps.sort(key=lambda s: parse_time(s["time"]), reverse=True)
            self._snap_cache = (time.time(), snaps)
            return snaps

    def health(self) -> Dict[str, Any]:
        """Per-dataset age of the newest restic snapshot -- the repository is the source of truth."""
        datasets = self.configured_datasets()
        snaps = self.get_current_snapshots()
        now = datetime.now(timezone.utc)
        report = []
        for ds in datasets:
            mine = [s for s in snaps if ds in (s.get("tags") or [])]
            latest = mine[0] if mine else None
            age_h = (now - parse_time(latest["time"])).total_seconds() / 3600 if latest else None
            report.append({
                "dataset": ds, "snapshots": len(mine),
                "latest_time": latest["time"] if latest else None,
                "latest_id": latest["short_id"] if latest else None,
                "latest_data_added": ((latest or {}).get("summary") or {}).get("data_added"),
                "latest_total_bytes": ((latest or {}).get("summary") or {}).get("total_bytes_processed"),
                "age_hours": round(age_h, 2) if age_h is not None else None,
                "stale": age_h is None or age_h > MAX_AGE_HOURS,
            })
        return {"ok": bool(report) and not any(r["stale"] for r in report),
                "max_age_hours": MAX_AGE_HOURS, "datasets": report}

    def list_dir(self, snapshot_id: str, path: str) -> List[Dict[str, Any]]:
        out = capture(["restic", "ls", "--no-lock", "--json", snapshot_id, path], timeout=600)
        norm = path.rstrip("/") or "/"
        entries = []
        for line in out.splitlines():
            try:
                node = json.loads(line)
            except ValueError:
                continue
            if (node.get("struct_type") or node.get("message_type")) != "node":
                continue
            p = node.get("path", "")
            if p == norm or (p.rsplit("/", 1)[0] or "/") != norm:
                continue
            entries.append({k: node.get(k) for k in ("name", "type", "path", "size", "mtime")})
        entries.sort(key=lambda e: (e["type"] != "dir", e["name"].lower()))
        return entries

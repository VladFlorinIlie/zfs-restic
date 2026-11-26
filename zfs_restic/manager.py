"""Owns the job lock, live status, history database and cached repository queries."""
from __future__ import annotations

import os
import re
import tempfile
import threading
import time
from collections import deque
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import jobs, rclone
from .config import load_config
from .config_store import ConfigStore
from .notify import Notifier
from .restic import Restic
from .settings import Settings
from .storage import Database
from .utils import now_iso, parse_time

LIVE_LOG_LINES = 2000
FIND_MAX_MATCHES = 5000
JOBS: Dict[str, Callable[..., jobs.JobOutcome]] = {
    "backup": jobs.run_backup, "check": jobs.run_check, "restore": jobs.run_restore,
}


class RestoreUnavailable(Exception):
    pass


class _TTLCache:
    def __init__(self):
        self._value: Any = None
        self._ts = 0.0
        self._lock = threading.Lock()

    def get(self, ttl: float, compute: Callable[[], Any]) -> Any:
        with self._lock:
            if self._value is not None and time.time() - self._ts < ttl:
                return self._value
            self._value, self._ts = compute(), time.time()
            return self._value

    def clear(self):
        self._value, self._ts = None, 0.0


class BackupManager:
    def __init__(self, settings: Settings, restic: Optional[Restic] = None, notifier: Optional[Notifier] = None):
        self.settings = settings
        self.restic = restic or Restic(settings.progress_fps)
        self.notifier = notifier or Notifier(settings)
        self.history_warning: Optional[str] = None
        try:
            self.db = Database(settings.db_path)
        except Exception as e:
            # Never let an unwritable DATA_DIR stop backups: fall back to a throwaway database.
            fallback = Path(tempfile.gettempdir()) / "zfs-restic-history.db"
            self.history_warning = (f"Run history is NOT persistent: cannot write {settings.db_path} ({e}). "
                                    "Mount a writable folder at /data or set DATA_DIR to one.")
            print(f"WARNING: {self.history_warning}", flush=True)
            self.db = Database(fallback)
        self.config_store = ConfigStore(settings.config_path, self.db)

        n = self.db.mark_interrupted(now_iso())
        if n:
            print(f"Marked {n} run(s) left 'running' by a previous process as interrupted.", flush=True)
        self.db.trim(settings.history_limit)

        self._lock = threading.Lock()
        self.live: Dict[str, Any] = {
            "live_status": "idle", "job": None, "run_id": None, "trigger": None,
            "current_task": "N/A", "last_run_start_time": None,
            "dataset_index": None, "dataset_total": None, "progress": None,
            "log": deque(maxlen=LIVE_LOG_LINES),
        }
        self._snapshots = _TTLCache()
        self._storage = _TTLCache()
        # Snapshots are immutable, so listings and diffs can be cached for the life of the process.
        self.list_dir = lru_cache(maxsize=128)(self.restic.ls)
        self.diff = lru_cache(maxsize=32)(self.restic.diff)

    # --- jobs ---
    @property
    def busy(self) -> bool:
        return self._lock.locked()

    def start_job(self, kind: str, trigger: str = "api", **kwargs) -> Optional[int]:
        """Start a job in the background. Returns the run id, or None if a job is already running."""
        if kind not in JOBS:
            raise ValueError(f"unknown job kind: {kind}")
        if not self._lock.acquire(blocking=False):
            return None
        try:
            started = now_iso()
            run_id = self.db.create_run(kind, trigger, started)
            self.live.update(live_status="running", job=kind, run_id=run_id, trigger=trigger,
                             current_task="Starting", last_run_start_time=started,
                             dataset_index=None, dataset_total=None, progress=None)
            self.live["log"].clear()
            ctx = jobs.JobContext(run_id, self.settings, self.db, self.restic, self.notifier, self.live,
                                  storage_info=self.storage_info)
            threading.Thread(target=self._run_job, args=(JOBS[kind], ctx, kwargs),
                             daemon=True, name=f"{kind}-{run_id}").start()
            return run_id
        except Exception:
            self._lock.release()
            raise

    def _run_job(self, job: Callable[..., jobs.JobOutcome], ctx: jobs.JobContext, kwargs: Dict[str, Any]):
        try:
            outcome, details = job(ctx, **kwargs)
        except Exception as e:  # last-resort guard; jobs handle their own errors
            outcome, details = "failure", f"Unexpected error: {e}"
            ctx.log(f"!!! {details}")
        try:
            ctx.log(f"Finished with outcome: {outcome}")
            self.db.finish_run(ctx.run_id, outcome, details, now_iso())
            self.db.trim(self.settings.history_limit)
        except Exception as e:
            print(f"!!! could not persist run result: {e}", flush=True)
        finally:
            self.invalidate_snapshots()
            self.live.update(live_status="idle", job=None, current_task="N/A", progress=None,
                             dataset_index=None, dataset_total=None)
            self._lock.release()

    def wait_idle(self, timeout: float = 600) -> bool:
        """Block until no job is running (used by tests)."""
        deadline = time.time() + timeout
        while self.busy and time.time() < deadline:
            time.sleep(0.2)
        return not self.busy

    # --- restore ---
    def restore_available(self) -> Tuple[bool, str]:
        d = self.settings.restore_dir
        if not d.is_dir():
            # Create e.g. /mnt/tank/restores when its parent is a mounted pool/dataset, but never
            # create top-level dirs like /restore inside the container (they'd vanish with it).
            if d.parent != Path("/") and d.parent.is_dir() and os.access(d.parent, os.W_OK):
                try:
                    d.mkdir()
                except OSError as e:
                    return False, f"Could not create {d}: {e}"
            else:
                return False, (f"Set RESTORE_DIR to a folder on a mounted pool (e.g. /mnt/<pool>/restores) "
                               f"or mount a volume at {d} to enable restores to the server.")
        if not os.access(d, os.W_OK):
            return False, f"{d} is not writable."
        return True, str(d)

    def start_restore(self, snapshot_id: str, path: str, is_dir: bool, trigger: str = "ui") -> Optional[Dict[str, Any]]:
        ok, msg = self.restore_available()
        if not ok:
            raise RestoreUnavailable(msg)
        snap = next((s for s in self.snapshots() if s["id"].startswith(snapshot_id)), None)
        if snap is None:
            raise ValueError(f"snapshot {snapshot_id} not found")
        dataset = next((t for t in snap.get("tags") or [] if "@" not in t), "snapshot").replace("/", "_")
        name = path.rstrip("/").rsplit("/", 1)[-1] or dataset
        if path.rstrip("/") in snap.get("paths", []):
            name = dataset
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        base = self.settings.restore_dir / f"{dataset}_{snap['short_id']}_{stamp}"
        target = base / name if is_dir else base
        run_id = self.start_job("restore", trigger=trigger, snapshot_id=snap["id"], path=path,
                                is_dir=is_dir, target=str(target))
        if run_id is None:
            return None
        return {"run_id": run_id, "target": str(target if is_dir else base / name)}

    # --- status ---
    def get_status(self, log_lines: int = 300) -> Dict[str, Any]:
        status = {k: v for k, v in self.live.items() if k != "log"}
        status["log"] = list(self.live["log"])[-log_lines:] if log_lines > 0 else []
        last = self.db.last_finished_run("backup")
        status["last_completed_run"] = (
            {"outcome": last["outcome"], "finish_time": last["finished_at"], "details": last["details"],
             "run_id": last["id"]}
            if last else {"outcome": "N/A", "finish_time": None, "details": "No backup has completed yet."}
        )
        return status

    # --- repository queries ---
    def invalidate_snapshots(self):
        self._snapshots.clear()

    def snapshots(self, max_age: Optional[int] = None) -> List[Dict[str, Any]]:
        ttl = self.settings.snapshot_cache_ttl if max_age is None else max_age
        return self._snapshots.get(ttl, self.restic.snapshots)

    def storage_info(self, refresh: bool = False) -> Optional[Dict[str, Any]]:
        """Usage of the rclone remote holding the repository (None for non-rclone repositories)."""
        remote = rclone.remote_for_repository()
        if not remote:
            return None

        def compute():
            about = rclone.about(remote)
            total, used = about.get("total"), about.get("used")
            pct = round(used / total * 100, 1) if total and used is not None else None
            return {"remote": remote, **about, "percent_used": pct,
                    "warn_percent": self.settings.quota_warn_percent,
                    "warning": pct is not None and pct >= self.settings.quota_warn_percent,
                    "checked_at": now_iso()}
        if refresh:
            self._storage.clear()
        return self._storage.get(self.settings.snapshot_cache_ttl, compute)

    def health(self) -> Dict[str, Any]:
        """Per-dataset age of the newest restic snapshot -- the repository is the source of truth."""
        datasets = load_config(self.settings.config_path).datasets
        snaps = self.snapshots()
        now = datetime.now(timezone.utc)
        report = []
        for ds in datasets:
            mine = [s for s in snaps if ds in (s.get("tags") or [])]  # newest first
            latest = mine[0] if mine else None
            summary = (latest or {}).get("summary") or {}
            age_h = (now - parse_time(latest["time"])).total_seconds() / 3600 if latest else None
            report.append({
                "dataset": ds, "snapshots": len(mine),
                "latest_time": latest["time"] if latest else None,
                "latest_id": latest["short_id"] if latest else None,
                "latest_data_added": summary.get("data_added"),
                "latest_total_bytes": summary.get("total_bytes_processed"),
                "age_hours": round(age_h, 2) if age_h is not None else None,
                "stale": age_h is None or age_h > self.settings.max_age_hours,
            })
        storage = None
        try:
            storage = self.storage_info()
        except Exception as e:
            storage = {"error": str(e)}
        ok = bool(report) and not any(r["stale"] for r in report) and not (storage or {}).get("warning")
        return {"ok": ok, "max_age_hours": self.settings.max_age_hours, "datasets": report, "storage": storage}

    def find(self, pattern: str, dataset: Optional[str] = None, ignore_case: bool = False) -> Dict[str, Any]:
        """Search all snapshots; results grouped by path with the snapshots each version appears in."""
        raw = self.restic.find(pattern, tag=dataset or None, ignore_case=ignore_case)
        snaps = {s["id"]: s for s in self.snapshots()}
        by_path: Dict[str, Dict[str, Any]] = {}
        matches = 0
        truncated = False
        for block in raw:
            snap = snaps.get(block.get("snapshot"), {})
            for m in block.get("matches") or []:
                if matches >= FIND_MAX_MATCHES:
                    truncated = True
                    break
                matches += 1
                entry = by_path.setdefault(m["path"], {"path": m["path"], "type": m.get("type"), "seen": []})
                entry["seen"].append({"snapshot_id": block.get("snapshot"), "short_id": snap.get("short_id"),
                                      "time": snap.get("time"), "size": m.get("size"), "mtime": m.get("mtime")})
        results = sorted(by_path.values(), key=lambda r: r["path"])
        for r in results:
            r["seen"].sort(key=lambda x: x["time"] or "", reverse=True)
        return {"results": results, "matches": matches, "truncated": truncated,
                "snapshots_searched": len(raw)}

    def retention_preview(self, retention_args: List[str]) -> Dict[str, Any]:
        if not retention_args:
            return {"groups": [], "note": "No retention policy configured: nothing would be removed."}
        groups = []
        for g in self.restic.forget_preview(retention_args):
            reasons = {r["snapshot"]["id"]: r.get("matches", []) for r in g.get("reasons") or []
                       if isinstance(r.get("snapshot"), dict)}
            tag = next((t for s in (g.get("keep") or []) + (g.get("remove") or [])
                        for t in s.get("tags") or [] if "@" not in t), None)
            groups.append({
                "paths": g.get("paths"), "dataset": tag,
                "keep": [{"short_id": s["short_id"], "time": s["time"], "reasons": reasons.get(s["id"], [])}
                         for s in g.get("keep") or []],
                "remove": [{"short_id": s["short_id"], "time": s["time"]} for s in g.get("remove") or []],
            })
        return {"groups": groups}

"""Owns the job lock, live status, history database and cached repository queries."""
from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import jobs
from .config import load_config
from .notify import Notifier
from .restic import Restic
from .settings import Settings
from .storage import Database
from .utils import now_iso, parse_time

LIVE_LOG_LINES = 2000
JOBS: Dict[str, Callable[..., jobs.JobOutcome]] = {"backup": jobs.run_backup, "check": jobs.run_check}


class BackupManager:
    def __init__(self, settings: Settings, restic: Optional[Restic] = None, notifier: Optional[Notifier] = None):
        self.settings = settings
        self.restic = restic or Restic(settings.progress_fps)
        self.notifier = notifier or Notifier(settings)
        self.db = Database(settings.db_path)

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
        self._snap_cache: Tuple[float, Optional[List[Dict[str, Any]]]] = (0.0, None)
        self._snap_lock = threading.Lock()
        # Snapshots are immutable, so directory listings can be cached for the life of the process.
        self.list_dir = lru_cache(maxsize=128)(self.restic.ls)

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
            ctx = jobs.JobContext(run_id, self.settings, self.db, self.restic, self.notifier, self.live)
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
        self._snap_cache = (0.0, None)

    def snapshots(self, max_age: Optional[int] = None) -> List[Dict[str, Any]]:
        max_age = self.settings.snapshot_cache_ttl if max_age is None else max_age
        with self._snap_lock:
            ts, cached = self._snap_cache
            if cached is not None and time.time() - ts < max_age:
                return cached
            snaps = self.restic.snapshots()
            self._snap_cache = (time.time(), snaps)
            return snaps

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
        return {"ok": bool(report) and not any(r["stale"] for r in report),
                "max_age_hours": self.settings.max_age_hours, "datasets": report}

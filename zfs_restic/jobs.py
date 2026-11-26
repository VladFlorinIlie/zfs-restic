"""The backup and check jobs. Each job returns (outcome, details)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from . import zfs
from .config import BackupConfig, load_config
from .notify import Notifier
from .restic import EXIT_INCOMPLETE, EXIT_OK, Restic
from .settings import Settings
from .storage import Database
from .utils import human_bytes, now_iso

JobOutcome = Tuple[str, str]


@dataclass
class JobContext:
    run_id: int
    settings: Settings
    db: Database
    restic: Restic
    notifier: Notifier
    live: Dict[str, Any]

    def log(self, message: str):
        for raw in str(message).rstrip("\n").splitlines() or [""]:
            line = f"[{datetime.now().strftime('%H:%M:%S')}] {raw}"
            print(line, flush=True)
            self.live["log"].append(line)
            try:
                self.db.append_log(self.run_id, line + "\n")
            except Exception as e:  # never let logging kill a backup
                print(f"!!! could not persist log line: {e}", flush=True)

    def task(self, text: str):
        self.live["current_task"] = text
        self.log(f"== {text}")

    def set_progress(self, progress: Optional[Dict[str, Any]]):
        self.live["progress"] = progress


# --- backup ---

def cleanup_leftovers(ctx: JobContext, datasets: List[str]):
    """Remove ZFS mounts/snapshots left behind by a run that was killed mid-way."""
    mounted = zfs.mounted_paths()
    for ds in datasets:
        mp = zfs.mount_point_for(ctx.settings.mount_root, ds)
        if str(mp) in mounted:
            ctx.log(f"Found leftover mount {mp}, unmounting")
            zfs.unmount(mp, ctx.log)
    try:
        leftovers = zfs.leftover_snapshots(datasets)
    except Exception as e:
        ctx.log(f"Could not list ZFS snapshots for leftover cleanup: {e}")
        return
    for name in leftovers:
        ctx.log(f"Destroying leftover snapshot {name}")
        zfs.destroy(name, ctx.log)


def backup_dataset(ctx: JobContext, dataset: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {"dataset": dataset, "started_at": now_iso()}
    full_snapshot = zfs.new_snapshot_name(dataset)
    mount_point = zfs.mount_point_for(ctx.settings.mount_root, dataset)
    created = mounted = False
    try:
        mount_point.mkdir(parents=True, exist_ok=True)
        zfs.snapshot(full_snapshot, ctx.log)
        created = True
        zfs.mount(full_snapshot, mount_point, ctx.log)
        mounted = True

        try:
            parent = ctx.restic.latest_snapshot_id(dataset)
            if parent:
                ctx.log(f"Using parent snapshot {parent}")
        except Exception as e:
            parent = None
            ctx.log(f"Could not find parent snapshot for '{dataset}' ({e}); restic will pick one itself.")

        # Tags are [dataset, full ZFS snapshot name] -- same as v1, parent lookup relies on the first one.
        res = ctx.restic.backup(mount_point, [dataset, full_snapshot], parent, ctx.log, ctx.set_progress)
        if res.summary:
            s = res.summary
            result.update(snapshot_id=s.get("snapshot_id"), files_new=s.get("files_new"),
                          files_changed=s.get("files_changed"), files_unmodified=s.get("files_unmodified"),
                          data_added=s.get("data_added"), total_bytes=s.get("total_bytes_processed"))
        if res.returncode == EXIT_OK:
            result["outcome"] = "success"
        elif res.returncode == EXIT_INCOMPLETE:
            result.update(outcome="partial", error="Snapshot created, but some files could not be read")
        else:
            result.update(outcome="failure", error=f"restic backup exited with {res.returncode}")
    except Exception as e:
        result.update(outcome="failure", error=str(e))
        ctx.log(f"!!! {dataset}: {e}")
    finally:
        ctx.set_progress(None)
        if mounted and not zfs.unmount(mount_point, ctx.log):
            ctx.log(f"Cleanup warning: could not unmount {mount_point}")
        if created and not zfs.destroy(full_snapshot, ctx.log):
            ctx.log(f"Cleanup warning: could not destroy {full_snapshot} (will retry next run)")
        try:
            mount_point.rmdir()
        except OSError as e:
            ctx.log(f"Cleanup warning: could not remove {mount_point}: {e}")
    result["finished_at"] = now_iso()
    return result


def summarize(results: List[Dict[str, Any]], problems: List[str]) -> JobOutcome:
    ok = [r for r in results if r["outcome"] == "success"]
    failed = [r for r in results if r["outcome"] == "failure"]
    if results and len(ok) == len(results) and not problems:
        outcome = "success"
    elif not results or len(failed) == len(results):
        outcome = "failure"
    else:
        outcome = "partial"

    icon = {"success": "✅", "partial": "⚠️", "failure": "❌"}
    lines = []
    for r in results:
        parts = ([f"+{human_bytes(r['data_added'])}"] if r.get("data_added") is not None else []) \
            + ([r["error"]] if r.get("error") else [])
        lines.append(f"{icon[r['outcome']]} {r['dataset']}: " + " — ".join(parts))
    lines += [f"❌ {p}" for p in problems]
    return outcome, "\n".join(lines) or "Nothing was backed up."


def run_backup(ctx: JobContext) -> JobOutcome:
    ctx.notifier.ping("start", ctx.log)
    results: List[Dict[str, Any]] = []
    problems: List[str] = []
    try:
        ctx.task("Reading config")
        config: BackupConfig = load_config(ctx.settings.config_path)
        if not config.datasets:
            raise ValueError(f"No datasets configured in {ctx.settings.config_path}")

        ctx.task("Checking repository access")
        ctx.restic.preflight(ctx.log)
        ctx.task("Cleaning up leftovers from interrupted runs")
        cleanup_leftovers(ctx, config.datasets)

        ctx.live["dataset_total"] = len(config.datasets)
        for i, dataset in enumerate(config.datasets, 1):
            ctx.live["dataset_index"] = i
            ctx.task(f"Backing up {dataset} ({i}/{len(config.datasets)})")
            result = backup_dataset(ctx, dataset)
            results.append(result)
            ctx.db.add_dataset_result(ctx.run_id, result)

        if config.retention and any(r["outcome"] != "failure" for r in results):
            ctx.task("Applying retention policy (forget + prune)")
            try:
                ctx.restic.forget(config.retention_args(), ctx.log)
            except Exception as e:
                problems.append(f"Retention/prune failed: {e}")
                ctx.log(f"!!! {problems[-1]}")
        elif config.retention:
            ctx.log("Skipping retention: no dataset was backed up successfully.")
    except Exception as e:
        problems.append(str(e))
        ctx.log(f"!!! {e}")

    outcome, details = summarize(results, problems)
    title = {"success": "✅ Backup succeeded", "partial": "⚠️ Backup partially failed",
             "failure": "❌ Backup FAILED"}[outcome]
    ctx.notifier.gotify(title, details, {"success": 3, "partial": 7, "failure": 8}[outcome], ctx.log,
                        success=outcome == "success")
    ctx.notifier.ping("success" if outcome == "success" else "fail", ctx.log, details)
    return outcome, details


# --- check ---

def run_check(ctx: JobContext, read_data_subset: Optional[str] = None) -> JobOutcome:
    try:
        if read_data_subset is None:
            read_data_subset = load_config(ctx.settings.config_path).check_read_data_subset
        ctx.task("Checking repository access")
        ctx.restic.preflight(ctx.log)
        ctx.task("Verifying repository" + (f" (reading {read_data_subset} of data)" if read_data_subset else ""))
        ctx.restic.check(read_data_subset or None, ctx.log)
        outcome = "success"
        details = "Repository check passed" + (
            f" (read {read_data_subset} of pack data)" if read_data_subset else " (metadata only)")
    except Exception as e:
        outcome, details = "failure", f"Repository check failed: {e}"
        ctx.log(f"!!! {details}")
    ctx.notifier.gotify("✅ Repository check passed" if outcome == "success" else "❌ Repository check FAILED",
                        details, 3 if outcome == "success" else 8, ctx.log, success=outcome == "success")
    return outcome, details

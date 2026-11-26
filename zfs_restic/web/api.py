"""HTTP routes. `/backup`, `/status` and `/snapshots` keep their v1 behaviour for existing cron jobs."""
from __future__ import annotations

import re
import subprocess
from urllib.parse import quote

from flask import Blueprint, Response, current_app, jsonify, request

from ..config import ConfigError
from ..manager import BackupManager
from ..shell import CommandError

bp = Blueprint("api", __name__)

SNAPSHOT_ID_RE = re.compile(r"^[0-9a-f]{8,64}$")
SUBSET_RE = re.compile(r"\d+(\.\d+)?%|\d+/\d+|\d+[KMGT]")


def manager() -> BackupManager:
    return current_app.extensions["zfs_restic"]


def error(message: str, code: int, details: str = ""):
    return jsonify({"status": "error", "message": message, "details": details}), code


def trigger() -> str:
    return "ui" if request.args.get("trigger") == "ui" else "api"


def snapshot_and_path(snapshot_id: str):
    path = request.args.get("path", "/")
    if not SNAPSHOT_ID_RE.match(snapshot_id):
        return None, error("Invalid snapshot id.", 400)
    if not path.startswith("/") or "\x00" in path:
        return None, error("Path must be absolute.", 400)
    return path, None


@bp.get("/")
def index():
    return current_app.send_static_file("index.html")


@bp.get("/healthz")
def healthz():
    return jsonify({"status": "ok"})


# --- jobs ---

@bp.post("/backup")
@bp.post("/api/backup")
def start_backup():
    run_id = manager().start_job("backup", trigger=trigger())
    if run_id is None:
        return jsonify({"status": "error", "message": "A job is already in progress."}), 409
    return jsonify({"status": "success", "message": "Backup process started in the background.",
                    "run_id": run_id}), 202


@bp.post("/api/check")
def start_check():
    body = request.get_json(silent=True) or {}
    subset = body.get("read_data_subset", request.args.get("read_data_subset"))
    if subset is not None:
        subset = str(subset).strip()
        if subset and not SUBSET_RE.fullmatch(subset):
            return error("read_data_subset must look like '5%', '1/10' or '500M'.", 400)
    run_id = manager().start_job("check", trigger=trigger(), read_data_subset=subset)
    if run_id is None:
        return error("A job is already in progress.", 409)
    return jsonify({"status": "success", "message": "Repository check started.", "run_id": run_id}), 202


# --- status & history ---

@bp.get("/status")
@bp.get("/api/status")
def status():
    lines = request.args.get("log_lines", default=300, type=int)
    return jsonify(manager().get_status(log_lines=lines)
                   | {"auth_enabled": bool(current_app.config["AUTH_TOKEN"])})


@bp.get("/api/runs")
def runs():
    limit = max(1, min(request.args.get("limit", default=50, type=int), 500))
    offset = max(0, request.args.get("offset", default=0, type=int))
    return jsonify(manager().db.list_runs(limit, offset))


@bp.get("/api/runs/<int:run_id>")
def run(run_id: int):
    found = manager().db.get_run(run_id)
    return jsonify(found) if found else error("Run not found.", 404)


@bp.get("/api/health")
def health():
    """200 when every configured dataset has a snapshot newer than MAX_AGE_HOURS, else 503.
    Point an Uptime Kuma / healthchecks HTTP monitor at this as a dead-man's switch."""
    try:
        report = manager().health()
    except ConfigError as e:
        return jsonify({"ok": False, "error": f"Invalid config: {e}"}), 503
    except Exception as e:
        return jsonify({"ok": False, "error": f"Could not query repository: {e}"}), 503
    return jsonify(report), 200 if report["ok"] else 503


# --- snapshots ---

@bp.get("/snapshots")
def snapshots_v1():
    try:
        return jsonify(manager().snapshots(max_age=0))
    except CommandError as e:
        return error("Failed to list snapshots.", 500, e.output)
    except Exception as e:
        return error("An unexpected error occurred.", 500, str(e))


@bp.get("/api/snapshots")
def snapshots():
    try:
        return jsonify(manager().snapshots(max_age=0 if request.args.get("refresh") == "1" else None))
    except CommandError as e:
        return error("Failed to list snapshots.", 500, e.output)


@bp.get("/api/snapshots/<snapshot_id>/ls")
def snapshot_ls(snapshot_id: str):
    path, err = snapshot_and_path(snapshot_id)
    if err:
        return err
    try:
        return jsonify(manager().list_dir(snapshot_id, path))
    except CommandError as e:
        return error("Failed to list directory.", 500, e.output)


@bp.get("/api/snapshots/<snapshot_id>/download")
def snapshot_download(snapshot_id: str):
    """Stream a file (or a directory as .zip) straight out of a snapshot via `restic dump`."""
    path, err = snapshot_and_path(snapshot_id)
    if err:
        return err
    as_zip = request.args.get("dir") == "1"
    proc = subprocess.Popen(manager().restic.dump_command(snapshot_id, path, as_zip),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert proc.stdout is not None and proc.stderr is not None
    first = proc.stdout.read(65536)
    if not first and proc.wait() != 0:
        return error("restic dump failed.", 404, proc.stderr.read().decode(errors="replace"))

    def generate():
        try:
            yield first
            while chunk := proc.stdout.read(1 << 20):
                yield chunk
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait()

    name = (path.rstrip("/").rsplit("/", 1)[-1] or "root") + (".zip" if as_zip else "")
    return Response(generate(), mimetype="application/zip" if as_zip else "application/octet-stream",
                    headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}"})

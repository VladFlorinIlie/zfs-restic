"""Jobs, status, history and health. `/backup` and `/status` keep their v1 behaviour for cron jobs."""
from __future__ import annotations

import re

from flask import Blueprint, current_app, jsonify, request

from ... import __version__
from ...config import ConfigError
from .common import auth_enabled, error, json_body, manager, trigger

bp = Blueprint("jobs", __name__)
SUBSET_RE = re.compile(r"\d+(\.\d+)?%|\d+/\d+|\d+[KMGT]")


@bp.get("/")
def index():
    return current_app.send_static_file("index.html")


@bp.get("/healthz")
def healthz():
    return jsonify({"status": "ok"})


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


@bp.get("/status")
@bp.get("/api/status")
def status():
    m = manager()
    lines = request.args.get("log_lines", default=300, type=int)
    restore_ok, restore_msg = m.restore_available()
    return jsonify(m.get_status(log_lines=lines) | {
        "auth_enabled": auth_enabled(), "version": __version__,
        "restore": {"available": restore_ok, "message": restore_msg},
        "notifications": m.notifier.channels,
        "recovery_kit_exported_at": m.db.kv_get("recovery_kit_exported_at"),
        "history_warning": m.history_warning,
    })


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
    """200 when every configured dataset has a recent snapshot and the remote isn't nearly full, else 503.
    Point an Uptime Kuma / healthchecks HTTP monitor at this as a dead-man's switch."""
    try:
        report = manager().health()
    except ConfigError as e:
        return jsonify({"ok": False, "error": f"Invalid config: {e}"}), 503
    except Exception as e:
        return jsonify({"ok": False, "error": f"Could not query repository: {e}"}), 503
    return jsonify(report), 200 if report["ok"] else 503


@bp.get("/api/storage")
def storage():
    try:
        info = manager().storage_info(refresh=request.args.get("refresh") == "1")
    except Exception as e:
        return error("Could not query remote storage.", 500, str(e))
    return jsonify(info or {"remote": None})


@bp.post("/api/notifications/test")
def test_notification():
    if json_body() is None:
        return error("Expected a JSON body.", 400)
    m = manager()
    if not m.notifier.channels:
        return error("No notification channels configured (GOTIFY_URL/GOTIFY_TOKEN or APPRISE_URLS).", 400)
    log = []
    results = m.notifier.notify("🔔 zfs-restic test notification",
                                "If you can read this, notifications from zfs-restic work.", "success",
                                log.append, force=True)
    return jsonify({"results": results, "log": log}), 200 if all(results.values()) else 502

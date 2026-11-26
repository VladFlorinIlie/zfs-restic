import base64
import hmac
import os
import re
import subprocess
from functools import lru_cache
from urllib.parse import quote

from flask import Flask, Response, jsonify, request, send_from_directory

from backup import BackupManager, CommandError

AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "")
SNAPSHOT_ID_RE = re.compile(r"^[0-9a-f]{8,64}$")
OPEN_PATHS = {"/healthz"}

app = Flask(__name__, static_folder="static", static_url_path="/static")
backup_manager = BackupManager()


# --- Auth: optional shared token, accepted as Bearer or as the Basic-auth password ---

def _presented_token() -> str:
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header[7:].strip()
    if header.startswith("Basic "):
        try:
            return base64.b64decode(header[6:]).decode().split(":", 1)[1]
        except Exception:
            return ""
    return ""


@app.before_request
def require_auth():
    if not AUTH_TOKEN or request.path in OPEN_PATHS:
        return None
    if hmac.compare_digest(_presented_token().encode(), AUTH_TOKEN.encode()):
        return None
    return Response("Unauthorized\n", 401, {"WWW-Authenticate": 'Basic realm="zfs-restic"'})


def error(message: str, code: int, details: str = ""):
    return jsonify({"status": "error", "message": message, "details": details}), code


def valid_snapshot_and_path():
    snap = request.view_args["snapshot_id"]
    path = request.args.get("path", "/")
    if not SNAPSHOT_ID_RE.match(snap):
        return None, None, error("Invalid snapshot id.", 400)
    if not path.startswith("/") or "\x00" in path:
        return None, None, error("Path must be absolute.", 400)
    return snap, path, None


# --- UI ---

@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok"})


# --- Original endpoints (kept compatible for existing cron jobs) ---

@app.post("/backup")
@app.post("/api/backup")
def backup_endpoint():
    trigger = "ui" if request.args.get("trigger") == "ui" else "api"
    run_id = backup_manager.start_job("backup", trigger=trigger)
    if run_id is None:
        return jsonify({"status": "error", "message": "A job is already in progress."}), 409
    return jsonify({"status": "success", "message": "Backup process started in the background.",
                    "run_id": run_id}), 202


@app.get("/status")
@app.get("/api/status")
def status_endpoint():
    lines = request.args.get("log_lines", default=300, type=int)
    return jsonify(backup_manager.get_status(log_lines=lines) | {"auth_enabled": bool(AUTH_TOKEN)})


@app.get("/snapshots")
def snapshots_endpoint():
    try:
        return jsonify(backup_manager.get_current_snapshots(max_age=0))
    except CommandError as e:
        return error("Failed to list snapshots.", 500, e.output)
    except Exception as e:
        return error("An unexpected error occurred.", 500, str(e))


# --- New API ---

@app.post("/api/check")
def check_endpoint():
    body = request.get_json(silent=True) or {}
    subset = body.get("read_data_subset", request.args.get("read_data_subset"))
    if subset is not None:
        subset = str(subset).strip()
        if subset and not re.fullmatch(r"\d+(\.\d+)?%|\d+/\d+|\d+[KMGT]", subset):
            return error("read_data_subset must look like '5%', '1/10' or '500M'.", 400)
    trigger = "ui" if request.args.get("trigger") == "ui" else "api"
    run_id = backup_manager.start_job("check", trigger=trigger, read_data_subset=subset)
    if run_id is None:
        return error("A job is already in progress.", 409)
    return jsonify({"status": "success", "message": "Repository check started.", "run_id": run_id}), 202


@app.get("/api/runs")
def runs_endpoint():
    limit = min(request.args.get("limit", default=50, type=int), 500)
    offset = request.args.get("offset", default=0, type=int)
    return jsonify(backup_manager.db.list_runs(limit, offset))


@app.get("/api/runs/<int:run_id>")
def run_endpoint(run_id: int):
    run = backup_manager.db.get_run(run_id)
    return jsonify(run) if run else error("Run not found.", 404)


@app.get("/api/health")
def health_endpoint():
    """200 when every configured dataset has a snapshot newer than MAX_AGE_HOURS, else 503.
    Point an Uptime Kuma / healthchecks HTTP monitor at this as a dead-man's switch."""
    try:
        report = backup_manager.health()
    except Exception as e:
        return jsonify({"ok": False, "error": f"Could not query repository: {e}"}), 503
    return jsonify(report), 200 if report["ok"] else 503


@app.get("/api/snapshots")
def api_snapshots():
    try:
        fresh = request.args.get("refresh") == "1"
        return jsonify(backup_manager.get_current_snapshots(max_age=0 if fresh else 600))
    except CommandError as e:
        return error("Failed to list snapshots.", 500, e.output)


@lru_cache(maxsize=128)
def _cached_ls(snapshot_id: str, path: str):
    # Snapshots are immutable, so listings can be cached for the life of the process.
    return backup_manager.list_dir(snapshot_id, path)


@app.get("/api/snapshots/<snapshot_id>/ls")
def snapshot_ls(snapshot_id: str):
    snap, path, err = valid_snapshot_and_path()
    if err:
        return err
    try:
        return jsonify(_cached_ls(snap, path))
    except CommandError as e:
        return error("Failed to list directory.", 500, e.output)


@app.get("/api/snapshots/<snapshot_id>/download")
def snapshot_download(snapshot_id: str):
    """Stream a file (or a directory as .zip) straight out of a snapshot via `restic dump`."""
    snap, path, err = valid_snapshot_and_path()
    if err:
        return err
    is_dir = request.args.get("dir") == "1"
    cmd = ["restic", "dump", "--no-lock"] + (["--archive", "zip"] if is_dir else []) + [snap, path]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

    name = (path.rstrip("/").rsplit("/", 1)[-1] or "root") + (".zip" if is_dir else "")
    return Response(generate(), mimetype="application/zip" if is_dir else "application/octet-stream",
                    headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}"})


if __name__ == "__main__":
    if not AUTH_TOKEN:
        print("WARNING: AUTH_TOKEN is not set -- the UI/API (including file downloads) is open to anyone "
              "who can reach this port.", flush=True)
    from waitress import serve
    port = int(os.environ.get("PORT", "8000"))
    print(f"Starting zfs-restic on :{port}", flush=True)
    serve(app, host="0.0.0.0", port=port, threads=8, channel_timeout=3600)

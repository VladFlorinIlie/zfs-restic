"""Snapshot listing, browsing, download, diff, find and restore."""
from __future__ import annotations

import subprocess
from urllib.parse import quote

from flask import Blueprint, Response, jsonify, request

from ...manager import RestoreUnavailable
from ...shell import CommandError
from .common import error, json_body, manager, snapshot_and_path, trigger

bp = Blueprint("snapshots", __name__)


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


@bp.get("/api/snapshots/<old_id>/diff/<new_id>")
def snapshot_diff(old_id: str, new_id: str):
    for sid in (old_id, new_id):
        _, err = snapshot_and_path(sid, "/")
        if err:
            return err
    try:
        return jsonify(manager().diff(old_id, new_id))
    except CommandError as e:
        return error("Failed to compare snapshots.", 500, e.output)


@bp.get("/api/find")
def find():
    pattern = (request.args.get("pattern") or "").strip()
    if not pattern or len(pattern) > 500 or "\x00" in pattern:
        return error("Give a file name or glob pattern, e.g. 'report.pdf' or '*.jpg'.", 400)
    dataset = request.args.get("dataset") or None
    try:
        return jsonify(manager().find(pattern, dataset, ignore_case=request.args.get("ignore_case") == "1"))
    except CommandError as e:
        return error("Search failed.", 500, e.output)


@bp.post("/api/restore")
def restore():
    body = json_body()
    if body is None:
        return error("Expected a JSON body.", 400)
    snapshot_id = str(body.get("snapshot_id", ""))
    path, err = snapshot_and_path(snapshot_id, body.get("path"))
    if err:
        return err
    try:
        started = manager().start_restore(snapshot_id, path, bool(body.get("is_dir")), trigger=trigger())
    except RestoreUnavailable as e:
        return error(str(e), 409)
    except ValueError as e:
        return error(str(e), 404)
    if started is None:
        return error("A job is already in progress.", 409)
    return jsonify({"status": "success", **started}), 202


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

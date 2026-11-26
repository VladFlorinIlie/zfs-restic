"""rclone status, connection tests and OAuth (re)authorisation of remotes."""
from __future__ import annotations

from flask import Blueprint, jsonify

from ... import backend, rclone
from ...shell import CommandError
from ...utils import now_iso
from .common import error, json_body, manager, requires_auth_token

bp = Blueprint("rclone", __name__)


@bp.get("/api/rclone")
def overview():
    if not rclone.available():
        return jsonify({"available": False})
    path = rclone.config_file()
    try:
        remotes = rclone.describe_remotes()
        err = None
    except CommandError as e:
        remotes, err = [], e.output
    return jsonify({
        "available": True, "version": rclone.version(), "config_file": str(path) if path else None,
        "config_writable": rclone.config_writable(path), "repository_remote": rclone.remote_for_repository(),
        "remotes": remotes, "error": err, "versions": manager().db.list_file_versions("rclone"),
        "backend": backend.detect(), "oauth_types": sorted(rclone.OAUTH_TYPES),
        "creatable_types": sorted(rclone.CREATABLE_TYPES),
    })


@bp.post("/api/rclone/test")
def test():
    body = json_body()
    if body is None or not body.get("remote"):
        return error("Expected {\"remote\": \"name\"}.", 400)
    return jsonify(rclone.test(str(body["remote"])))


@bp.post("/api/rclone/authorize-command")
def authorize_command():
    body = json_body() or {}
    return jsonify({"command": rclone.authorize_command(str(body.get("type") or "drive"),
                                                        str(body.get("client_id") or "").strip(),
                                                        str(body.get("client_secret") or "").strip())})


@bp.post("/api/rclone/authorize")
@requires_auth_token
def authorize():
    """Re-authorise an existing OAuth remote (any type) or create a new one from a pasted `rclone authorize`
    token, then test it. The current rclone config is kept as a previous version first."""
    body = json_body()
    if body is None:
        return error("Expected a JSON body.", 400)
    path = rclone.config_file()
    if not rclone.config_writable(path):
        return error(f"rclone config file {path} is not writable.", 409)
    name = str(body.get("name") or "").strip()
    try:
        if path and path.exists():
            manager().db.add_file_version("rclone", path.read_text(), now_iso(), f"before authorising remote {name}")
    except OSError as e:
        return error("Could not back up the current rclone config.", 500, str(e))
    extra = {k: str(body.get(k) or "").strip() for k in ("scope", "root_folder_id") if body.get(k)}
    try:
        rtype = rclone.set_oauth_remote(
            name=name, token=str(body.get("token") or "").strip(),
            client_id=str(body.get("client_id") or "").strip(), client_secret=str(body.get("client_secret") or "").strip(),
            new_type=str(body.get("type") or "").strip() or None, extra=extra)
    except ValueError as e:
        return error(str(e), 400)
    except (CommandError, RuntimeError) as e:
        return error("rclone could not save the remote.", 500, getattr(e, "output", str(e)))
    manager().invalidate_snapshots()
    return jsonify({"status": "saved", "type": rtype, "test": rclone.test(name)})


@bp.post("/api/rclone/versions/<int:version_id>/restore")
@requires_auth_token
def restore_version(version_id: int):
    if json_body() is None:
        return error("Expected a JSON body.", 400)
    v = manager().db.get_file_version("rclone", version_id)
    path = rclone.config_file()
    if not v or not path:
        return error("Version not found.", 404)
    if not rclone.config_writable(path):
        return error(f"rclone config file {path} is not writable.", 409)
    try:
        if path.exists():
            manager().db.add_file_version("rclone", path.read_text(), now_iso(), f"before reverting to #{version_id}")
        path.write_text(v["content"])
    except OSError as e:
        return error("Could not write rclone config.", 500, str(e))
    return jsonify({"status": "restored"})

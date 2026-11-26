from __future__ import annotations

import re
from functools import wraps
from typing import Any, Dict, Optional, Tuple

from flask import current_app, jsonify, request

from ...manager import BackupManager

SNAPSHOT_ID_RE = re.compile(r"^[0-9a-f]{8,64}$")


def manager() -> BackupManager:
    return current_app.extensions["zfs_restic"]


def error(message: str, code: int, details: str = ""):
    return jsonify({"status": "error", "message": message, "details": details}), code


def trigger() -> str:
    return "ui" if request.args.get("trigger") == "ui" else "api"


def json_body() -> Optional[Dict[str, Any]]:
    """Mutating endpoints only accept application/json, which browsers can't send cross-origin
    without a CORS preflight -- a cheap CSRF guard for the Basic-auth UI."""
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else None


def auth_enabled() -> bool:
    return bool(current_app.config["AUTH_TOKEN"])


def requires_auth_token(view):
    """For endpoints that expose secrets or can break backups: refuse unless AUTH_TOKEN is set."""
    @wraps(view)
    def wrapper(*args, **kwargs):
        if not auth_enabled():
            return error("Set AUTH_TOKEN to use this feature: it exposes secrets or changes how backups run.", 403)
        return view(*args, **kwargs)
    return wrapper


def snapshot_and_path(snapshot_id: str, path: Optional[str] = None) -> Tuple[Optional[str], Any]:
    path = request.args.get("path", "/") if path is None else path
    if not SNAPSHOT_ID_RE.match(snapshot_id or ""):
        return None, error("Invalid snapshot id.", 400)
    if not isinstance(path, str) or not path.startswith("/") or "\x00" in path:
        return None, error("Path must be absolute.", 400)
    return path, None

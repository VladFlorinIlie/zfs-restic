"""config.yml editor and retention preview."""
from __future__ import annotations

import yaml
from flask import Blueprint, jsonify

from ...config import ConfigError, load_config, parse_config
from ...config_store import ConflictError, ReadOnlyError
from ...shell import CommandError
from .common import error, json_body, manager, requires_auth_token

bp = Blueprint("config", __name__)


@bp.get("/api/config")
def get_config():
    store = manager().config_store
    data = store.read()
    data["validation"] = store.validate(data["content"]) if data["exists"] else None
    return jsonify(data)


@bp.post("/api/config/validate")
def validate_config():
    body = json_body()
    if body is None or not isinstance(body.get("content"), str):
        return error("Expected {\"content\": \"...\"}.", 400)
    return jsonify(manager().config_store.validate(body["content"]))


@bp.put("/api/config")
@requires_auth_token
def save_config():
    body = json_body()
    if body is None or not isinstance(body.get("content"), str) or not isinstance(body.get("sha256"), str):
        return error("Expected {\"content\": \"...\", \"sha256\": \"<of the version you edited>\"}.", 400)
    m = manager()
    try:
        saved = m.config_store.save(body["content"], body["sha256"], note=str(body.get("note") or "")[:200])
    except ConfigError as e:
        return error("Config is invalid; nothing was saved.", 400, str(e))
    except ConflictError as e:
        return error(str(e), 409)
    except ReadOnlyError as e:
        return error(str(e), 409)
    m.invalidate_snapshots()
    saved["validation"] = m.config_store.validate(saved["content"])
    return jsonify(saved)


@bp.get("/api/config/versions")
def config_versions():
    return jsonify(manager().config_store.versions())


@bp.get("/api/config/versions/<int:version_id>")
def config_version(version_id: int):
    v = manager().config_store.version(version_id)
    return jsonify(v) if v else error("Version not found.", 404)


@bp.post("/api/retention/preview")
def retention_preview():
    """What `forget` would remove with the given (draft) config, or the saved one if none given."""
    body = json_body() or {}
    try:
        if isinstance(body.get("content"), str):
            cfg = parse_config(yaml.safe_load(body["content"]))
        else:
            cfg = load_config(manager().settings.config_path)
    except (ConfigError, yaml.YAMLError) as e:
        return error("Config is invalid.", 400, str(e))
    try:
        return jsonify(manager().retention_preview(cfg.retention_args()) | {"args": cfg.retention_args()})
    except CommandError as e:
        return error("restic forget --dry-run failed.", 500, e.output)

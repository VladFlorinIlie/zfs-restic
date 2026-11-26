"""Disaster-recovery kit export."""
from __future__ import annotations

from flask import Blueprint, Response, request

from ... import recovery
from ...utils import now_iso
from .common import auth_enabled, error, manager

bp = Blueprint("recovery", __name__)


@bp.get("/api/recovery-kit")
def recovery_kit():
    include_secrets = request.args.get("secrets") == "1"
    if include_secrets and not auth_enabled():
        return error("Set AUTH_TOKEN before exporting a kit with secrets: without it, anyone on your "
                     "network could download your repository password.", 403)
    fmt = request.args.get("format", "html")
    m = manager()
    kit = recovery.build_kit(m, include_secrets)
    if include_secrets:
        m.db.kv_set("recovery_kit_exported_at", now_iso())
    headers = {"Cache-Control": "no-store", "X-Robots-Tag": "noindex"}
    if fmt == "md":
        headers["Content-Disposition"] = "attachment; filename=zfs-restic-recovery-kit.md"
        return Response(recovery.render_markdown(kit), mimetype="text/markdown", headers=headers)
    return Response(recovery.render_html(kit), mimetype="text/html", headers=headers)

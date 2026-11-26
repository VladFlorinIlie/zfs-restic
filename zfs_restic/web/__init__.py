from __future__ import annotations

from flask import Flask, request

from ..manager import BackupManager
from . import auth
from .routes import BLUEPRINTS


def create_app(manager: BackupManager) -> Flask:
    app = Flask(__name__, static_folder="static", static_url_path="/static")
    app.config["AUTH_TOKEN"] = manager.settings.auth_token
    app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024
    app.extensions["zfs_restic"] = manager

    @app.before_request
    def require_auth():
        return auth.check(request, app.config["AUTH_TOKEN"])

    @app.after_request
    def security_headers(resp):
        resp.headers.setdefault("X-Content-Type-Options", "nosniff")
        resp.headers.setdefault("X-Frame-Options", "DENY")
        resp.headers.setdefault("Referrer-Policy", "no-referrer")
        return resp

    for bp in BLUEPRINTS:
        app.register_blueprint(bp)
    return app

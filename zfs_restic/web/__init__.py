from __future__ import annotations

from flask import Flask, request

from ..manager import BackupManager
from . import auth
from .api import bp


def create_app(manager: BackupManager) -> Flask:
    app = Flask(__name__, static_folder="static", static_url_path="/static")
    app.config["AUTH_TOKEN"] = manager.settings.auth_token
    app.extensions["zfs_restic"] = manager

    @app.before_request
    def require_auth():
        return auth.check(request, app.config["AUTH_TOKEN"])

    app.register_blueprint(bp)
    return app

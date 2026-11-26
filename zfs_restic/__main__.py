"""Entrypoint: `python -m zfs_restic`."""
from __future__ import annotations

import sys

from waitress import serve

from . import __version__
from .config import ConfigError, load_config
from .manager import BackupManager
from .settings import Settings
from .web import create_app


def main() -> int:
    settings = Settings.from_env()
    print(f"zfs-restic {__version__}", flush=True)

    # Validate the config up front so mistakes show in the container log, not at 3 AM.
    try:
        cfg = load_config(settings.config_path)
        print(f"Config OK: {len(cfg.datasets)} dataset(s): {', '.join(cfg.datasets) or '-'}", flush=True)
    except ConfigError as e:
        print(f"WARNING: {e} -- backups will fail until this is fixed.", flush=True)

    if not settings.auth_token:
        print("WARNING: AUTH_TOKEN is not set -- the UI/API (including file downloads) is open to anyone "
              "who can reach this port.", flush=True)

    app = create_app(BackupManager(settings))
    print(f"Listening on :{settings.port}", flush=True)
    serve(app, host="0.0.0.0", port=settings.port, threads=8, channel_timeout=3600)
    return 0


if __name__ == "__main__":
    sys.exit(main())

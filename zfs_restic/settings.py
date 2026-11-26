"""Runtime settings, read from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional


def _bool(value: Optional[str], default: bool) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() not in ("0", "false", "no", "off")


@dataclass(frozen=True)
class Settings:
    config_path: Path = Path("/config/config.yml")
    data_dir: Path = Path("/data")
    mount_root: Path = Path("/mnt")
    port: int = 8000
    auth_token: str = ""
    history_limit: int = 500
    max_age_hours: float = 36.0
    snapshot_cache_ttl: int = 600
    progress_fps: str = "0.5"
    gotify_url: Optional[str] = None
    gotify_token: Optional[str] = None
    gotify_notify_success: bool = True
    healthcheck_url: Optional[str] = None
    healthcheck_fail_url: Optional[str] = None
    healthcheck_start_url: Optional[str] = None

    @property
    def db_path(self) -> Path:
        return self.data_dir / "zfs-restic.db"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> "Settings":
        d = cls()
        return cls(
            config_path=Path(env.get("CONFIG_PATH", d.config_path)),
            data_dir=Path(env.get("DATA_DIR", d.data_dir)),
            mount_root=Path(env.get("MOUNT_ROOT", d.mount_root)),
            port=int(env.get("PORT", d.port)),
            auth_token=env.get("AUTH_TOKEN", ""),
            history_limit=int(env.get("HISTORY_LIMIT", d.history_limit)),
            max_age_hours=float(env.get("MAX_AGE_HOURS", d.max_age_hours)),
            snapshot_cache_ttl=int(env.get("SNAPSHOT_CACHE_TTL", d.snapshot_cache_ttl)),
            progress_fps=env.get("RESTIC_PROGRESS_FPS", d.progress_fps),
            gotify_url=env.get("GOTIFY_URL") or None,
            gotify_token=env.get("GOTIFY_TOKEN") or None,
            gotify_notify_success=_bool(env.get("GOTIFY_NOTIFY_SUCCESS"), True),
            healthcheck_url=env.get("HEALTHCHECK_URL") or None,
            healthcheck_fail_url=env.get("HEALTHCHECK_FAIL_URL") or None,
            healthcheck_start_url=env.get("HEALTHCHECK_START_URL") or None,
        )

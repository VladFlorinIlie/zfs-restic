"""Reading, validating and saving config.yml from the UI, with version history."""
from __future__ import annotations

import errno
import hashlib
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from . import zfs
from .config import BackupConfig, ConfigError, parse_config
from .storage import Database
from .utils import now_iso

MAX_CONFIG_BYTES = 256 * 1024


class ConflictError(Exception):
    """The file changed on disk since the editor loaded it."""


class ReadOnlyError(Exception):
    pass


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class ConfigStore:
    def __init__(self, path: Path, db: Database):
        self.path, self.db = path, db

    def read(self) -> Dict[str, Any]:
        try:
            content = self.path.read_text()
            exists = True
        except FileNotFoundError:
            content, exists = "", False
        return {"path": str(self.path), "exists": exists, "content": content, "sha256": sha256(content),
                "writable": self.writable()}

    def writable(self) -> bool:
        if self.path.exists():
            return os.access(self.path, os.W_OK)
        return self.path.parent.exists() and os.access(self.path.parent, os.W_OK)

    def validate(self, content: str, check_datasets: bool = True) -> Dict[str, Any]:
        errors: List[str] = []
        warnings: List[str] = []
        cfg: Optional[BackupConfig] = None
        if len(content.encode()) > MAX_CONFIG_BYTES:
            errors.append("config is too large")
        else:
            try:
                cfg = parse_config(yaml.safe_load(content))
            except yaml.YAMLError as e:
                errors.append(f"YAML syntax error: {e}")
            except ConfigError as e:
                errors.append(str(e))
        if cfg is not None:
            if not cfg.datasets:
                warnings.append("No datasets configured: backups will fail.")
            if not cfg.retention:
                warnings.append("No retention policy: snapshots will never be pruned.")
            if check_datasets and cfg.datasets:
                try:
                    existing = set(zfs.list_filesystems())
                    for ds in cfg.datasets:
                        if ds not in existing:
                            warnings.append(f"Dataset '{ds}' does not exist on this system.")
                except Exception as e:
                    warnings.append(f"Could not check datasets against ZFS: {e}")
        summary = None
        if cfg is not None:
            summary = {"datasets": cfg.datasets, "retention_args": cfg.retention_args(),
                       "check_read_data_subset": cfg.check_read_data_subset}
        return {"ok": not errors, "errors": errors, "warnings": warnings, "summary": summary}

    def save(self, content: str, expected_sha256: str, note: str = "") -> Dict[str, Any]:
        result = self.validate(content, check_datasets=False)
        if not result["ok"]:
            raise ConfigError("; ".join(result["errors"]))
        if not self.writable():
            raise ReadOnlyError(f"{self.path} is not writable (mounted read-only?)")
        current = self.read()
        if current["sha256"] != expected_sha256:
            raise ConflictError("config.yml changed on disk since you loaded it; reload and re-apply your edits")
        if current["exists"]:
            self.db.add_file_version("config", current["content"], now_iso(), note or "before UI edit")
        if not content.endswith("\n"):
            content += "\n"
        _write_atomic(self.path, content)
        return self.read()

    def versions(self) -> List[Dict[str, Any]]:
        return self.db.list_file_versions("config")

    def version(self, version_id: int) -> Optional[Dict[str, Any]]:
        return self.db.get_file_version("config", version_id)


def _write_atomic(path: Path, content: str):
    """Atomic replace when possible; falls back to an in-place write when config.yml itself is a
    bind mount (rename onto a mount point fails with EBUSY/EXDEV)."""
    try:
        fd, tmp = tempfile.mkstemp(prefix=".config.", dir=path.parent)
    except OSError:
        fd, tmp = -1, ""
    if fd != -1:
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            if path.exists():
                os.chmod(tmp, path.stat().st_mode & 0o7777)
            os.replace(tmp, path)
            return
        except OSError as e:
            if os.path.exists(tmp):
                os.unlink(tmp)
            if e.errno not in (errno.EBUSY, errno.EXDEV, errno.EPERM, errno.EACCES):
                raise
    with open(path, "r+" if path.exists() else "w") as f:
        f.seek(0)
        f.write(content)
        f.truncate()
        f.flush()
        os.fsync(f.fileno())

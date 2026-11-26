"""rclone remote inspection and (re)configuration -- mainly for Google Drive."""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .shell import CommandError, capture

SECRET_KEYS = {"token", "client_secret", "pass", "password", "password2", "key", "secret_access_key",
               "service_account_credentials", "sa_credentials", "bearer_token", "session_token"}
NAME_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_. -]*$")
OPTION_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_REPO_RE = re.compile(r"^rclone:([^:]+):")


def repository() -> Optional[str]:
    repo = os.environ.get("RESTIC_REPOSITORY")
    if not repo and os.environ.get("RESTIC_REPOSITORY_FILE"):
        try:
            repo = Path(os.environ["RESTIC_REPOSITORY_FILE"]).read_text().strip()
        except OSError:
            return None
    return repo


def remote_for_repository(repo: Optional[str] = None) -> Optional[str]:
    """`rclone:gdrive:backups/restic` -> `gdrive`. None for non-rclone repositories."""
    m = _REPO_RE.match(repo if repo is not None else (repository() or ""))
    return m.group(1) if m else None


def available() -> bool:
    try:
        capture(["rclone", "version"], timeout=30)
        return True
    except Exception:
        return False


def version() -> str:
    try:
        return capture(["rclone", "version"], timeout=30).splitlines()[0].strip()
    except Exception as e:
        return f"unavailable ({e})"


def config_file() -> Optional[Path]:
    try:
        out = capture(["rclone", "config", "file"], timeout=30)
    except Exception:
        return None
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    return Path(lines[-1]) if lines else None


def config_writable(path: Optional[Path]) -> bool:
    if path is None:
        return False
    if path.exists():
        return os.access(path, os.W_OK)
    return os.access(path.parent if path.parent.exists() else path.parent.parent, os.W_OK)


def dump() -> Dict[str, Dict[str, Any]]:
    """Full config including secrets. Handle with care."""
    return json.loads(capture(["rclone", "config", "dump"], timeout=30) or "{}")


def _token_info(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {"present": False}
    try:
        tok = json.loads(raw)
    except ValueError:
        return {"present": True, "valid_json": False}
    return {"present": True, "valid_json": True, "has_refresh_token": bool(tok.get("refresh_token")),
            "expiry": tok.get("expiry")}


def describe_remotes() -> List[Dict[str, Any]]:
    """Remotes with secrets removed, plus Google-Drive-specific health hints."""
    out = []
    for name, cfg in dump().items():
        rtype = cfg.get("type")
        options = {k: ("•••" if k in SECRET_KEYS and v else v) for k, v in cfg.items() if k != "type"}
        info: Dict[str, Any] = {"name": name, "type": rtype, "options": options, "warnings": []}
        if rtype == "drive":
            own_client = bool(cfg.get("client_id"))
            info["drive"] = {
                "own_client_id": own_client,
                "service_account": bool(cfg.get("service_account_file") or cfg.get("service_account_credentials")),
                "scope": cfg.get("scope") or "drive",
                "root_folder_id": cfg.get("root_folder_id") or None,
                "team_drive": cfg.get("team_drive") or None,
                "token": _token_info(cfg.get("token")),
            }
            if not own_client and not info["drive"]["service_account"]:
                info["warnings"].append(
                    "Uses rclone's shared Google Drive client_id, which rclone is retiring during 2026. "
                    "Set up your own client_id/client_secret to avoid backups suddenly failing.")
            if info["drive"]["token"].get("present") and not info["drive"]["token"].get("has_refresh_token"):
                info["warnings"].append("Token has no refresh_token; it will stop working when it expires.")
            if not info["drive"]["token"].get("present") and not info["drive"]["service_account"]:
                info["warnings"].append("No token: this remote is not authorised yet.")
        out.append(info)
    return out


def show(name: str, redacted: bool) -> str:
    """The remote's config section as INI text (secrets redacted unless redacted=False)."""
    if redacted:
        out = capture(["rclone", "config", "redacted", name], timeout=30)
        return "\n".join(l for l in out.splitlines() if not l.startswith("###")).strip()
    return capture(["rclone", "config", "show", name], timeout=30).strip()


def about(name: str) -> Dict[str, Any]:
    """Storage quota for a remote: total/used/free/trashed/other (bytes; some may be missing)."""
    return json.loads(capture(["rclone", "about", f"{name}:", "--json"], timeout=120))


def test(name: str) -> Dict[str, Any]:
    started = datetime.now(timezone.utc)
    try:
        capture(["rclone", "lsf", f"{name}:", "--max-depth", "1", "--dirs-only"], timeout=120)
        return {"ok": True, "seconds": (datetime.now(timezone.utc) - started).total_seconds()}
    except CommandError as e:
        return {"ok": False, "error": e.output.strip()[-2000:]}


def set_drive_remote(name: str, token: str, client_id: str = "", client_secret: str = "",
                     scope: str = "drive", root_folder_id: str = "") -> None:
    """Create or update a Google Drive remote from a token produced by `rclone authorize "drive" ...`."""
    if not NAME_RE.match(name):
        raise ValueError("invalid remote name")
    try:
        tok = json.loads(token)
    except ValueError:
        raise ValueError("token must be the JSON printed by `rclone authorize` ({\"access_token\": ...})") from None
    if not isinstance(tok, dict) or "access_token" not in tok:
        raise ValueError("token JSON has no access_token")
    if not tok.get("refresh_token"):
        raise ValueError("token has no refresh_token; it would stop working within an hour")
    if scope not in ("drive", "drive.file"):
        raise ValueError("scope must be 'drive' or 'drive.file'")
    if bool(client_id) != bool(client_secret):
        raise ValueError("client_id and client_secret must be given together")

    params = {"scope": scope, "token": json.dumps(tok, separators=(",", ":")),
              "client_id": client_id, "client_secret": client_secret, "root_folder_id": root_folder_id}
    exists = name in dump()
    args: List[str] = []
    for k, v in params.items():
        if v or exists:  # on update, empty values clear the option (e.g. back to no root folder)
            args += [k, v]
    verb = "update" if exists else "create"
    cmd = ["rclone", "config", verb, name] + ([] if exists else ["drive"]) + args + ["--non-interactive"]
    capture(cmd, timeout=60)
    if name not in dump():
        raise RuntimeError("rclone did not save the remote")


def authorize_command(client_id: str = "", client_secret: str = "") -> str:
    if client_id and client_secret:
        return f'rclone authorize "drive" "{client_id}" "{client_secret}"'
    return 'rclone authorize "drive"'


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

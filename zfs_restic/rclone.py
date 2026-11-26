"""rclone remote inspection and OAuth (re)authorisation, for any remote type."""
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
    """Remotes with secrets removed, plus health hints (OAuth token state; Drive client_id retirement)."""
    out = []
    for name, cfg in dump().items():
        rtype = cfg.get("type")
        options = {k: ("•••" if k in SECRET_KEYS and v else v) for k, v in cfg.items() if k != "type"}
        info: Dict[str, Any] = {"name": name, "type": rtype, "options": options, "warnings": [],
                                "oauth": rtype in OAUTH_TYPES, "can_reauthorise": rtype in OAUTH_TYPES,
                                "own_client_id": bool(cfg.get("client_id")), "token": _token_info(cfg.get("token"))}
        if rtype in OAUTH_TYPES and rtype != "drive" and not cfg.get("token"):
            info["warnings"].append("No token: this remote is not authorised yet.")
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


# rclone backends that authorise with an OAuth token from `rclone authorize "<type>"`.
OAUTH_TYPES = {"drive", "onedrive", "dropbox", "box", "pcloud", "google photos", "yandex", "zoho", "hidrive",
               "putio", "sharefile", "premiumizeme", "mailru"}
# Types this app can *create* non-interactively; anything else: create with `rclone config`, re-authorise here.
CREATABLE_TYPES = {"drive"}
# Extra options the form may set, per type.
EXTRA_OPTIONS = {"drive": {"scope": {"drive", "drive.file"}, "root_folder_id": None}}


def _check_token(token: str) -> str:
    try:
        tok = json.loads(token)
    except ValueError:
        raise ValueError("token must be the JSON printed by `rclone authorize` ({\"access_token\": ...})") from None
    if not isinstance(tok, dict) or "access_token" not in tok:
        raise ValueError("token JSON has no access_token")
    # Non-expiring tokens (e.g. pCloud) carry the zero time as expiry; everything else needs a refresh_token.
    never_expires = str(tok.get("expiry") or "").startswith("0001-")
    if not tok.get("refresh_token") and not never_expires:
        raise ValueError("token has no refresh_token, so it would stop working within hours")
    return json.dumps(tok, separators=(",", ":"))


def set_oauth_remote(name: str, token: str, client_id: str = "", client_secret: str = "",
                     new_type: Optional[str] = None, extra: Optional[Dict[str, str]] = None) -> str:
    """Re-authorise an existing OAuth remote (any type), or create a new one of a creatable type, from a token
    produced by `rclone authorize`. Empty fields keep the remote's current values. Returns the remote type."""
    if not NAME_RE.match(name or ""):
        raise ValueError("invalid remote name")
    if client_id and not client_secret and not _has_option(name, "client_secret"):
        raise ValueError("client_id needs a client_secret")
    if client_secret and not client_id and not _has_option(name, "client_id"):
        raise ValueError("client_secret needs a client_id")
    current = dump().get(name)
    rtype = current.get("type") if current else new_type
    if current is None and rtype not in CREATABLE_TYPES:
        raise ValueError(f"creating '{rtype}' remotes here isn't supported: create it with `rclone config`, "
                         "then re-authorise it here" if rtype else "unknown remote; choose a type to create it")
    if rtype not in OAUTH_TYPES:
        raise ValueError(f"remote '{name}' is of type '{rtype}', which doesn't use OAuth tokens")

    params = {"token": _check_token(token), "client_id": client_id, "client_secret": client_secret}
    allowed = EXTRA_OPTIONS.get(rtype, {})
    for key, value in (extra or {}).items():
        if not value:
            continue
        if key not in allowed:
            raise ValueError(f"option '{key}' can't be set for {rtype} remotes here")
        if allowed[key] is not None and value not in allowed[key]:
            raise ValueError(f"{key} must be one of: {', '.join(sorted(allowed[key]))}")
        params[key] = value
    if current is None and rtype == "drive":
        params.setdefault("scope", "drive")

    args: List[str] = []
    for k, v in params.items():
        # Empty fields never clear anything: re-authorising with blank client fields must keep the remote's
        # own client_id/secret and other options, or the remote silently breaks.
        if v:
            args += [k, v]
    cmd = ["rclone", "config", "update", name] if current else ["rclone", "config", "create", name, rtype]
    capture(cmd + args + ["--non-interactive"], timeout=60)
    if name not in dump():
        raise RuntimeError("rclone did not save the remote")
    return rtype


def _has_option(name: str, key: str) -> bool:
    return bool((dump().get(name) or {}).get(key))


def authorize_command(rtype: str = "drive", client_id: str = "", client_secret: str = "") -> str:
    if client_id:
        return f'rclone authorize "{rtype}" "{client_id}" "{client_secret or "<client secret>"}"'
    return f'rclone authorize "{rtype}"'


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

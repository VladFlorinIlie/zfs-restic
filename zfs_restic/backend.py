"""Which storage backend the restic repository uses, detected from RESTIC_REPOSITORY."""
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

from . import rclone

# scheme -> (display name, credential environment variables restic reads for it)
BACKENDS: Dict[str, Dict[str, Any]] = {
    "rclone": {"name": "rclone", "env": ["RCLONE_CONFIG"]},
    "s3": {"name": "S3-compatible storage", "env": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
                                                    "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_SHARED_CREDENTIALS_FILE"]},
    "b2": {"name": "Backblaze B2", "env": ["B2_ACCOUNT_ID", "B2_ACCOUNT_KEY"]},
    "azure": {"name": "Azure Blob Storage", "env": ["AZURE_ACCOUNT_NAME", "AZURE_ACCOUNT_KEY", "AZURE_ACCOUNT_SAS",
                                                    "AZURE_ENDPOINT_SUFFIX"]},
    "gs": {"name": "Google Cloud Storage", "env": ["GOOGLE_PROJECT_ID", "GOOGLE_APPLICATION_CREDENTIALS",
                                                   "GOOGLE_ACCESS_TOKEN"]},
    "swift": {"name": "OpenStack Swift", "env": ["OS_AUTH_URL", "OS_REGION_NAME", "OS_USERNAME", "OS_PASSWORD",
                                                 "OS_TENANT_NAME", "OS_PROJECT_NAME", "OS_USER_DOMAIN_NAME",
                                                 "OS_PROJECT_DOMAIN_NAME", "OS_APPLICATION_CREDENTIAL_ID",
                                                 "OS_APPLICATION_CREDENTIAL_SECRET", "OS_STORAGE_URL", "OS_AUTH_TOKEN"]},
    "sftp": {"name": "SFTP", "env": []},
    "rest": {"name": "REST server", "env": ["RESTIC_REST_USERNAME", "RESTIC_REST_PASSWORD"]},
    "local": {"name": "local directory", "env": []},
}
SECRET_ENV = {"AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "B2_ACCOUNT_KEY", "AZURE_ACCOUNT_KEY", "AZURE_ACCOUNT_SAS",
              "GOOGLE_ACCESS_TOKEN", "OS_PASSWORD", "OS_APPLICATION_CREDENTIAL_SECRET", "OS_AUTH_TOKEN",
              "RESTIC_REST_PASSWORD"}
_URL_PASSWORD = re.compile(r"(://[^:/@]+:)([^@]+)(@)")


def scheme_of(repo: Optional[str]) -> Optional[str]:
    if not repo:
        return None
    m = re.match(r"^([a-z0-9]+):", repo)
    if m and m.group(1) in BACKENDS and m.group(1) != "local":
        return m.group(1)
    return "local"


def mask_repository(repo: Optional[str]) -> Optional[str]:
    """Hide a password embedded in the repository URL, e.g. rest:https://user:secret@host/."""
    return _URL_PASSWORD.sub(r"\1•••\3", repo) if repo else repo


def detect(repo: Optional[str] = None) -> Dict[str, Any]:
    repo = rclone.repository() if repo is None else repo
    scheme = scheme_of(repo)
    info: Dict[str, Any] = {"repository": repo, "scheme": scheme,
                            "name": BACKENDS[scheme]["name"] if scheme else "not configured",
                            "rclone_remote": None, "rclone_type": None}
    if scheme == "local":
        info["location"] = repo.removeprefix("local:") if repo else None
    if scheme == "rclone":
        remote = rclone.remote_for_repository(repo)
        info["rclone_remote"] = remote
        try:
            info["rclone_type"] = (rclone.dump().get(remote) or {}).get("type") if remote else None
        except Exception:
            info["rclone_type"] = None
        if info["rclone_type"]:
            info["name"] = f"rclone ({info['rclone_type']})"
    return info


def credential_env(scheme: Optional[str]) -> List[str]:
    """Credential variables for this backend that are actually set in the environment."""
    keys = BACKENDS.get(scheme or "", {}).get("env", [])
    return [k for k in keys if os.environ.get(k)]

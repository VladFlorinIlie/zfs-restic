"""Optional shared-token auth: accepted as a Bearer token or as the Basic-auth password."""
from __future__ import annotations

import base64
import hmac

from flask import Request, Response

OPEN_PATHS = {"/healthz"}


def presented_token(request: Request) -> str:
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header[7:].strip()
    if header.startswith("Basic "):
        try:
            return base64.b64decode(header[6:]).decode().split(":", 1)[1]
        except Exception:
            return ""
    return ""


def check(request: Request, token: str):
    """Returns None when the request may proceed, otherwise a 401 response."""
    if not token or request.path in OPEN_PATHS:
        return None
    if hmac.compare_digest(presented_token(request).encode(), token.encode()):
        return None
    return Response("Unauthorized\n", 401, {"WWW-Authenticate": 'Basic realm="zfs-restic"'})

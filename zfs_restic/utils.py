from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_time(value: str) -> datetime:
    """Parse restic's RFC3339 timestamps (nanosecond precision) into aware datetimes."""
    value = re.sub(r"(\.\d{6})\d+", r"\1", value.replace("Z", "+00:00"))
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def human_bytes(n: Optional[float]) -> str:
    if n is None:
        return "?"
    if abs(n) < 1024:
        return f"{int(n)} B"
    for unit in ("KiB", "MiB", "GiB", "TiB"):
        n /= 1024
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}"
    raise AssertionError("unreachable")

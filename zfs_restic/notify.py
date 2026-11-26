"""Gotify notifications and healthchecks-style pings."""
from __future__ import annotations

from typing import Optional

import requests

from .settings import Settings
from .shell import Logger

HTTP_TIMEOUT = 15


class Notifier:
    def __init__(self, settings: Settings):
        self.s = settings

    def gotify(self, title: str, message: str, priority: int, log: Logger, success: bool = False):
        if not self.s.gotify_url or not self.s.gotify_token:
            return
        if success and not self.s.gotify_notify_success:
            return
        try:
            requests.post(self.s.gotify_url, params={"token": self.s.gotify_token}, timeout=HTTP_TIMEOUT,
                          json={"title": title, "message": message, "priority": priority}).raise_for_status()
            log(f"Sent Gotify notification: {title}")
        except Exception as e:
            log(f"!!! Could not send Gotify notification: {e}")

    def ping(self, which: str, log: Logger, body: str = ""):
        """which: 'start' | 'success' | 'fail'"""
        url: Optional[str] = {"start": self.s.healthcheck_start_url, "success": self.s.healthcheck_url,
                              "fail": self.s.healthcheck_fail_url}[which]
        if not url:
            return
        try:
            requests.post(url, data=body.encode()[:10000], timeout=HTTP_TIMEOUT).raise_for_status()
            log(f"Pinged healthcheck ({which})")
        except Exception as e:
            log(f"!!! Could not ping healthcheck ({which}): {e}")

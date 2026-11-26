"""Notifications: native Gotify (v1-compatible), Apprise (Matrix, ntfy, Telegram, ...), healthcheck pings."""
from __future__ import annotations

from typing import Dict, List, Optional

import requests

from .settings import Settings
from .shell import Logger

HTTP_TIMEOUT = 15

# level -> Gotify priority (unchanged from v1/v2)
GOTIFY_PRIORITY = {"success": 3, "warning": 7, "failure": 8}


class Notifier:
    def __init__(self, settings: Settings):
        self.s = settings

    @property
    def channels(self) -> List[str]:
        out = []
        if self.s.gotify_url and self.s.gotify_token:
            out.append("gotify")
        out += [f"apprise:{u.split('://', 1)[0]}" for u in self.s.apprise_urls]
        return out

    def notify(self, title: str, message: str, level: str, log: Logger, force: bool = False) -> Dict[str, bool]:
        """level: success | warning | failure. Successes are skipped when NOTIFY_SUCCESS=false (unless forced)."""
        if level == "success" and not self.s.notify_success and not force:
            return {}
        results = {}
        if self.s.gotify_url and self.s.gotify_token:
            results["gotify"] = self._gotify(title, message, GOTIFY_PRIORITY[level], log)
        if self.s.apprise_urls:
            results["apprise"] = self._apprise(title, message, level, log)
        return results

    # Kept for compatibility with existing call sites
    def gotify(self, title: str, message: str, priority: int, log: Logger, success: bool = False):
        level = "success" if success else ("failure" if priority >= 8 else "warning")
        self.notify(title, message, level, log)

    def _gotify(self, title: str, message: str, priority: int, log: Logger) -> bool:
        # Same request as v1: POST {GOTIFY_URL}?token={GOTIFY_TOKEN} with {title, message, priority}
        try:
            requests.post(self.s.gotify_url, params={"token": self.s.gotify_token}, timeout=HTTP_TIMEOUT,
                          json={"title": title, "message": message, "priority": priority}).raise_for_status()
            log(f"Sent Gotify notification: {title}")
            return True
        except Exception as e:
            log(f"!!! Could not send Gotify notification: {e}")
            return False

    def _apprise(self, title: str, message: str, level: str, log: Logger) -> bool:
        try:
            import apprise
            ap = apprise.Apprise()
            for url in self.s.apprise_urls:
                if not ap.add(url):
                    log(f"!!! Apprise could not parse URL with scheme '{url.split('://', 1)[0]}'")
            notify_type = {"success": apprise.NotifyType.SUCCESS, "warning": apprise.NotifyType.WARNING,
                           "failure": apprise.NotifyType.FAILURE}[level]
            ok = bool(ap.notify(title=title, body=message, notify_type=notify_type))
            log(f"Sent Apprise notification: {title}" if ok else "!!! Apprise notification failed for some targets")
            return ok
        except Exception as e:
            log(f"!!! Could not send Apprise notification: {e}")
            return False

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

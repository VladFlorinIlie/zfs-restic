"""Disaster-recovery kit: everything needed to restore without this server."""
from __future__ import annotations

import html
import os
import socket
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from . import __version__, rclone, zfs
from .utils import human_bytes, now_iso

if TYPE_CHECKING:
    from .manager import BackupManager

MASK = "•••••• (hidden; export with secrets to include)"
ENV_KEYS = ["RESTIC_REPOSITORY", "RESTIC_REPOSITORY_FILE", "RESTIC_PASSWORD_FILE", "RESTIC_PASSWORD_COMMAND",
            "RCLONE_CONFIG", "CONFIG_PATH", "DATA_DIR", "RESTORE_DIR", "MAX_AGE_HOURS", "GOTIFY_URL",
            "NOTIFY_SUCCESS", "AUTH_TOKEN", "GOTIFY_TOKEN", "APPRISE_URLS", "HEALTHCHECK_URL"]
SECRET_ENV = {"AUTH_TOKEN", "GOTIFY_TOKEN", "APPRISE_URLS", "HEALTHCHECK_URL"}


def _password(include_secrets: bool) -> Dict[str, Any]:
    if os.environ.get("RESTIC_PASSWORD"):
        return {"source": "RESTIC_PASSWORD environment variable",
                "value": os.environ["RESTIC_PASSWORD"] if include_secrets else MASK}
    if os.environ.get("RESTIC_PASSWORD_FILE"):
        path = os.environ["RESTIC_PASSWORD_FILE"]
        try:
            value = Path(path).read_text().strip()
        except OSError as e:
            value = None
            source = f"RESTIC_PASSWORD_FILE ({path}), which could not be read: {e}"
        else:
            source = f"RESTIC_PASSWORD_FILE ({path})"
        return {"source": source, "value": (value if include_secrets else MASK) if value is not None else None}
    if os.environ.get("RESTIC_PASSWORD_COMMAND"):
        return {"source": "RESTIC_PASSWORD_COMMAND", "command": os.environ["RESTIC_PASSWORD_COMMAND"],
                "value": None}
    return {"source": "not found in the environment", "value": None}


def build_kit(manager: "BackupManager", include_secrets: bool) -> Dict[str, Any]:
    repo = rclone.repository()
    remote = rclone.remote_for_repository(repo)
    kit: Dict[str, Any] = {
        "generated_at": now_iso(), "app_version": __version__, "hostname": socket.gethostname(),
        "include_secrets": include_secrets, "repository": repo, "password": _password(include_secrets),
        "repository_id": manager.restic.repository_id(), "restic_version": manager.restic.version(),
        "rclone_version": rclone.version() if remote else None, "remote": remote,
        "remote_config": None, "remote_error": None, "datasets": [], "config": None, "env": {},
    }
    if remote:
        try:
            kit["remote_config"] = rclone.show(remote, redacted=not include_secrets)
        except Exception as e:
            kit["remote_error"] = str(e)
    try:
        kit["config"] = manager.settings.config_path.read_text()
    except OSError as e:
        kit["config"] = f"# could not read {manager.settings.config_path}: {e}"
    try:
        for d in manager.health()["datasets"]:
            d["snapshot_path"] = str(zfs.mount_point_for(manager.settings.mount_root, d["dataset"]))
            kit["datasets"].append(d)
    except Exception as e:
        kit["datasets_error"] = str(e)
    for key in ENV_KEYS:
        if os.environ.get(key):
            kit["env"][key] = os.environ[key] if (include_secrets or key not in SECRET_ENV) else "•••"
    return kit


def _restore_steps(kit: Dict[str, Any]) -> List[Dict[str, str]]:
    remote = kit["remote"]
    steps = [{"title": "Install restic and rclone",
              "body": f"Use restic {kit['restic_version'].split()[1] if kit['restic_version'].startswith('restic ') else ''} "
                      "or newer (https://github.com/restic/restic/releases)"
                      + (" and rclone (https://rclone.org/downloads/)." if remote else "."),
              "code": ""}]
    if remote:
        steps.append({
            "title": f"Recreate the rclone remote \"{remote}\"",
            "body": "Either paste the remote config from section 1 into ~/.config/rclone/rclone.conf, or create it "
                    f"again with `rclone config` (Google Drive, same name \"{remote}\") and sign in with the Google "
                    "account that owns the backups. Then check it works:",
            "code": f"rclone lsd {remote}:"})
    password_cmd = ("export RESTIC_PASSWORD_COMMAND='" + kit["password"]["command"] + "'"
                    if kit["password"].get("command") else "read -rs RESTIC_PASSWORD && export RESTIC_PASSWORD")
    steps.append({"title": "Point restic at the repository",
                  "body": "Use the repository and password from section 1.",
                  "code": f"export RESTIC_REPOSITORY='{kit['repository'] or '<repository>'}'\n{password_cmd}\n"
                          "restic snapshots"})
    lines = []
    for d in kit["datasets"]:
        safe = d["dataset"].replace("/", "_")
        lines.append(f"restic restore 'latest:{d['snapshot_path']}' --tag '{d['dataset']}' "
                     f"--target '/restore/{safe}'")
    steps.append({"title": "Restore each dataset",
                  "body": "Each command restores the newest snapshot of a dataset into a new folder (the "
                          "`latest:<path>` form strips the internal mount path). Use a snapshot ID instead of "
                          "`latest` for an older version, or `restic mount /mnt/restic` to browse everything.",
                  "code": "\n".join(lines) or "restic restore latest --target /restore"})
    steps.append({"title": "Bring zfs-restic back (optional)",
                  "body": "Run the container again with the same RESTIC_* variables, the rclone config and the "
                          "config.yml from section 4. Existing snapshots are picked up automatically.",
                  "code": ""})
    return steps


def render_markdown(kit: Dict[str, Any]) -> str:
    pw = kit["password"]
    out = [f"# zfs-restic recovery kit",
           f"Generated {kit['generated_at']} by zfs-restic {kit['app_version']} on `{kit['hostname']}`.", ""]
    if kit["include_secrets"]:
        out += ["> **Contains secrets.** Anyone with this document can read and delete your backups. "
                "Store it offline (printed, or in a password manager), never next to the server.", ""]
    else:
        out += ["> Secrets are hidden in this copy. You still need the repository password from somewhere "
                "else: **without it the backups cannot be decrypted by anyone.**", ""]
    out += ["## 1. Access", "",
            f"- **Repository:** `{kit['repository'] or 'not set'}`",
            f"- **Repository ID:** `{kit['repository_id'] or 'unknown'}`",
            f"- **Password source:** {pw['source']}"]
    if pw.get("value"):
        out.append(f"- **Password:** `{pw['value']}`")
    if pw.get("command"):
        out.append(f"- **Password command:** `{pw['command']}`")
    out.append(f"- **restic:** {kit['restic_version']}")
    if kit["remote"]:
        out += [f"- **rclone:** {kit['rclone_version']}", "", f"rclone remote `{kit['remote']}`:", "", "```ini",
                kit["remote_config"] or f"# could not read: {kit['remote_error']}", "```"]
    out += ["", "## 2. What is backed up", "",
            "| Dataset | Snapshots | Newest | Size | Path inside snapshots |", "|---|---|---|---|---|"]
    for d in kit["datasets"]:
        out.append(f"| `{d['dataset']}` | {d['snapshots']} | {d['latest_time'] or '—'} "
                   f"({d['latest_id'] or '—'}) | {human_bytes(d['latest_total_bytes'])} | `{d['snapshot_path']}` |")
    if kit.get("datasets_error"):
        out.append(f"\n(could not list snapshots: {kit['datasets_error']})")
    out += ["", "## 3. Restore on a new machine", ""]
    for i, step in enumerate(_restore_steps(kit), 1):
        out += [f"**{i}. {step['title']}**", "", step["body"], ""]
        if step["code"]:
            out += ["```sh", step["code"], "```", ""]
    out += ["## 4. config.yml", "", "```yaml", (kit["config"] or "").rstrip(), "```", "",
            "## 5. Container environment", ""]
    out += [f"- `{k}` = `{v}`" for k, v in kit["env"].items()] or ["(none of the known variables are set)"]
    return "\n".join(out) + "\n"


def render_html(kit: Dict[str, Any]) -> str:
    e = html.escape
    pw = kit["password"]
    rows = "".join(
        f"<tr><td><code>{e(d['dataset'])}</code></td><td>{d['snapshots']}</td>"
        f"<td>{e(d['latest_time'] or '—')}<br><code>{e(d['latest_id'] or '')}</code></td>"
        f"<td>{e(human_bytes(d['latest_total_bytes']))}</td><td><code>{e(d['snapshot_path'])}</code></td></tr>"
        for d in kit["datasets"])
    steps = "".join(
        f"<li><b>{e(s['title'])}</b><p>{e(s['body'])}</p>" + (f"<pre>{e(s['code'])}</pre>" if s["code"] else "")
        + "</li>" for s in _restore_steps(kit))
    env = "".join(f"<tr><td><code>{e(k)}</code></td><td><code>{e(v)}</code></td></tr>" for k, v in kit["env"].items())
    banner = ("<div class='warn'><b>Contains secrets.</b> Anyone with this page can read and delete your backups. "
              "Print it or keep it in a password manager, never next to the server.</div>"
              if kit["include_secrets"] else
              "<div class='note'>Secrets are hidden in this copy. You still need the repository password from "
              "somewhere else: <b>without it the backups cannot be decrypted by anyone.</b></div>")
    pw_rows = f"<tr><th>Password source</th><td>{e(pw['source'])}</td></tr>"
    if pw.get("value"):
        pw_rows += f"<tr><th>Password</th><td><code class='secret'>{e(pw['value'])}</code></td></tr>"
    if pw.get("command"):
        pw_rows += f"<tr><th>Password command</th><td><code>{e(pw['command'])}</code></td></tr>"
    remote = ""
    if kit["remote"]:
        remote = (f"<tr><th>rclone</th><td>{e(kit['rclone_version'] or '')}</td></tr>"
                  f"<tr><th>rclone remote <code>{e(kit['remote'])}</code></th><td><pre>"
                  f"{e(kit['remote_config'] or 'could not read: ' + str(kit['remote_error']))}</pre></td></tr>")
    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<title>zfs-restic recovery kit</title>
<style>
 body {{ font: 13px/1.5 system-ui, sans-serif; color: #111; max-width: 900px; margin: 24px auto; padding: 0 20px; }}
 h1 {{ font-size: 22px; margin-bottom: 0; }} h2 {{ font-size: 16px; border-bottom: 2px solid #111; padding-bottom: 2px; margin-top: 28px; }}
 table {{ border-collapse: collapse; width: 100%; }} th, td {{ text-align: left; vertical-align: top; padding: 5px 8px; border: 1px solid #bbb; }}
 th {{ background: #f2f2f2; width: 180px; }} code, pre {{ font: 12px/1.45 ui-monospace, Menlo, Consolas, monospace; }}
 pre {{ background: #f6f6f6; border: 1px solid #ccc; padding: 8px; white-space: pre-wrap; word-break: break-all; margin: 4px 0; }}
 .warn {{ border: 2px solid #b00; background: #fff0f0; padding: 10px; margin: 14px 0; }}
 .note {{ border: 2px solid #a70; background: #fff8e8; padding: 10px; margin: 14px 0; }}
 .secret {{ font-size: 14px; font-weight: 700; letter-spacing: .5px; }} .muted {{ color: #666; }}
 ol li {{ margin-bottom: 10px; }} p {{ margin: 2px 0; }}
 .toolbar {{ margin: 10px 0; }} @media print {{ .toolbar {{ display: none; }} body {{ margin: 0; }} }}
</style></head><body>
<div class="toolbar"><button onclick="window.print()">Print / save as PDF</button></div>
<h1>zfs-restic recovery kit</h1>
<div class="muted">Generated {e(kit['generated_at'])} · zfs-restic {e(kit['app_version'])} · host {e(kit['hostname'])}</div>
{banner}
<h2>1. Access</h2><table>
<tr><th>Repository</th><td><code>{e(kit['repository'] or 'not set')}</code></td></tr>
<tr><th>Repository ID</th><td><code>{e(kit['repository_id'] or 'unknown')}</code></td></tr>
{pw_rows}<tr><th>restic</th><td>{e(kit['restic_version'])}</td></tr>{remote}</table>
<h2>2. What is backed up</h2><table><tr><th>Dataset</th><th>Snapshots</th><th>Newest</th><th>Size</th><th>Path inside snapshots</th></tr>{rows}</table>
<h2>3. Restore on a new machine</h2><ol>{steps}</ol>
<h2>4. config.yml</h2><pre>{e(kit['config'] or '')}</pre>
<h2>5. Container environment</h2><table>{env or '<tr><td>(none of the known variables are set)</td></tr>'}</table>
</body></html>"""

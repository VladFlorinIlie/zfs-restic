"""Notifications, diff/find, retention preview, restore, config editor, rclone, storage and recovery kit."""
import json
import os
import threading
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from zfs_restic.manager import BackupManager
from zfs_restic.web import create_app

AUTH = {"Authorization": "Bearer tok"}


def make(env, **overrides):
    m = BackupManager(replace(env.settings, **overrides))
    return m, create_app(m).test_client()


def backup(client, m, headers=None):
    r = client.post("/backup", headers=headers or {})
    assert r.status_code == 202, r.data
    assert m.wait_idle(120)
    return m.db.get_run(r.get_json()["run_id"])


@pytest.fixture
def http_sink():
    """A tiny HTTP server that records every request (stands in for Gotify / Apprise json://)."""
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            received.append({"path": urlparse(self.path).path, "query": parse_qs(urlparse(self.path).query),
                             "json": json.loads(body or b"null")})
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"id": 1}')

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{srv.server_port}", received
    srv.shutdown()


# --- notifications ---

def test_gotify_request_is_unchanged_from_v1(env, http_sink):
    addr, received = http_sink
    m, c = make(env, gotify_url=f"http://{addr}/message", gotify_token="AbCd")
    backup(c, m)
    env.settings.config_path.write_text("datasets: [tank/missing, tank/docs]\n")
    backup(c, m)
    env.settings.config_path.write_text("datasets: [tank/missing]\n")
    backup(c, m)
    gotify = [r for r in received if r["path"] == "/message"]
    assert [r["query"] for r in gotify] == [{"token": ["AbCd"]}] * 3
    assert [r["json"]["priority"] for r in gotify] == [3, 7, 8]
    assert set(gotify[0]["json"]) == {"title", "message", "priority"}
    assert "tank/docs" in gotify[0]["json"]["message"]


def test_notify_success_false_only_sends_problems(env, http_sink):
    addr, received = http_sink
    m, c = make(env, gotify_url=f"http://{addr}/message", gotify_token="t", notify_success=False)
    backup(c, m)
    assert received == []
    env.settings.config_path.write_text("datasets: [tank/missing]\n")
    backup(c, m)
    assert len(received) == 1 and received[0]["json"]["priority"] == 8


def test_apprise_alongside_gotify_and_test_endpoint(env, http_sink):
    addr, received = http_sink
    m, c = make(env, gotify_url=f"http://{addr}/message", gotify_token="t",
                apprise_urls=(f"json://{addr}/apprise",))
    assert m.notifier.channels == ["gotify", "apprise:json"]
    r = c.post("/api/notifications/test", json={})
    assert r.status_code == 200 and r.get_json()["results"] == {"gotify": True, "apprise": True}
    apprise_msg = next(x for x in received if x["path"] == "/apprise")["json"]
    assert apprise_msg["title"].startswith("🔔") and apprise_msg["type"] == "success"


def test_settings_from_env_keeps_v1_variables():
    from zfs_restic.settings import Settings
    s = Settings.from_env({"GOTIFY_URL": "https://g/message", "GOTIFY_TOKEN": "x", "GOTIFY_NOTIFY_SUCCESS": "false",
                           "APPRISE_URLS": "matrix://a, ntfy://b"})
    assert (s.gotify_url, s.gotify_token, s.notify_success) == ("https://g/message", "x", False)
    assert s.apprise_urls == ("matrix://a", "ntfy://b")


# --- diff / find / retention preview ---

def test_diff_and_find(env, manager, client):
    backup(client, manager)
    env.write("tank/docs/readme.txt", "changed\n")
    env.write("tank/docs/new.txt", "new\n")
    (env.pool / "tank/docs/sub/nested file.txt").unlink()
    backup(client, manager)
    docs = [s for s in env.restic_json("snapshots") if "tank/docs" in s["tags"]]
    old, new = sorted(docs, key=lambda s: s["time"])
    d = client.get(f"/api/snapshots/{old['short_id']}/diff/{new['short_id']}").get_json()
    mods = {c["path"].rsplit("/", 1)[-1]: c["modifier"] for c in d["changes"]}
    assert mods["new.txt"] == "+" and mods["nested file.txt"] == "-" and mods["readme.txt"] == "M"
    assert d["statistics"]["added"]["files"] >= 1

    f = client.get("/api/find?pattern=nested*").get_json()
    assert len(f["results"]) == 1 and len(f["results"][0]["seen"]) == 1  # only in the older snapshot
    f = client.get("/api/find?pattern=README.TXT&ignore_case=1&dataset=tank/docs").get_json()
    assert len(f["results"][0]["seen"]) == 2
    assert client.get("/api/find?pattern=").status_code == 400


def test_retention_preview_uses_draft(env, manager, client):
    backup(client, manager)
    backup(client, manager)
    r = client.post("/api/retention/preview", json={"content": "datasets: [tank/docs]\nretention: {keep-last: 1}\n"})
    groups = r.get_json()["groups"]
    assert len(groups) == 2 and all(len(g["keep"]) == 1 and len(g["remove"]) == 1 for g in groups)
    assert groups[0]["keep"][0]["reasons"] == ["last snapshot"]
    assert client.post("/api/retention/preview", json={"content": "datasets: []\n"}).get_json()["groups"] == []
    assert client.post("/api/retention/preview", json={"content": "datasets: x"}).status_code == 400
    assert len(env.restic_json("snapshots")) == 4  # dry run only


# --- restore ---

def test_restore_dir_and_file(env, client, manager, tmp_path):
    restore_root = tmp_path / "pool2"
    restore_root.mkdir()
    m, c = make(env, restore_dir=restore_root / "restores")  # created on demand
    backup(c, m)
    snap = next(s for s in env.restic_json("snapshots") if "tank/docs" in s["tags"])
    root = snap["paths"][0]
    r = c.post("/api/restore", json={"snapshot_id": snap["short_id"], "path": f"{root}/sub", "is_dir": True})
    assert r.status_code == 202, r.data
    assert m.wait_idle(60)
    target = r.get_json()["target"]
    assert open(os.path.join(target, "nested file.txt")).read() == "nested\n"
    r = c.post("/api/restore", json={"snapshot_id": snap["short_id"], "path": f"{root}/readme.txt"})
    assert m.wait_idle(60)
    assert open(r.get_json()["target"]).read() == "hello\n"
    run = m.db.get_run(r.get_json()["run_id"])
    assert run["kind"] == "restore" and run["outcome"] == "success"


def test_restore_unavailable_without_volume(env, manager, client):
    from pathlib import Path
    m, c = make(env, restore_dir=Path("/definitely-not-here"))
    r = c.post("/api/restore", json={"snapshot_id": "abcdef12", "path": "/x", "is_dir": True})
    assert r.status_code == 409 and "RESTORE_DIR" in r.get_json()["message"]
    assert c.post("/api/restore", data="x", content_type="text/plain").status_code == 400  # JSON only


# --- config editor ---

def test_config_editor(env):
    m, c = make(env, auth_token="tok")
    cfg = c.get("/api/config", headers=AUTH).get_json()
    assert cfg["writable"] and cfg["validation"]["ok"]
    original = cfg["content"]

    v = c.post("/api/config/validate", json={"content": "datasets: [tank/docs, tank/nope]\n"}, headers=AUTH).get_json()
    assert v["ok"] and any("tank/nope" in w for w in v["warnings"])
    v = c.post("/api/config/validate", json={"content": "datasets: [a@b]\n"}, headers=AUTH).get_json()
    assert not v["ok"]

    new = "# edited in UI\ndatasets:\n  - tank/docs\nretention:\n  keep-last: 3\n"
    r = c.put("/api/config", json={"content": new, "sha256": cfg["sha256"]}, headers=AUTH)
    assert r.status_code == 200, r.data
    assert env.settings.config_path.read_text() == new  # comments preserved, written verbatim
    assert c.put("/api/config", json={"content": new, "sha256": cfg["sha256"]}, headers=AUTH).status_code == 409
    bad = c.put("/api/config", json={"content": "datasets: x", "sha256": r.get_json()["sha256"]}, headers=AUTH)
    assert bad.status_code == 400 and env.settings.config_path.read_text() == new

    versions = c.get("/api/config/versions", headers=AUTH).get_json()
    assert len(versions) == 1
    assert c.get(f"/api/config/versions/{versions[0]['id']}", headers=AUTH).get_json()["content"] == original


def test_config_save_requires_auth_token(env, manager, client):
    cfg = client.get("/api/config").get_json()
    r = client.put("/api/config", json={"content": "datasets: [tank/docs]\n", "sha256": cfg["sha256"]})
    assert r.status_code == 403 and "AUTH_TOKEN" in r.get_json()["message"]


def test_config_read_only(env):
    m, c = make(env, auth_token="tok")
    os.chmod(env.settings.config_path, 0o444)
    try:
        if os.access(env.settings.config_path, os.W_OK):
            pytest.skip("running as root: file permissions are not enforced")
        cfg = c.get("/api/config", headers=AUTH).get_json()
        assert not cfg["writable"]
    finally:
        os.chmod(env.settings.config_path, 0o644)


# --- rclone, storage, recovery kit ---

def test_storage_and_health_with_rclone_repo(rclone_env):
    m, c = make(rclone_env)
    backup(c, m)
    s = c.get("/api/storage").get_json()
    assert s["remote"] == "loc" and s["total"] > 0 and 0 <= s["percent_used"] <= 100
    assert c.get("/api/health").get_json()["storage"]["remote"] == "loc"

    m2, c2 = make(rclone_env, quota_warn_percent=0.0)  # everything is "nearly full"
    run = backup(c2, m2)
    assert "loc is" in run["details"] and "% full" in run["details"]
    assert c2.get("/api/health").status_code == 503


def test_rclone_overview_and_drive_remote(rclone_env):
    m, c = make(rclone_env, auth_token="tok")
    o = c.get("/api/rclone", headers=AUTH).get_json()
    assert o["available"] and o["repository_remote"] == "loc" and o["config_writable"]
    assert [r["name"] for r in o["remotes"]] == ["loc"]
    assert c.post("/api/rclone/test", json={"remote": "loc"}, headers=AUTH).get_json()["ok"]

    tok = json.dumps({"access_token": "x", "token_type": "Bearer", "refresh_token": "r", "expiry": "2030-01-01T00:00:00Z"})
    r = c.post("/api/rclone/authorize", json={"name": "gd", "type": "drive", "token": tok}, headers=AUTH)
    assert r.status_code == 200, r.data
    gd = next(x for x in c.get("/api/rclone", headers=AUTH).get_json()["remotes"] if x["name"] == "gd")
    assert gd["type"] == "drive" and gd["options"]["token"] == "•••"
    assert not gd["drive"]["own_client_id"] and any("shared Google Drive client_id" in w for w in gd["warnings"])

    r = c.post("/api/rclone/authorize", json={"name": "gd", "token": tok, "client_id": "id.apps.googleusercontent.com",
                                          "client_secret": "sec"}, headers=AUTH)
    gd = next(x for x in c.get("/api/rclone", headers=AUTH).get_json()["remotes"] if x["name"] == "gd")
    assert gd["drive"]["own_client_id"] and gd["options"]["client_secret"] == "•••" and not gd["warnings"]
    assert len(c.get("/api/rclone", headers=AUTH).get_json()["versions"]) == 2

    # Re-authorising with blank client fields must keep the remote's own client and root folder
    c.post("/api/rclone/authorize", json={"name": "gd", "token": tok, "root_folder_id": "ROOT1"}, headers=AUTH)
    r = c.post("/api/rclone/authorize", json={"name": "gd", "token": tok.replace('"x"', '"y"')}, headers=AUTH)
    assert r.status_code == 200
    from zfs_restic import rclone as rc
    cfg = rc.dump()["gd"]
    assert cfg["client_id"] == "id.apps.googleusercontent.com" and cfg["client_secret"] == "sec"
    assert cfg["root_folder_id"] == "ROOT1" and '"y"' in cfg["token"]

    no_refresh = json.dumps({"access_token": "x"})
    assert c.post("/api/rclone/authorize", json={"name": "gd", "token": no_refresh}, headers=AUTH).status_code == 400
    assert c.post("/api/rclone/authorize", json={"name": "gd", "token": "nope"}, headers=AUTH).status_code == 400
    cmd = c.post("/api/rclone/authorize-command", json={"type": "onedrive", "client_id": "a", "client_secret": "b"},
                 headers=AUTH)
    assert cmd.get_json()["command"] == 'rclone authorize "onedrive" "a" "b"'


def test_rclone_changes_require_auth_token(rclone_env):
    m, c = make(rclone_env)
    assert c.post("/api/rclone/authorize", json={"name": "gd", "token": "{}"}).status_code == 403


def test_recovery_kit(rclone_env):
    m, c = make(rclone_env, auth_token="tok")
    backup(c, m, AUTH)
    hidden = c.get("/api/recovery-kit?format=md", headers=AUTH).get_data(as_text=True)
    assert "test" not in hidden.split("Password source")[1].split("\n")[1]  # password not shown
    assert "restic restore 'latest:" in hidden and "--tag 'tank/docs'" in hidden
    full = c.get("/api/recovery-kit?format=md&secrets=1", headers=AUTH).get_data(as_text=True)
    assert "**Password:** `test`" in full and "[loc]" in full and "rclone:loc:" in full
    page = c.get("/api/recovery-kit?secrets=1", headers=AUTH)
    assert page.headers["Cache-Control"] == "no-store" and b"Contains secrets" in page.data
    assert c.get("/api/status", headers=AUTH).get_json()["recovery_kit_exported_at"]

    m2, c2 = make(rclone_env)  # no AUTH_TOKEN: secrets refused, redacted kit allowed
    assert c2.get("/api/recovery-kit?secrets=1").status_code == 403
    assert c2.get("/api/recovery-kit").status_code == 200


def test_unwritable_data_dir_does_not_stop_backups(env, tmp_path):
    ro = tmp_path / "ro"
    ro.mkdir()
    blocker = ro / "data"
    blocker.write_text("a file where the data dir should be")  # mkdir fails even as root
    m, c = make(env, data_dir=blocker)
    assert m.history_warning and "NOT persistent" in m.history_warning
    assert c.get("/api/status").get_json()["history_warning"] == m.history_warning
    assert backup(c, m)["outcome"] == "success"


def test_cron_backup_skipped_while_busy_notifies(env, http_sink):
    addr, received = http_sink
    m, c = make(env, gotify_url=f"http://{addr}/message", gotify_token="t")
    assert c.post("/api/check", json={}).status_code == 202
    r = c.post("/backup")
    assert r.status_code == 409 and "check" in r.get_json()["message"]
    assert m.wait_idle(120)
    skipped = [x for x in received if x["json"]["title"].startswith("⚠️ Backup skipped")]
    assert len(skipped) == 1 and skipped[0]["json"]["priority"] == 7


def test_reauthorise_any_oauth_remote_type(rclone_env):
    conf = rclone_env.root / "rclone.conf"
    conf.write_text(conf.read_text() + "\n[od]\ntype = onedrive\ndrive_id = D1\ndrive_type = personal\n"
                    'token = {"access_token":"old","refresh_token":"r","expiry":"2020-01-01T00:00:00Z"}\n'
                    "\n[pc]\ntype = pcloud\n")
    m, c = make(rclone_env, auth_token="tok")
    remotes = {r["name"]: r for r in c.get("/api/rclone", headers=AUTH).get_json()["remotes"]}
    assert remotes["od"]["can_reauthorise"] and not remotes["loc"]["can_reauthorise"]
    assert not any("client_id" in w for w in remotes["od"]["warnings"])  # Drive-only warning

    new = json.dumps({"access_token": "new", "refresh_token": "r2", "expiry": "2030-01-01T00:00:00Z"})
    r = c.post("/api/rclone/authorize", json={"name": "od", "token": new}, headers=AUTH)
    assert r.status_code == 200 and r.get_json()["type"] == "onedrive", r.data
    from zfs_restic import rclone as rc
    od = rc.dump()["od"]
    assert '"new"' in od["token"] and od["drive_id"] == "D1" and od["drive_type"] == "personal"

    # pcloud tokens never expire and carry no refresh_token: accepted
    forever = json.dumps({"access_token": "p", "token_type": "bearer", "expiry": "0001-01-01T00:00:00Z"})
    assert c.post("/api/rclone/authorize", json={"name": "pc", "token": forever}, headers=AUTH).status_code == 200
    # drive-only options are refused for other types; non-OAuth remotes and non-creatable new types too
    assert c.post("/api/rclone/authorize", json={"name": "od", "token": new, "root_folder_id": "x"},
                  headers=AUTH).status_code == 400
    assert c.post("/api/rclone/authorize", json={"name": "loc", "token": new}, headers=AUTH).status_code == 400
    assert c.post("/api/rclone/authorize", json={"name": "new-od", "type": "onedrive", "token": new},
                  headers=AUTH).status_code == 400


def test_backend_detection_and_masking():
    from zfs_restic import backend
    assert backend.scheme_of("rclone:gdrive:restic") == "rclone"
    assert backend.scheme_of("s3:https://s3.amazonaws.com/bucket") == "s3"
    assert backend.scheme_of("/srv/restic") == "local" and backend.scheme_of("local:/srv/r") == "local"
    assert backend.scheme_of("sftp:user@host:/srv/restic") == "sftp"
    assert backend.mask_repository("rest:https://u:secret@host:8000/r") == "rest:https://u:•••@host:8000/r"
    assert backend.mask_repository("s3:https://s3.amazonaws.com/b") == "s3:https://s3.amazonaws.com/b"


def test_recovery_kit_is_backend_generic(env, monkeypatch):
    m, c = make(env, auth_token="tok")  # `env` uses a plain local-path repository
    backup(c, m, AUTH)
    md = c.get("/api/recovery-kit?format=md&secrets=1", headers=AUTH).get_data(as_text=True)
    assert "**Backend:** local directory" in md and "Make the repository folder available" in md
    assert "rclone" not in md.split("## 3.")[1].split("## 4.")[0]  # no rclone steps for a non-rclone repo

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA1")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "topsecret")
    from zfs_restic import backend, recovery
    monkeypatch.setattr(backend.rclone, "repository", lambda: "s3:https://s3.example.com/bucket")
    kit = recovery.build_kit(m, include_secrets=False)
    md = recovery.render_markdown(kit)
    assert "S3-compatible storage" in md and "AKIA1" in md and "topsecret" not in md
    assert "export AWS_SECRET_ACCESS_KEY='•••'" in md
    assert "topsecret" in recovery.render_markdown(recovery.build_kit(m, include_secrets=True))

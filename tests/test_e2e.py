"""End-to-end: real restic + local repository, fake zfs/mount (see conftest)."""
import base64
import io
import zipfile

from zfs_restic.web import create_app


def run_backup(client, manager):
    r = client.post("/backup")
    assert r.status_code == 202
    assert manager.wait_idle(120)
    return manager.db.get_run(r.get_json()["run_id"])


def test_backup_restore_roundtrip(env, manager, client):
    run = run_backup(client, manager)
    assert run["outcome"] == "success", run["log"]
    assert {r["dataset"]: r["outcome"] for r in run["results"]} == {"tank/photos": "success", "tank/docs": "success"}

    # v1-compatible snapshot layout: path + tags
    snaps = env.restic_json("snapshots")
    docs = next(s for s in snaps if "tank/docs" in s["tags"])
    assert docs["paths"] == [str(env.settings.mount_root / "restic_backup_tank_docs")]
    assert any(t.startswith("tank/docs@restic-backup-") for t in docs["tags"])

    # nothing left behind
    assert not list((env.root / "snaps").iterdir())
    assert not list(env.settings.mount_root.iterdir())

    # browse + download
    root = docs["paths"][0]
    entries = client.get(f"/api/snapshots/{docs['short_id']}/ls?path={root}").get_json()
    assert [e["name"] for e in entries] == ["sub", "readme.txt"]
    r = client.get(f"/api/snapshots/{docs['short_id']}/download?path={root}/readme.txt")
    assert r.status_code == 200 and r.data == b"hello\n"
    r = client.get(f"/api/snapshots/{docs['short_id']}/download?path={root}/sub&dir=1")
    names = zipfile.ZipFile(io.BytesIO(r.data)).namelist()
    assert any(n.endswith("sub/nested file.txt") for n in names)
    assert client.get(f"/api/snapshots/{docs['short_id']}/download?path=/nope").status_code == 404
    assert client.get("/api/snapshots/not-an-id/ls?path=/").status_code == 400

    # health
    r = client.get("/api/health")
    assert r.status_code == 200 and r.get_json()["ok"]


def test_second_run_uses_parent_and_retention_groups(env, manager, client):
    first = run_backup(client, manager)
    env.write("tank/docs/readme.txt", "changed\n")
    second = run_backup(client, manager)
    assert second["outcome"] == "success"
    first_ids = {r["dataset"]: r["snapshot_id"][:8] for r in first["results"]}
    assert f"Using parent snapshot {first_ids['tank/docs']}" in second["log"]
    docs = next(r for r in second["results"] if r["dataset"] == "tank/docs")
    # (the fake mount copies files, so inodes change and restic re-reads them; real ZFS mounts don't)
    assert docs["files_new"] == 0 and docs["files_changed"] >= 1


def test_failing_dataset_does_not_block_others(env, manager, client):
    env.settings.config_path.write_text("datasets: [tank/missing, tank/docs]\n")
    run = run_backup(client, manager)
    assert run["outcome"] == "partial"
    assert {r["dataset"]: r["outcome"] for r in run["results"]} == {"tank/missing": "failure", "tank/docs": "success"}
    assert client.get("/api/health").status_code == 503  # tank/missing has no snapshot


def test_leftovers_from_crashed_run_are_cleaned(env, manager, client):
    leftover = env.root / "snaps" / "tank__docs@restic-backup-2026-01-01_00-00-00"
    foreign = env.root / "snaps" / "tank__docs@manual-snapshot"
    leftover.mkdir()
    foreign.mkdir()
    run = run_backup(client, manager)
    assert "Destroying leftover snapshot tank/docs@restic-backup-2026-01-01_00-00-00" in run["log"]
    assert not leftover.exists()
    assert foreign.exists()  # never touches snapshots it didn't create


def test_interrupted_runs_are_marked_on_startup(env, manager):
    run_id = manager.db.create_run("backup", "api", "2026-01-01T00:00:00+00:00")
    from zfs_restic.manager import BackupManager
    BackupManager(env.settings)  # simulates a container restart
    assert manager.db.get_run(run_id)["outcome"] == "interrupted"


def test_concurrent_job_rejected_and_check_runs(env, manager, client):
    r1 = client.post("/api/check", json={"read_data_subset": "50%"})
    r2 = client.post("/backup")
    assert r1.status_code == 202 and r2.status_code == 409
    assert manager.wait_idle(120)
    run = manager.db.get_run(r1.get_json()["run_id"])
    assert run["outcome"] == "success" and "50%" in run["details"]
    assert client.post("/api/check", json={"read_data_subset": "x; rm"}).status_code == 400


def test_v1_status_shape(env, manager, client):
    s = client.get("/status").get_json()
    for key in ("live_status", "current_task", "last_run_start_time", "last_completed_run", "log"):
        assert key in s
    run_backup(client, manager)
    assert client.get("/status").get_json()["last_completed_run"]["outcome"] == "success"
    assert isinstance(client.get("/snapshots").get_json(), list)


def test_auth(env, manager):
    from dataclasses import replace
    manager.settings = replace(manager.settings, auth_token="s3cret")
    c = create_app(manager).test_client()
    assert c.get("/api/runs").status_code == 401
    assert c.get("/healthz").status_code == 200
    assert c.get("/api/runs", headers={"Authorization": "Bearer s3cret"}).status_code == 200
    basic = base64.b64encode(b"cron:s3cret").decode()
    assert c.get("/api/runs", headers={"Authorization": f"Basic {basic}"}).status_code == 200
    assert c.get("/api/runs", headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_ui_served(client):
    assert b"zfs-restic" in client.get("/").data
    assert client.get("/static/js/main.js").status_code == 200
    assert client.get("/static/css/style.css").status_code == 200

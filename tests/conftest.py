"""Test fixtures. End-to-end tests use a real restic binary against a local repository,
with fake `zfs`/`mount`/`umount` commands (tests/fakebin) that copy directories instead."""
from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from zfs_restic.manager import BackupManager
from zfs_restic.settings import Settings
from zfs_restic.web import create_app

FAKEBIN = Path(__file__).parent / "fakebin"

V1_CONFIG = """\
datasets:
  - tank/photos
  - tank/docs
retention:
  keep-daily: 7
  keep-weekly: 4
"""


@dataclass
class Env:
    root: Path
    pool: Path
    settings: Settings

    def write(self, rel: str, content: str | bytes):
        p = self.pool / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content.encode() if isinstance(content, str) else content)

    def restic_json(self, *args: str):
        import json
        return json.loads(subprocess.run(["restic", *args, "--json"], check=True, capture_output=True,
                                         text=True).stdout)


@pytest.fixture
def env(tmp_path, monkeypatch):
    if not shutil.which("restic"):
        pytest.skip("restic binary not on PATH")
    pool, snaps, repo, mnt, data = (tmp_path / d for d in ("pool", "snaps", "repo", "mnt", "data"))
    for d in (pool, snaps, mnt):
        d.mkdir()
    monkeypatch.setenv("PATH", f"{FAKEBIN}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKEPOOL", str(pool))
    monkeypatch.setenv("FAKESNAPS", str(snaps))
    monkeypatch.setenv("RESTIC_REPOSITORY", str(repo))
    monkeypatch.setenv("RESTIC_PASSWORD", "test")
    monkeypatch.setenv("RESTIC_CACHE_DIR", str(tmp_path / "cache"))
    subprocess.run(["restic", "init", "-q"], check=True)

    config = tmp_path / "config.yml"
    config.write_text(V1_CONFIG)
    settings = Settings(config_path=config, data_dir=data, mount_root=mnt, progress_fps="5")
    e = Env(tmp_path, pool, settings)
    e.write("tank/photos/a.jpg", os.urandom(200_000))
    e.write("tank/photos/2025/b.jpg", os.urandom(100_000))
    e.write("tank/docs/readme.txt", "hello\n")
    e.write("tank/docs/sub/nested file.txt", "nested\n")
    return e


@pytest.fixture
def rclone_env(env, monkeypatch):
    """Same as `env`, but the repository lives behind an rclone remote ("loc", type local)."""
    if not shutil.which("rclone"):
        pytest.skip("rclone binary not on PATH")
    conf = env.root / "rclone.conf"
    conf.write_text("[loc]\ntype = local\n")
    repo = env.root / "rrepo"
    monkeypatch.setenv("RCLONE_CONFIG", str(conf))
    monkeypatch.setenv("RESTIC_REPOSITORY", f"rclone:loc:{repo}")
    subprocess.run(["restic", "init", "-q"], check=True)
    return env


@pytest.fixture
def manager(env):
    return BackupManager(env.settings)


@pytest.fixture
def client(manager):
    return create_app(manager).test_client()

# zfs-restic

Backs up ZFS datasets with restic: for each dataset it takes a ZFS snapshot, mounts it, runs `restic backup` against the mount, cleans up, then applies the retention policy. Runs are triggered over HTTP (e.g. from a TrueNAS cron job). The web UI is on the same port.

## Web UI (`http://<host>:8000/`)

| Tab | What it does |
|---|---|
| **Dashboard** | Live progress of the running job (progress bar, ETA, current file, log); per-dataset freshness cards; Drive storage usage; run history with per-dataset results and full logs. |
| **Snapshots** | Browse any snapshot; download files, or folders as `.zip`; **restore a file or folder to the server** (into `RESTORE_DIR`, never overwriting anything); **see what changed** between a snapshot and the previous one. |
| **Find a file** | Search every snapshot for a name or glob. Shows which snapshots contain each file, when it was first and last seen, and flags files that are gone from the newest snapshot. |
| **Config** | Edit `config.yml` in place, with validation (including "does this dataset exist?"), a **retention preview** (`restic forget --dry-run` with the edited policy) and previous versions. |
| **rclone** | Remotes and their health (e.g. still on rclone's shared Google client_id), connection test, and a guided form to set up or re-authorise a Google Drive remote. |
| **Recovery & alerts** | The **disaster-recovery kit**, plus notification channels and a test button. |

Saving the config, changing rclone and exporting the kit with secrets only work when `AUTH_TOKEN` is set.

## Configuration

`/config/config.yml` (same format as before):

```yaml
datasets:
  - tank/photos
  - tank/documents
retention:              # passed to `restic forget --prune --group-by paths`
  keep-daily: 7
  keep-weekly: 4
  keep-monthly: 12
check:                  # optional
  read_data_subset: "2%"  # default subset for /api/check when none is given
```

Each dataset is snapshotted **non-recursively**. Child datasets are separate mounts and were never included in the backup. List them explicitly if you want them backed up.

### Environment

| Variable | Default | Purpose |
|---|---|---|
| `RESTIC_REPOSITORY`, `RESTIC_PASSWORD`, … | – | Standard restic variables, e.g. `RESTIC_REPOSITORY=rclone:gdrive:backups/restic` for Google Drive via rclone |
| `RCLONE_*` | – | rclone reads its options from the environment, e.g. `RCLONE_CONFIG` or `RCLONE_DRIVE_USE_TRASH=false` (see below). Its config file defaults to `/root/.config/rclone/rclone.conf`. |
| `AUTH_TOKEN` | unset | Protects the UI and API. Send it as a Bearer token, or as the password in Basic auth (any username). `/healthz` stays open. |
| `DATA_DIR` | `/data` | Where the SQLite history lives. **Mount a writable persistent folder here.** |
| `MAX_AGE_HOURS` | `36` | A dataset counts as stale when its newest snapshot is older than this |
| `HISTORY_LIMIT` | `500` | Number of runs kept in history |
| `RESTORE_DIR` | `/restore` | Where "restore to server" puts files, each restore in its own new folder. Mount a dedicated writable folder here (e.g. host `/mnt/tank/restores` → `/restore`); restores are disabled if it's missing or read-only. |
| `GOTIFY_URL`, `GOTIFY_TOKEN` | unset | Gotify notifications, sent exactly as in v1 (`POST GOTIFY_URL?token=…`, priority 3/7/8 for success/partial/failure) |
| `APPRISE_URLS` | unset | More notification targets via [Apprise](https://github.com/caronc/apprise/wiki), comma- or space-separated: Matrix (`matrixs://…`), ntfy, Telegram, email, … Sent alongside Gotify. |
| `NOTIFY_SUCCESS` | `true` | Set to `false` to be notified only about problems (`GOTIFY_NOTIFY_SUCCESS` still works) |
| `QUOTA_WARN_PERCENT` | `90` | Warn (notification, dashboard, `/api/health` → 503) when the rclone remote holding the repository is this full |
| `HEALTHCHECK_URL` | unset | Pinged (POST, with the summary as the body) after a fully successful backup. Works with healthchecks.io and Uptime Kuma push monitors. |
| `HEALTHCHECK_FAIL_URL` | unset | Pinged on partial or failed backups (e.g. `https://hc-ping.com/<uuid>/fail`) |
| `HEALTHCHECK_START_URL` | unset | Pinged when a backup starts (e.g. `https://hc-ping.com/<uuid>/start`) |
| `CONFIG_PATH` | `/config/config.yml` | |
| `PORT` | `8000` | |

## API

| Method | Path | |
|---|---|---|
| POST | `/backup` (or `/api/backup`) | Start a backup. Returns `202`, or `409` if a job is already running. Unchanged from the original API. |
| POST | `/api/check` | Start `restic check`. Optional JSON body: `{"read_data_subset": "5%"}` |
| GET | `/status` (or `/api/status`) | Live status. Same keys as before, plus `job`, `run_id`, `progress`, `dataset_index`/`dataset_total` |
| GET | `/snapshots` | Raw `restic snapshots --json`, as before |
| GET | `/api/runs`, `/api/runs/<id>` | Run history, and one run with its per-dataset results and log |
| GET | `/api/health` | `200` if every dataset is fresh and storage isn't nearly full, `503` otherwise. Point an Uptime Kuma HTTP monitor at it. |
| GET | `/api/snapshots/<id>/ls?path=` | List a directory inside a snapshot |
| GET | `/api/snapshots/<id>/download?path=[&dir=1]` | Download a file, or a directory as a zip |
| GET | `/api/snapshots/<old>/diff/<new>` | What changed between two snapshots |
| GET | `/api/find?pattern=&dataset=&ignore_case=1` | Search every snapshot |
| POST | `/api/restore` | `{"snapshot_id", "path", "is_dir"}`: restore into a new folder under `RESTORE_DIR` |
| GET/PUT | `/api/config` | Read / save `config.yml` (PUT needs the `sha256` you loaded; `409` if the file changed meanwhile) |
| POST | `/api/config/validate`, `/api/retention/preview` | Validate a draft config; dry-run its retention policy |
| GET | `/api/storage` | Usage of the rclone remote holding the repository |
| GET/POST | `/api/rclone`, `/api/rclone/test`, `/api/rclone/drive` | rclone overview, connection test, create/update a Drive remote |
| GET | `/api/recovery-kit?format=html\|md[&secrets=1]` | The disaster-recovery kit |
| POST | `/api/notifications/test` | Send a test notification to every channel |
| GET | `/healthz` | Liveness check, no auth |

Example TrueNAS cron jobs:

```sh
# nightly backup
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8000/backup
# weekly integrity check that reads 5% of the pack data
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -d '{"read_data_subset":"5%"}' http://127.0.0.1:8000/api/check
```

## Reliability behaviour

- If one dataset fails, the others are still backed up. The run is then marked `partial` and the notification lists what failed.
- At the start of each backup it checks that the repository is reachable, then runs `restic unlock` (this removes only stale locks, e.g. after a container crash).
- ZFS snapshots and mounts left behind by an interrupted run are cleaned up at the start of the next one. Runs that were in progress when the container died are recorded as `interrupted`.
- restic exit code 3 (snapshot created, but some files were unreadable) is recorded as `partial`, not as a failure.
- Retention (`forget --prune`) still runs if some datasets failed, but is skipped if every dataset failed.
- The snapshot browser and downloads use `--no-lock`, so they never block or wait on a running backup.

## Google Drive notes

- **Use your own Google OAuth client.** rclone's built-in client_id is being retired during 2026, and remotes still using it will stop working. The rclone tab flags affected remotes and walks you through it: create a Desktop OAuth client in Google Cloud, **publish the consent screen to "In production"** (in "Testing", tokens expire after 7 days), run `rclone authorize "drive" "<id>" "<secret>"` on your PC and paste the token.
- **Pruned data goes to the Drive trash by default.** rclone moves deleted files to the trash, so space freed by `forget --prune` only comes back once Drive empties the trash (after 30 days). Set `RCLONE_DRIVE_USE_TRASH=false` to delete immediately. Keeping the trash on gives you a 30-day undo for a bad prune, at the cost of storage.
- **Keep `rclone.conf` writable.** rclone saves refreshed OAuth tokens back into it.
- **Browsing, downloads and `check --read-data-subset` all download from Drive**, so they count against Drive's API and download quotas. The snapshot list is cached for `SNAPSHOT_CACHE_TTL` seconds (default 600), so a health monitor polling `/api/health` doesn't hit Drive on every request.

## Example: TrueNAS SCALE custom app

```
Environment:
  RESTIC_REPOSITORY = rclone:gdrive:<path>
  RESTIC_PASSWORD   = …
  GOTIFY_URL        = https://gotify.example/message
  GOTIFY_TOKEN      = …
  AUTH_TOKEN        = <openssl rand -hex 32>
Host paths (all read-write):
  /mnt/tank/apps/zfs-restic/config -> /config
  /mnt/tank/apps/zfs-restic/rclone -> /root/.config/rclone
  /mnt/tank/apps/zfs-restic/data   -> /data
  /mnt/tank/restores               -> /restore      (optional: enables "restore to server")
```

Backups don't need the pools mounted at all: snapshots are taken and mounted through ZFS itself. If you do mount pools (e.g. to browse them), mount them read-only; only the paths above need write access. Mount the config folder rather than the single file. If `/data` isn't writable the app keeps running, but run history isn't saved and the UI shows a warning.

## Upgrading from v1

Existing repositories, snapshots and `config.yml` work as they are. The mount paths (`/mnt/restic_backup_<dataset>`) and tags are unchanged, so new backups use your old snapshots as parents and share their retention groups.

- **Mount a volume at `/data`.** The run history lives there. Without a volume it's lost when the container is recreated (backups are not affected).
- **The start command changed** to `python3 -m zfs_restic`. If your TrueNAS app overrides the command (e.g. `python3 app.py`), remove that override.
- **The base image is Debian 13**, so the container's ZFS tools are now 2.3.x (v1 shipped 2.1). This is a better match for current TrueNAS SCALE. If you're on an older release and `zfs snapshot` fails, compare the versions with `zfs version` on the host. To go back, build with `--build-arg DEBIAN_VERSION=12.15-slim` and edit the `sed` line, since bookworm's `debian.sources` is compatible.
- **Snapshots are no longer taken recursively.** Child datasets were never part of the backup anyway (they're separate mounts), so add them to `datasets` explicitly if you want them backed up.

## Project layout

```
zfs_restic/
  __main__.py      entrypoint (python -m zfs_restic): validates config, starts waitress
  settings.py      environment variables -> Settings
  config.py        config.yml parsing and validation (v1 format)
  config_store.py  config.yml editing from the UI: validation, conflict detection, version history
  manager.py       job lock, live status, caches, health, find, retention preview, restore targets
  jobs.py          backup, check and restore jobs
  zfs.py           ZFS snapshot/mount operations
  restic.py        restic operations and JSON parsing
  rclone.py        rclone remotes, quota, Google Drive (re)authorisation
  recovery.py      disaster-recovery kit (HTML + Markdown)
  notify.py        Gotify (v1-compatible), Apprise, healthcheck pings
  storage.py       SQLite: run history, file versions, key/value
  shell.py         subprocess helpers
  web/
    routes/        Flask blueprints: jobs, snapshots, config, rclone, recovery
    auth.py        optional AUTH_TOKEN (Bearer or Basic)
    static/        UI: index.html, css/, js/ (ES modules, no build step)
tests/             pytest; fakebin/ holds fake zfs/mount/umount for end-to-end tests
```

## Development

```sh
pip install -r requirements-dev.txt
pytest            # end-to-end tests need `restic` (and `rclone` for some) on PATH; they're skipped otherwise
docker build -t zfs-restic .
```

The end-to-end tests run a real restic against a local repository. `tests/fakebin` stands in for `zfs`, `mount` and `umount` by copying directories, so no ZFS pool or root access is needed.

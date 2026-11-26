# zfs-restic

Backs up ZFS datasets with restic: for each dataset it takes a ZFS snapshot, mounts it, runs `restic backup` against the mount, cleans up, then applies the retention policy. Runs are triggered over HTTP (e.g. from a TrueNAS cron job). The web UI is on the same port.

## Web UI (`http://<host>:8000/`)

- **Live progress** of the running job: progress bar, bytes and files, ETA, current file, streaming log.
- **Per-dataset freshness cards**, based on the newest snapshot in the repository. A dataset turns red when its newest snapshot is older than `MAX_AGE_HOURS`.
- **Run history** (stored in SQLite, survives restarts): outcome, duration, data added, and per-dataset results with the full log.
- **Snapshot browser**: open any snapshot, navigate folders, download single files or whole folders as `.zip` (via `restic dump`).
- Buttons for **Run backup now** and **Verify repo** (`restic check`, optionally with `--read-data-subset`).

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
| `RESTIC_REPOSITORY`, `RESTIC_PASSWORD`, … | – | Standard restic variables (plus whatever your backend needs, e.g. `GOOGLE_PROJECT_ID`, `GOOGLE_APPLICATION_CREDENTIALS`) |
| `AUTH_TOKEN` | unset | Protects the UI and API. Send it as a Bearer token, or as the password in Basic auth (any username). `/healthz` stays open. |
| `DATA_DIR` | `/data` | Where the SQLite history lives. **Mount a persistent volume here.** |
| `MAX_AGE_HOURS` | `36` | A dataset counts as stale when its newest snapshot is older than this |
| `HISTORY_LIMIT` | `500` | Number of runs kept in history |
| `GOTIFY_URL`, `GOTIFY_TOKEN` | unset | Gotify notifications (success, partial and failure) |
| `GOTIFY_NOTIFY_SUCCESS` | `true` | Set to `false` to be notified only on problems |
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
| GET | `/api/health` | `200` if every dataset is fresh, `503` otherwise. Point an Uptime Kuma HTTP monitor at it. |
| GET | `/api/snapshots/<id>/ls?path=` | List a directory inside a snapshot |
| GET | `/api/snapshots/<id>/download?path=[&dir=1]` | Download a file, or a directory as a zip |
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

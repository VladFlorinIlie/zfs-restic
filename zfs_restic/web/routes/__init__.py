from . import config, jobs, rclone, recovery, snapshots

BLUEPRINTS = [jobs.bp, snapshots.bp, config.bp, rclone.bp, recovery.bp]

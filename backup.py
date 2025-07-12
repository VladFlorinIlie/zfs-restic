from __future__ import annotations
import os
import subprocess
import json
import yaml
import requests
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from threading import Thread, Lock

# --- Helper for Centralized Logging ---
def log_message(message: str, status_dict: Optional[Dict[str, Any]] = None):
    """Prints a message and optionally appends it to the status log."""
    print(message)
    if status_dict is not None:
        if "log" not in status_dict:
            status_dict["log"] = []
        status_dict["log"].append(str(message))

# --- Client Classes for External Commands ---

class SubprocessClient:
    """A base class for running external commands and optionally logging their output."""
    def __init__(self, status_dict: Optional[Dict[str, Any]] = None):
        self.status_dict = status_dict

    def _run(self, command: List[str], log_command: bool = True, log_output: bool = True) -> subprocess.CompletedProcess:
        if log_command:
            log_message(f"Executing: {' '.join(command)}", self.status_dict)
        env = os.environ.copy()
        result = subprocess.run(command, capture_output=True, text=True, env=env)
        
        if log_command and log_output:
            output_log = []
            if result.stdout: output_log.append(result.stdout)
            if result.stderr: output_log.append(result.stderr)
            if output_log: log_message("\n".join(output_log), self.status_dict)
        
        result.check_returncode()
        return result

class ZFSClient(SubprocessClient):
    """A client for handling ZFS and filesystem commands."""
    def mkdir(self, path: Path):
        log_message(f"Creating directory: {path}", self.status_dict)
        path.mkdir(parents=True, exist_ok=True)

    def rmdir(self, path: Path):
        log_message(f"Removing directory: {path}", self.status_dict)
        path.rmdir()
        
    def snapshot(self, dataset: str, snap_name: str):
        self._run(['zfs', 'snapshot', '-r', f'{dataset}@{snap_name}'])

    def destroy(self, full_snapshot_name: str):
        self._run(['zfs', 'destroy', '-r', full_snapshot_name])

    def mount(self, full_snapshot_name: str, mount_point: Path):
        self._run(['mount', '-t', 'zfs', full_snapshot_name, str(mount_point)])

    def unmount(self, mount_point: Path):
        self._run(['umount', str(mount_point)])

class ResticClient(SubprocessClient):
    """A client for handling Restic commands."""
    def list_snapshots(self) -> List[Dict[str, Any]]:
        result = self._run(['restic', 'snapshots', '--json'], log_command=False)
        return json.loads(result.stdout)

    def find_parent_snapshot_id(self, tag: str) -> Optional[str]:
        try:
            result = self._run(['restic', 'snapshots', '--tag', tag, '--json'], log_output=False)
            all_snapshots = json.loads(result.stdout)
            if all_snapshots:
                all_snapshots.sort(key=lambda s: datetime.fromisoformat(s['time']))
                return all_snapshots[-1]['short_id']
        except Exception as e:
            log_message(f"Could not find parent snapshot for tag '{tag}'. Error: {e}", self.status_dict)
        return None

    def backup(self, path: Path, tags: List[str], parent_id: Optional[str]):
        cmd = ['restic', 'backup']
        for tag in tags: cmd.extend(['--tag', tag])
        if parent_id:
            cmd.extend(['--parent', parent_id])
            log_message(f"Using parent snapshot {parent_id} for this backup.", self.status_dict)
        cmd.append(str(path))
        self._run(cmd)

    def forget(self, retention_args: List[str]):
        cmd = ['restic', 'forget', '--prune', '--group-by', 'paths'] + retention_args
        self._run(cmd)

class NotificationClient:
    """A client for sending notifications."""
    def __init__(self, status_dict: Optional[Dict[str, Any]] = None):
        self.status_dict = status_dict

    def send(self, title: str, message: str, priority: int = 5):
        gotify_url = os.environ.get('GOTIFY_URL')
        gotify_token = os.environ.get('GOTIFY_TOKEN')
        if not gotify_url or not gotify_token: return
        
        log_message(f"Sending Gotify notification: {title}", self.status_dict)
        try:
            requests.post(f"{gotify_url}?token={gotify_token}", json={"title": title, "message": message, "priority": priority}).raise_for_status()
        except Exception as e:
            log_message(f"!!! Could not send Gotify notification. Error: {e}", self.status_dict)

# --- Main Backup Manager Class ---

class BackupManager:
    """Orchestrates the backup process, managing state and clients."""
    def __init__(self):
        self.status = {
            "live_status": "idle", "current_task": "N/A", "last_run_start_time": None,
            "last_completed_run": {"outcome": "N/A", "finish_time": None, "details": "No backup has completed yet."},
            "log": []
        }
        self.lock = Lock()
        self.zfs = ZFSClient(self.status)
        self.restic = ResticClient(self.status)
        self.notifier = NotificationClient(self.status)

    def get_status(self) -> Dict[str, Any]:
        """Returns the current status dictionary."""
        return self.status

    def start_backup_job(self) -> bool:
        """Tries to acquire a lock and start a backup job. Returns True on success."""
        if self.lock.acquire(blocking=False):
            print("Acquired backup lock, starting backup thread.")
            thread = Thread(target=self._perform_backup_thread)
            thread.start()
            return True
        else:
            print("Could not acquire lock, backup already in progress.")
            return False
        
    def get_current_snapshots(self) -> List[Dict[str, Any]]:
        """Returns a list of current snapshots."""
        return self.restic.list_snapshots()

    def _perform_backup_thread(self):
        """The internal backup logic, not to be called directly."""
        self.status.update({
            "live_status": "running", "current_task": "Reading config",
            "last_run_start_time": datetime.now().isoformat(), "log": []
        })

        try:
            with open(Path('/config/config.yml'), 'r') as f: config = yaml.safe_load(f)

            for dataset in config.get('datasets', []):
                self.status["current_task"] = f"Processing dataset: {dataset}"
                snapshot_name = f"restic-backup-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
                full_snapshot_name = f'{dataset}@{snapshot_name}'
                
                safe_dataset_name = dataset.replace('/', '_')
                temp_mount_point = Path(f"/mnt/restic_backup_{safe_dataset_name}")
                
                self.zfs.mkdir(temp_mount_point)
                
                try:
                    self.zfs.snapshot(dataset, snapshot_name)
                    self.zfs.mount(full_snapshot_name, temp_mount_point)
                    parent_id = self.restic.find_parent_snapshot_id(dataset)
                    self.restic.backup(path=temp_mount_point, tags=[dataset, full_snapshot_name], parent_id=parent_id)
                finally:
                    self.status["current_task"] = f"Cleaning up: {dataset}"
                    try: self.zfs.unmount(temp_mount_point)
                    except Exception as e: log_message(f"Cleanup warning: {e}", self.status)
                    try: self.zfs.destroy(full_snapshot_name)
                    except Exception as e: log_message(f"Cleanup warning: {e}", self.status)
                    try: self.zfs.rmdir(temp_mount_point)
                    except Exception as e: log_message(f"Cleanup warning: {e}", self.status)

            self.status["current_task"] = "Pruning old backups"
            retention_policy = config.get('retention', {})
            if retention_policy:
                retention_args = [arg for k, v in retention_policy.items() for arg in [f'--{k}', str(v)]]
                self.restic.forget(retention_args)
            
            success_details = "Backup and prune completed successfully."
            self.status["last_completed_run"] = {"outcome": "success", "finish_time": datetime.now().isoformat(), "details": success_details}
            self.notifier.send("✅ Backup Success", success_details, priority=3)

        except Exception as e:
            error_message = f"Failed: {e}"
            log_message(error_message, self.status)
            self.status["last_completed_run"] = {"outcome": "failure", "finish_time": datetime.now().isoformat(), "details": error_message}
            self.notifier.send("❌ Backup FAILED", error_message, priority=8)
        finally:
            self.status.update({"live_status": "idle", "current_task": "N/A"})
            self.lock.release()
            log_message("Backup lock released.", self.status)
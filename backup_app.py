import os
import yaml
import subprocess
import json
from datetime import datetime
from flask import Flask, jsonify
from threading import Thread, Lock

# --- Globals for Status and Locking ---
app = Flask(__name__)
backup_status = {
    "live_status": "idle", # Can be "idle", "running"
    "current_task": "N/A",
    "last_run_start_time": None,
    "last_completed_run": {
        "outcome": "N/A", # "success" or "failure"
        "finish_time": None,
        "details": "No backup has completed yet."
    },
    "log": []
}
backup_lock = Lock()

# --- Configuration ---
CONFIG_PATH = '/config/config.yml'
TEMP_MOUNT_POINT = '/mnt/restic_backup_mount'

def run_command(command, shell=False):
    """Runs a shell command, logs output, and raises an exception on error."""
    log_line = f"Executing: {command if shell else ' '.join(command)}"
    print(log_line)
    backup_status["log"].append(log_line)
    
    process_env = os.environ.copy()
    result = subprocess.run(command, capture_output=True, text=True, env=process_env, shell=shell)
    
    if result.stdout:
        print(result.stdout)
        backup_status["log"].append(result.stdout)
    if result.stderr:
        print(result.stderr)
        backup_status["log"].append(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {command}")
    return result

def perform_backup_thread():
    """The main backup logic that runs in a background thread."""
    global backup_status
    
    backup_status.update({
        "live_status": "running",
        "current_task": "Reading config",
        "last_run_start_time": datetime.now().isoformat(),
        "log": []
    })

    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)

        for dataset in config.get('datasets', []):
            snapshot_name = f"restic-backup-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
            full_snapshot_name = f'{dataset}@{snapshot_name}'
            backup_status["current_task"] = f"Processing dataset: {dataset}"
            
            try:
                run_command(['mkdir', '-p', TEMP_MOUNT_POINT])
                run_command(['zfs', 'snapshot', '-r', full_snapshot_name])
                run_command(['mount', '-t', 'zfs', full_snapshot_name, TEMP_MOUNT_POINT])
                run_command(['restic', 'backup', '--tag', dataset, '--tag', full_snapshot_name, TEMP_MOUNT_POINT])
            finally:
                backup_status["current_task"] = f"Cleaning up: {dataset}"
                try: run_command(['umount', TEMP_MOUNT_POINT])
                except Exception as e: print(f"Cleanup warning: Failed to unmount: {e}")
                try: run_command(['zfs', 'destroy', '-r', full_snapshot_name])
                except Exception as e: print(f"Cleanup warning: Failed to destroy snapshot: {e}")
                try: run_command(['rmdir', TEMP_MOUNT_POINT])
                except Exception as e: print(f"Cleanup warning: Failed to remove temp directory: {e}")

        backup_status["current_task"] = "Pruning old backups"
        retention_args = []
        for key, value in config.get('retention', {}).items():
            retention_args.append(f'--{key}')
            retention_args.append(str(value))
        if retention_args:
            run_command(['restic', 'forget', '--prune', '--group-by', 'tags'] + retention_args)

        backup_status["last_completed_run"] = {
            "outcome": "success",
            "finish_time": datetime.now().isoformat(),
            "details": "Backup and prune completed successfully."
        }

    except Exception as e:
        error_message = f"Failed: {e}"
        print(f"!!! A CRITICAL ERROR OCCURRED: {error_message}")
        backup_status["last_completed_run"] = {
            "outcome": "failure",
            "finish_time": datetime.now().isoformat(),
            "details": error_message
        }
    finally:
        backup_status["live_status"] = "idle"
        backup_status["current_task"] = "N/A"
        backup_lock.release()
        print("Backup lock released.")

# --- API Endpoints ---

@app.route('/backup', methods=['POST'])
def backup_endpoint():
    """Endpoint to trigger the backup process."""
    if backup_lock.acquire(blocking=False):
        print("Acquired backup lock, starting backup thread.")
        thread = Thread(target=perform_backup_thread)
        thread.start()
        return jsonify({"status": "success", "message": "Backup process started in the background."}), 202
    else:
        print("Could not acquire lock, backup already in progress.")
        return jsonify({"status": "error", "message": "Backup already in progress."}), 409

@app.route('/status', methods=['GET'])
def status_endpoint():
    """Endpoint to get the current status of the backup process."""
    return jsonify(backup_status)

@app.route('/snapshots', methods=['GET'])
def snapshots_endpoint():
    """Endpoint to list all snapshots in the repository."""
    try:
        process_env = os.environ.copy()
        result = subprocess.run(
            ['restic', 'snapshots', '--json'], 
            capture_output=True, text=True, env=process_env, check=True
        )
        snapshots = json.loads(result.stdout)
        return jsonify(snapshots)
    except subprocess.CalledProcessError as e:
        return jsonify({"status": "error", "message": "Failed to list snapshots.", "details": e.stderr}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": "An unexpected error occurred.", "details": str(e)}), 500


if __name__ == '__main__':
    print("Starting backup web server with QoL improvements...")
    app.run(host='0.0.0.0', port=8000)
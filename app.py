import subprocess
from flask import Flask, jsonify
from backup import BackupManager

# --- Create a single instance of the BackupManager ---
app = Flask(__name__)
backup_manager = BackupManager()

# --- API Endpoints ---
@app.route('/backup', methods=['POST'])
def backup_endpoint():
    """Endpoint to trigger the backup process."""
    if backup_manager.start_backup_job():
        return jsonify({"status": "success", "message": "Backup process started in the background."}), 202
    else:
        return jsonify({"status": "error", "message": "Backup already in progress."}), 409

@app.route('/status', methods=['GET'])
def status_endpoint():
    """Endpoint to get the current status of the backup process."""
    return jsonify(backup_manager.get_status())

@app.route('/snapshots', methods=['GET'])
def snapshots_endpoint():
    """Endpoint to list all snapshots in the repository."""
    try:
        snapshots = backup_manager.get_current_snapshots()
        return jsonify(snapshots)
    except subprocess.CalledProcessError as e:
        return jsonify({"status": "error", "message": "Failed to list snapshots.", "details": e.stderr}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": "An unexpected error occurred.", "details": str(e)}), 500

if __name__ == '__main__':
    print("Starting backup web server...")
    app.run(host='0.0.0.0', port=8000)
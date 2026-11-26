"""SQLite persistence for run history. One connection per call; a lock serialises writes."""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,              -- backup | check
    trigger     TEXT NOT NULL,              -- api | ui
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    outcome     TEXT NOT NULL,              -- running | success | partial | failure | interrupted
    details     TEXT,
    log         TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS dataset_results (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id           INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    dataset          TEXT NOT NULL,
    outcome          TEXT NOT NULL,         -- success | partial | failure
    started_at       TEXT,
    finished_at      TEXT,
    snapshot_id      TEXT,
    files_new        INTEGER,
    files_changed    INTEGER,
    files_unmodified INTEGER,
    data_added       INTEGER,
    total_bytes      INTEGER,
    error            TEXT
);
CREATE INDEX IF NOT EXISTS idx_results_run ON dataset_results(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);
"""

RUN_COLUMNS = "id, kind, trigger, started_at, finished_at, outcome, details"


class Database:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._conn() as c:
            c.executescript(SCHEMA)

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _write(self, sql: str, params: tuple = ()) -> int:
        with self._lock, self._conn() as c:
            return c.execute(sql, params).lastrowid

    # --- runs ---
    def create_run(self, kind: str, trigger: str, started_at: str) -> int:
        return self._write(
            "INSERT INTO runs (kind, trigger, started_at, outcome) VALUES (?, ?, ?, 'running')",
            (kind, trigger, started_at),
        )

    def append_log(self, run_id: int, text: str):
        self._write("UPDATE runs SET log = log || ? WHERE id = ?", (text, run_id))

    def finish_run(self, run_id: int, outcome: str, details: str, finished_at: str):
        self._write(
            "UPDATE runs SET outcome = ?, details = ?, finished_at = ? WHERE id = ?",
            (outcome, details, finished_at, run_id),
        )

    def mark_interrupted(self, finished_at: str) -> int:
        with self._lock, self._conn() as c:
            return c.execute(
                "UPDATE runs SET outcome = 'interrupted', finished_at = ?, "
                "details = 'Process stopped while this run was in progress (container restart or crash).' "
                "WHERE outcome = 'running'",
                (finished_at,),
            ).rowcount

    def trim(self, keep: int):
        self._write("DELETE FROM runs WHERE id NOT IN (SELECT id FROM runs ORDER BY id DESC LIMIT ?)", (keep,))

    def add_dataset_result(self, run_id: int, r: Dict[str, Any]):
        self._write(
            "INSERT INTO dataset_results (run_id, dataset, outcome, started_at, finished_at, snapshot_id, "
            "files_new, files_changed, files_unmodified, data_added, total_bytes, error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, r["dataset"], r["outcome"], r.get("started_at"), r.get("finished_at"), r.get("snapshot_id"),
             r.get("files_new"), r.get("files_changed"), r.get("files_unmodified"), r.get("data_added"),
             r.get("total_bytes"), r.get("error")),
        )

    def list_runs(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        with self._conn() as c:
            runs = [dict(r) for r in c.execute(
                f"SELECT {RUN_COLUMNS} FROM runs ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset))]
            for run in runs:
                agg = c.execute(
                    "SELECT COUNT(*) AS n, SUM(data_added) AS added, "
                    "SUM(outcome = 'failure') AS failed FROM dataset_results WHERE run_id = ?", (run["id"],)
                ).fetchone()
                run.update(datasets=agg["n"], data_added=agg["added"], datasets_failed=agg["failed"] or 0)
            return runs

    def get_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        with self._conn() as c:
            row = c.execute(f"SELECT {RUN_COLUMNS}, log FROM runs WHERE id = ?", (run_id,)).fetchone()
            if not row:
                return None
            run = dict(row)
            run["results"] = [dict(r) for r in c.execute(
                "SELECT * FROM dataset_results WHERE run_id = ? ORDER BY id", (run_id,))]
            return run

    def last_finished_run(self, kind: str = "backup") -> Optional[Dict[str, Any]]:
        with self._conn() as c:
            row = c.execute(
                f"SELECT {RUN_COLUMNS} FROM runs WHERE kind = ? AND outcome != 'running' ORDER BY id DESC LIMIT 1",
                (kind,),
            ).fetchone()
            return dict(row) if row else None

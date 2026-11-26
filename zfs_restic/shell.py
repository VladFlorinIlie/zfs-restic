"""Helpers for running external commands."""
from __future__ import annotations

import os
import subprocess
from typing import Callable, Dict, List, Optional, Tuple

Logger = Callable[[str], None]


class CommandError(Exception):
    def __init__(self, cmd: List[str], returncode: int, output: str = ""):
        self.cmd, self.returncode, self.output = cmd, returncode, output
        tail = output.strip().splitlines()[-3:]
        super().__init__(f"`{' '.join(cmd)}` exited with {returncode}" + (f": {' | '.join(tail)}" if tail else ""))


def stream(cmd: List[str], log: Logger, quiet: bool = False,
           env: Optional[Dict[str, str]] = None) -> Tuple[int, str]:
    """Run a command, streaming merged stdout/stderr line by line into the log."""
    log(f"$ {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            env=env or os.environ.copy(), bufsize=1)
    lines = []
    assert proc.stdout is not None
    for line in proc.stdout:
        lines.append(line)
        if not quiet:
            log(line.rstrip())
    return proc.wait(), "".join(lines)


def run_checked(cmd: List[str], log: Logger, quiet: bool = False) -> str:
    """Like stream(), but raises CommandError on a non-zero exit (logging the output if it was suppressed)."""
    rc, out = stream(cmd, log, quiet=quiet)
    if rc != 0:
        if quiet and out.strip():
            log(out.rstrip())
        raise CommandError(cmd, rc, out)
    return out


def capture(cmd: List[str], timeout: int = 300) -> str:
    """Run a command and return its stdout; raises CommandError on failure."""
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if res.returncode != 0:
        raise CommandError(cmd, res.returncode, res.stderr or res.stdout)
    return res.stdout

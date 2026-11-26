"""The user's backup config (config.yml). Format is unchanged from v1; this adds validation."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

_FLAG_RE = re.compile(r"^[a-z][a-z0-9-]*$")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class BackupConfig:
    datasets: List[str]
    retention: Dict[str, Any] = field(default_factory=dict)
    check_read_data_subset: Optional[str] = None

    def retention_args(self) -> List[str]:
        """`{keep-daily: 7}` -> `['--keep-daily', '7']`. Lists repeat the flag, `true` is a bare flag."""
        args: List[str] = []
        for key, value in self.retention.items():
            if value is True:
                args.append(f"--{key}")
            elif value is False or value is None:
                continue
            elif isinstance(value, list):
                for item in value:
                    args += [f"--{key}", str(item)]
            else:
                args += [f"--{key}", str(value)]
        return args


def parse_config(raw: Any) -> BackupConfig:
    raw = raw or {}
    if not isinstance(raw, dict):
        raise ConfigError("config must be a mapping with a 'datasets' list")

    datasets = raw.get("datasets") or []
    if not isinstance(datasets, list):
        raise ConfigError("'datasets' must be a list")
    clean: List[str] = []
    for ds in datasets:
        if not isinstance(ds, str) or not ds.strip():
            raise ConfigError(f"invalid dataset entry: {ds!r}")
        ds = ds.strip().rstrip("/")
        if "@" in ds or ds.startswith(("-", "/")):
            raise ConfigError(f"'{ds}' is not a dataset name (expected e.g. 'tank/photos')")
        if ds in clean:
            raise ConfigError(f"dataset '{ds}' is listed twice")
        clean.append(ds)

    retention = raw.get("retention") or {}
    if not isinstance(retention, dict):
        raise ConfigError("'retention' must be a mapping, e.g. {keep-daily: 7}")
    for key in retention:
        if not isinstance(key, str) or not _FLAG_RE.match(key):
            raise ConfigError(f"invalid retention option {key!r} (expected restic flag names like 'keep-daily')")

    check = raw.get("check") or {}
    if not isinstance(check, dict):
        raise ConfigError("'check' must be a mapping")
    subset = check.get("read_data_subset")

    return BackupConfig(datasets=clean, retention=dict(retention),
                        check_read_data_subset=str(subset) if subset not in (None, "") else None)


def load_config(path: Path) -> BackupConfig:
    try:
        with open(path) as f:
            raw = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigError(f"config file not found: {path}") from None
    except yaml.YAMLError as e:
        raise ConfigError(f"could not parse {path}: {e}") from None
    return parse_config(raw)

import pytest
import yaml

from zfs_restic.config import ConfigError, load_config, parse_config


def v1_retention_args(retention):
    """Exactly how v1 built the forget arguments."""
    return [arg for k, v in retention.items() for arg in [f"--{k}", str(v)]]


def test_v1_config_is_accepted_unchanged(tmp_path):
    raw = {"datasets": ["tank/photos", "tank/docs"],
           "retention": {"keep-daily": 7, "keep-weekly": 4, "keep-monthly": 12, "keep-within": "30d"}}
    p = tmp_path / "config.yml"
    p.write_text(yaml.safe_dump(raw, sort_keys=False))
    cfg = load_config(p)
    assert cfg.datasets == raw["datasets"]
    assert cfg.retention_args() == v1_retention_args(raw["retention"])
    assert cfg.check_read_data_subset is None


def test_empty_and_missing_sections():
    assert parse_config(None).datasets == []
    assert parse_config({"datasets": ["a"]}).retention_args() == []


def test_lists_and_flags_in_retention():
    cfg = parse_config({"datasets": ["a"], "retention": {"keep-tag": ["x", "y"], "keep-daily": 3, "compact": False}})
    assert cfg.retention_args() == ["--keep-tag", "x", "--keep-tag", "y", "--keep-daily", "3"]


def test_check_subset():
    assert parse_config({"datasets": ["a"], "check": {"read_data_subset": "2%"}}).check_read_data_subset == "2%"


@pytest.mark.parametrize("raw", [
    {"datasets": "tank"},
    {"datasets": ["tank@snap"]},
    {"datasets": ["/mnt/tank"]},
    {"datasets": ["a", "a"]},
    {"datasets": [""]},
    {"datasets": ["a"], "retention": ["keep-daily"]},
    {"datasets": ["a"], "retention": {"--keep-daily": 1}},
    ["tank"],
])
def test_invalid_configs(raw):
    with pytest.raises(ConfigError):
        parse_config(raw)


def test_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yml")

from datetime import timedelta
from pathlib import Path

import pytest

from transforms.config.settings import (
    DuckTransformsSettings,
    _load_config_file,
    _parse_global_config,
    get_settings,
    parse_duration,
    render_source_query,
)


def test_parse_duration():
    assert parse_duration("90m") == timedelta(minutes=90)
    assert parse_duration("7d") == timedelta(days=7)
    assert parse_duration("2h") == timedelta(hours=2)
    assert parse_duration(None) is None


def test_parse_duration_invalid():
    with pytest.raises(ValueError):
        parse_duration("seven-days")


def test_for_dataset_global_defaults():
    settings = DuckTransformsSettings(
        allowed_stale_time=timedelta(days=7),
        source_query="SELECT * FROM `{dataset_rid}`",
        datasets={},
    )
    resolved = settings.for_dataset("ri.foundry.main.dataset.abc")
    assert resolved.allowed_stale_time == timedelta(days=7)
    assert resolved.source_query == "SELECT * FROM `{dataset_rid}`"


def test_for_dataset_override_by_rid():
    settings = DuckTransformsSettings(
        allowed_stale_time=timedelta(days=7),
        datasets={
            "ri.foundry.main.dataset.abc": {
                "allowed_stale_time": "30d",
                "source_query": "SELECT a FROM `{dataset_rid}`",
            }
        },
    )
    resolved = settings.for_dataset("ri.foundry.main.dataset.abc")
    assert resolved.allowed_stale_time == timedelta(days=30)
    assert resolved.source_query == "SELECT a FROM `{dataset_rid}`"


def test_for_dataset_override_by_dataset_path():
    path = "/project/my_dataset"
    settings = DuckTransformsSettings(
        datasets={path: {"allowed_stale_time": "14d"}},
    )
    resolved = settings.for_dataset(
        "ri.foundry.main.dataset.abc",
        dataset_path=path,
    )
    assert resolved.allowed_stale_time == timedelta(days=14)


def test_render_source_query():
    query = render_source_query(
        "SELECT a FROM `{dataset_rid}` WHERE branch = '{branch}'",
        dataset_rid="ri.foundry.main.dataset.abc",
        dataset_path="/project/my_dataset",
        branch="master",
    )
    assert query == "SELECT a FROM `ri.foundry.main.dataset.abc` WHERE branch = 'master'"


def test_load_and_merge_config_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    base = tmp_path / "base.toml"
    override = tmp_path / "override.toml"
    base.write_text(
        """
[config]
allowed_stale_time = "7d"

[datasets."ri.foundry.main.dataset.abc"]
allowed_stale_time = "14d"
""".strip()
    )
    override.write_text(
        """
[datasets."ri.foundry.main.dataset.abc"]
allowed_stale_time = "30d"
source_query = "SELECT x FROM `{dataset_rid}`"
""".strip()
    )

    merged: dict = {}
    from foundry_dev_tools.utils.config import merge_dicts

    for file in (base, override):
        merged = merge_dicts(merged, _load_config_file(file))

    settings = _parse_global_config(merged)
    resolved = settings.for_dataset("ri.foundry.main.dataset.abc")
    assert resolved.allowed_stale_time == timedelta(days=30)
    assert resolved.source_query == "SELECT x FROM `{dataset_rid}`"


def test_get_settings_reload(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    config_file = tmp_path / "config_foundry_duck_transforms.toml"
    config_file.write_text(
        """
[config]
allowed_stale_time = "1d"
""".strip()
    )
    monkeypatch.setattr(
        "transforms.config.settings.cfg_files",
        lambda use_project_config=True: [config_file],
    )
    settings = get_settings(reload=True)
    assert settings.allowed_stale_time == timedelta(days=1)

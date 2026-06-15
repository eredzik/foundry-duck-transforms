from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from datetime import timedelta
from functools import cache
from pathlib import Path
from typing import Any

import platformdirs
from foundry_dev_tools.utils.config import find_project_config_file, merge_dicts

if sys.version_info < (3, 11):
    import tomli as tomllib
else:
    import tomllib

CFG_FILE_NAME = "config_foundry_duck_transforms.toml"
_DURATION_PATTERN = re.compile(
    r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>[smhd])$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DatasetSettings:
    allowed_stale_time: timedelta | None = None
    source_query: str | None = None


@dataclass
class DuckTransformsSettings:
    allowed_stale_time: timedelta | None = None
    source_query: str | None = None
    datasets: dict[str, dict[str, Any]] = field(default_factory=dict)

    def for_dataset(
        self,
        path_or_rid: str,
        *,
        dataset_path: str | None = None,
    ) -> DatasetSettings:
        overrides = _resolve_dataset_overrides(
            self.datasets,
            path_or_rid=path_or_rid,
            dataset_path=dataset_path,
        )
        stale_raw = overrides.get("allowed_stale_time", self.allowed_stale_time)
        query_raw = overrides.get("source_query", self.source_query)
        return DatasetSettings(
            allowed_stale_time=_coerce_duration(stale_raw),
            source_query=_coerce_optional_str(query_raw),
        )


@cache
def _platformdirs() -> platformdirs.PlatformDirsABC:
    return platformdirs.PlatformDirs("foundry-dev-tools")


def cfg_files(use_project_config: bool = True) -> list[Path]:
    files = [
        _platformdirs().site_config_path.joinpath(CFG_FILE_NAME),
        Path.home().joinpath(".foundry-dev-tools", CFG_FILE_NAME),
        Path.home().joinpath(".config", "foundry-dev-tools", CFG_FILE_NAME),
        _platformdirs().user_config_path.joinpath(CFG_FILE_NAME),
    ]
    if use_project_config:
        if project_cfg := find_project_config_file():
            files.append(project_cfg.parent.joinpath(CFG_FILE_NAME))
        cwd_config = Path.cwd().joinpath(CFG_FILE_NAME)
        if cwd_config.exists() and cwd_config not in files:
            files.append(cwd_config)
    return files


def _load_config_file(config_file: Path) -> dict[str, Any]:
    try:
        with config_file.open("rb") as config_file_fd:
            loaded = tomllib.load(config_file_fd)
            return loaded if isinstance(loaded, dict) else {}
    except OSError:
        return {}


def _load_merged_config() -> dict[str, Any]:
    config: dict[str, Any] = {}
    for cfg_file in cfg_files():
        if cfg_file.exists():
            config = merge_dicts(config, _load_config_file(cfg_file))
    return config


def parse_duration(value: str | timedelta | None) -> timedelta | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value
    if not isinstance(value, str):
        raise ValueError(f"Invalid duration value: {value!r}")
    stripped = value.strip()
    if not stripped:
        return None
    match = _DURATION_PATTERN.match(stripped)
    if not match:
        raise ValueError(
            f"Invalid duration {value!r}; expected forms like 90m, 24h, 7d"
        )
    amount = float(match.group("value"))
    unit = match.group("unit").lower()
    if unit == "s":
        return timedelta(seconds=amount)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    return timedelta(days=amount)


def _coerce_duration(value: Any) -> timedelta | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    if isinstance(value, str):
        return parse_duration(value)
    raise ValueError(f"Invalid allowed_stale_time value: {value!r}")


def _coerce_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip():
        return value
    raise ValueError(f"Invalid source_query value: {value!r}")


def _parse_global_config(raw: dict[str, Any]) -> DuckTransformsSettings:
    global_config = raw.get("config", {})
    if not isinstance(global_config, dict):
        global_config = {}
    datasets = raw.get("datasets", {})
    if not isinstance(datasets, dict):
        datasets = {}
    return DuckTransformsSettings(
        allowed_stale_time=_coerce_duration(global_config.get("allowed_stale_time")),
        source_query=_coerce_optional_str(global_config.get("source_query")),
        datasets=datasets,
    )


def _resolve_dataset_overrides(
    datasets: dict[str, dict[str, Any]],
    *,
    path_or_rid: str,
    dataset_path: str | None,
) -> dict[str, Any]:
    keys = [path_or_rid]
    if dataset_path and dataset_path not in keys:
        keys.append(dataset_path)
    for key in keys:
        entry = datasets.get(key)
        if isinstance(entry, dict):
            return entry
    return {}


def render_source_query(
    template: str,
    *,
    dataset_rid: str,
    dataset_path: str,
    branch: str,
) -> str:
    return (
        template.replace("{dataset_rid}", dataset_rid)
        .replace("{dataset_path}", dataset_path)
        .replace("{branch}", branch)
    )


_settings: DuckTransformsSettings | None = None


def get_settings(reload: bool = False) -> DuckTransformsSettings:
    global _settings
    if _settings is None or reload:
        _settings = _parse_global_config(_load_merged_config())
    return _settings


def get_dataset_settings(
    path_or_rid: str,
    *,
    dataset_path: str | None = None,
    reload: bool = False,
) -> DatasetSettings:
    return get_settings(reload=reload).for_dataset(
        path_or_rid,
        dataset_path=dataset_path,
    )

from datetime import datetime, timedelta, timezone

from transforms.config.settings import DatasetSettings
from transforms.runner.data_source.foundry_source import FoundrySource


def _identity(
    *,
    dataset_rid: str,
    dataset_path: str,
    transaction_rid: str,
    close_time: str,
) -> dict:
    return {
        "dataset_path": dataset_path,
        "dataset_rid": dataset_rid,
        "last_transaction_rid": transaction_rid,
        "last_transaction": {
            "rid": transaction_rid,
            "transaction": {
                "closeTime": close_time,
            },
        },
    }


class StubFoundrySource(FoundrySource):
    def __init__(self, *, online: dict, cached: dict | None):
        self._online = online
        self._cached = cached
        self.ctx = None  # type: ignore[assignment]
        self.session = None  # type: ignore[assignment]
        self.settings = None

    def _get_online_identity(self, dataset_path_or_rid: str, branch: str) -> dict:
        return self._online

    def _get_cached_identity_if_on_disk(self, dataset_path_or_rid: str) -> dict | None:
        return self._cached


def test_resolve_dataset_identity_uses_online_when_no_stale_setting():
    online = _identity(
        dataset_rid="ri.foundry.main.dataset.abc",
        dataset_path="/project/ds",
        transaction_rid="ri.foundry.main.transaction.online",
        close_time="2026-03-10T13:00:00Z",
    )
    cached = _identity(
        dataset_rid="ri.foundry.main.dataset.abc",
        dataset_path="/project/ds",
        transaction_rid="ri.foundry.main.transaction.cached",
        close_time="2026-03-01T13:00:00Z",
    )
    source = StubFoundrySource(online=online, cached=cached)
    resolved = source.resolve_dataset_identity(
        "ri.foundry.main.dataset.abc",
        "master",
        DatasetSettings(),
    )
    assert resolved["last_transaction_rid"] == online["last_transaction_rid"]


def test_resolve_dataset_identity_pins_cached_within_stale_window():
    online = _identity(
        dataset_rid="ri.foundry.main.dataset.abc",
        dataset_path="/project/ds",
        transaction_rid="ri.foundry.main.transaction.online",
        close_time=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )
    cached = _identity(
        dataset_rid="ri.foundry.main.dataset.abc",
        dataset_path="/project/ds",
        transaction_rid="ri.foundry.main.transaction.cached",
        close_time=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )
    source = StubFoundrySource(online=online, cached=cached)
    resolved = source.resolve_dataset_identity(
        "ri.foundry.main.dataset.abc",
        "master",
        DatasetSettings(allowed_stale_time=timedelta(days=30)),
    )
    assert resolved["last_transaction_rid"] == cached["last_transaction_rid"]


def test_resolve_dataset_identity_refreshes_when_stale_window_expired():
    online = _identity(
        dataset_rid="ri.foundry.main.dataset.abc",
        dataset_path="/project/ds",
        transaction_rid="ri.foundry.main.transaction.online",
        close_time=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )
    cached = _identity(
        dataset_rid="ri.foundry.main.dataset.abc",
        dataset_path="/project/ds",
        transaction_rid="ri.foundry.main.transaction.cached",
        close_time="2020-01-01T00:00:00Z",
    )
    source = StubFoundrySource(online=online, cached=cached)
    resolved = source.resolve_dataset_identity(
        "ri.foundry.main.dataset.abc",
        "master",
        DatasetSettings(allowed_stale_time=timedelta(days=7)),
    )
    assert resolved["last_transaction_rid"] == online["last_transaction_rid"]

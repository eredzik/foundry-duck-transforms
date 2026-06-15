from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from asyncer import asyncify
from foundry_dev_tools import FoundryContext
from foundry_dev_tools.errors.dataset import BranchNotFoundError
from foundry_dev_tools.utils.caches.spark_caches import (
    _infer_dataset_format,
    _validate_cache_key,
    get_dataset_path,
)
from pyspark.sql import DataFrame, SparkSession

from transforms.config.settings import (
    DatasetSettings,
    DuckTransformsSettings,
    get_settings,
    render_source_query,
)
from transforms.runner.data_source.base import (
    BranchNotFoundError as BranchNotFoundErrorBase,
)
from transforms.runner.data_source.base import DataSource
from transforms.runner.data_source.download_result import DownloadMetadata, DownloadResult
from transforms.runner.dataset_logging import (
    dataset_display_name,
    log_dataset_phase,
    log_verbose_step,
)

logger = logging.getLogger(__name__)

QUERY_CACHE_DIR_NAME = "foundry-duck-queries"


@dataclass
class FoundrySource(DataSource):
    ctx: FoundryContext
    session: SparkSession
    settings: DuckTransformsSettings | None = None
    verbose: bool = False
    _spark_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __post_init__(self) -> None:
        if self.settings is None:
            self.settings = get_settings()

    def resolve_dataset_label(self, dataset_path_or_rid: str, branch: str = "master") -> str:
        try:
            identity = self.ctx.cached_foundry_client._get_dataset_identity(
                dataset_path_or_rid, branch
            )
            return dataset_display_name(
                dataset_path_or_rid, identity.get("dataset_path")
            )
        except Exception:
            return dataset_display_name(dataset_path_or_rid)

    def _dataset_settings(
        self,
        path_or_rid: str,
        dataset_path: str | None,
    ) -> DatasetSettings:
        assert self.settings is not None
        return self.settings.for_dataset(path_or_rid, dataset_path=dataset_path)

    def _get_online_identity(self, dataset_path_or_rid: str, branch: str) -> dict[str, Any]:
        client = self.ctx.cached_foundry_client
        if client.api.ctx.config.transforms_freeze_cache is False:
            return client._get_dataset_identity_online(dataset_path_or_rid, branch)
        return client._get_dataset_identity(dataset_path_or_rid, branch)

    def _get_cached_identity_if_on_disk(
        self, dataset_path_or_rid: str
    ) -> dict[str, Any] | None:
        cache = self.ctx.cached_foundry_client.cache
        try:
            cached = cache.get_dataset_identity_not_branch_aware(dataset_path_or_rid)
        except KeyError:
            return None
        cache_dir = cache.get_cache_dir()
        try:
            local_path = get_dataset_path(cache_dir, cached)
            if local_path.exists():
                return cached
        except Exception:
            logger.debug(
                "Cached dataset path missing for %s",
                dataset_path_or_rid,
                exc_info=True,
            )
        return None

    @staticmethod
    def _transaction_close_time(identity: dict[str, Any]) -> datetime | None:
        last_transaction = identity.get("last_transaction") or {}
        transaction = last_transaction.get("transaction") or {}
        close_time = transaction.get("closeTime") or transaction.get("startTime")
        if not close_time:
            return None
        normalized = close_time.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)

    def resolve_dataset_identity(
        self,
        dataset_path_or_rid: str,
        branch: str,
        dataset_settings: DatasetSettings,
    ) -> dict[str, Any]:
        online = self._get_online_identity(dataset_path_or_rid, branch)
        if dataset_settings.allowed_stale_time is None:
            return online

        cached = self._get_cached_identity_if_on_disk(dataset_path_or_rid)
        if cached is None:
            return online

        if cached.get("last_transaction_rid") == online.get("last_transaction_rid"):
            return online

        close_time = self._transaction_close_time(cached)
        if close_time is None:
            return online

        age = datetime.now(timezone.utc) - close_time
        if age <= dataset_settings.allowed_stale_time:
            label = dataset_display_name(
                dataset_path_or_rid, cached.get("dataset_path")
            )
            logger.info(
                "Using pinned stale cache for %s (cached_tx=%s, online_tx=%s, "
                "cached_tx_close=%s, allowed_stale_time=%s)",
                label,
                cached.get("last_transaction_rid"),
                online.get("last_transaction_rid"),
                close_time.isoformat(),
                dataset_settings.allowed_stale_time,
            )
            return cached

        label = dataset_display_name(dataset_path_or_rid, online.get("dataset_path"))
        logger.info(
            "Stale window expired for %s (cached_tx_close=%s, allowed_stale_time=%s); "
            "refreshing dataset",
            label,
            close_time.isoformat(),
            dataset_settings.allowed_stale_time,
        )
        return online

    def _fetch_dataset_files(
        self,
        dataset_path_or_rid: str,
        branch: str,
        identity: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        client = self.ctx.cached_foundry_client
        last_path = os.fspath(client._fetch_dataset(identity, branch=branch))
        return last_path, identity

    def _query_cache_dir(self, cache_key: str) -> Path:
        cache_dir = self.ctx.cached_foundry_client.cache.get_cache_dir()
        return cache_dir.joinpath(QUERY_CACHE_DIR_NAME, cache_key)

    @staticmethod
    def _query_cache_key(*, branch: str, dataset_rid: str, query: str) -> str:
        digest = hashlib.sha256(f"{branch}:{dataset_rid}:{query}".encode()).hexdigest()
        return digest[:16]

    def _load_query_cache(
        self,
        cache_dir: Path,
        allowed_stale_time: timedelta | None,
    ) -> DataFrame | None:
        metadata_path = cache_dir.joinpath("metadata.json")
        parquet_path = cache_dir.joinpath("data.parquet")
        if not parquet_path.exists():
            return None
        if allowed_stale_time is None:
            return None
        if metadata_path.exists():
            with metadata_path.open(encoding="utf-8") as handle:
                metadata = json.load(handle)
            cached_at = datetime.fromisoformat(metadata["cached_at"])
        else:
            cached_at = datetime.fromtimestamp(
                parquet_path.stat().st_mtime,
                tz=timezone.utc,
            )
        age = datetime.now(timezone.utc) - cached_at
        if age > allowed_stale_time:
            return None
        with self._spark_lock:
            return self.session.read.parquet(str(parquet_path))

    def _save_query_cache(self, cache_dir: Path, df: DataFrame) -> None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = cache_dir.joinpath("data.parquet")
        df.write.parquet(str(parquet_path), mode="overwrite")
        metadata = {"cached_at": datetime.now(timezone.utc).isoformat()}
        with cache_dir.joinpath("metadata.json").open("w", encoding="utf-8") as handle:
            json.dump(metadata, handle)

    def _load_via_source_query_sync(
        self,
        *,
        dataset_path_or_rid: str,
        branch: str,
        identity: dict[str, Any],
        dataset_settings: DatasetSettings,
        label: str,
    ) -> DownloadResult:
        assert dataset_settings.source_query is not None
        query = render_source_query(
            dataset_settings.source_query,
            dataset_rid=identity["dataset_rid"],
            dataset_path=identity["dataset_path"],
            branch=branch,
        )
        cache_key = self._query_cache_key(
            branch=branch,
            dataset_rid=identity["dataset_rid"],
            query=query,
        )
        cache_dir = self._query_cache_dir(cache_key)
        last_path = f"query-cache:{cache_key}"

        with log_dataset_phase(
            "SQL source read",
            label,
            log=logger,
            branch=branch,
            source="foundry",
            query=query,
        ) as phase:
            cached_df = self._load_query_cache(
                cache_dir,
                dataset_settings.allowed_stale_time,
            )
            if cached_df is not None:
                phase["cache"] = "query-cache"
                return DownloadResult(
                    df=cached_df,
                    metadata=DownloadMetadata(
                        dataset_path_or_rid=dataset_path_or_rid,
                        branch=branch,
                        last_path=last_path,
                        is_query_cache=True,
                    ),
                )

            df = self.ctx.cached_foundry_client.api.query_foundry_sql(
                query,
                branch=branch,
                return_type="spark",
            )
            self._save_query_cache(cache_dir, df)
            phase["cache"] = "miss"
            return DownloadResult(
                df=df,
                metadata=DownloadMetadata(
                    dataset_path_or_rid=dataset_path_or_rid,
                    branch=branch,
                    last_path=last_path,
                    is_query_cache=True,
                ),
            )

    def _load_dataframe_from_cache_path(
        self,
        identity: dict[str, Any],
        last_path: str,
    ) -> DataFrame:
        cache = self.ctx.cached_foundry_client.cache
        inferred_format = _infer_dataset_format(
            cache.get_cache_dir(),
            identity,
        )
        path = cache._get_storage_location(identity, inferred_format)
        with self._spark_lock:
            if inferred_format == "parquet":
                try:
                    return self.session.read.parquet(str(path.joinpath("*.parquet")))
                except Exception:
                    return self.session.read.parquet(
                        str(path.joinpath("**", "*.parquet"))
                    )
            if inferred_format == "csv":
                try:
                    return self.session.read.csv(str(path.joinpath("*.csv")))
                except Exception:
                    return self.session.read.csv(str(path.joinpath("**", "*.csv")))
        raise NotImplementedError(f"Format {inferred_format} is not supported")

    def _download_dataset_sync(
        self, dataset_path_or_rid: str, branch: str
    ) -> DownloadResult:
        try:
            with log_verbose_step(
                "online identity",
                verbose=self.verbose,
                log=logger,
                dataset=dataset_path_or_rid,
                branch=branch,
            ):
                online_identity = self._get_online_identity(dataset_path_or_rid, branch)
            if online_identity.get("last_transaction") is None:
                raise BranchNotFoundErrorBase("FOUNDRY")

            with log_verbose_step(
                "resolve identity",
                verbose=self.verbose,
                log=logger,
                dataset=dataset_path_or_rid,
                branch=branch,
            ):
                dataset_settings = self._dataset_settings(
                    dataset_path_or_rid,
                    online_identity.get("dataset_path"),
                )
                identity = self.resolve_dataset_identity(
                    dataset_path_or_rid,
                    branch,
                    dataset_settings,
                )
            label = dataset_display_name(
                dataset_path_or_rid, identity.get("dataset_path")
            )

            if dataset_settings.source_query:
                return self._load_via_source_query_sync(
                    dataset_path_or_rid=dataset_path_or_rid,
                    branch=branch,
                    identity=identity,
                    dataset_settings=dataset_settings,
                    label=label,
                )

            with log_verbose_step(
                "cache membership",
                verbose=self.verbose,
                log=logger,
                label=label,
            ):
                cached = identity in list(self.ctx.cached_foundry_client.cache.keys())
            file_count: int | str = "unknown"
            if not cached:
                with log_verbose_step(
                    "list files",
                    verbose=self.verbose,
                    log=logger,
                    label=label,
                ):
                    try:
                        files = self.ctx.cached_foundry_client.api.list_dataset_files(
                            identity["dataset_rid"],
                            exclude_hidden_files=True,
                            view=branch,
                        )
                        file_count = len(files)
                    except Exception:
                        logger.debug(
                            "Could not list files before download for %s",
                            label,
                            exc_info=True,
                        )

            operation = "cache read" if cached else "download"
            with log_dataset_phase(
                operation,
                label,
                log=logger,
                branch=branch,
                source="foundry",
                files=file_count if not cached else None,
            ) as phase:
                with log_verbose_step(
                    "fetch dataset",
                    verbose=self.verbose,
                    log=logger,
                    label=label,
                ):
                    last_path, identity = self._fetch_dataset_files(
                        dataset_path_or_rid,
                        branch,
                        identity,
                    )
                phase["path"] = last_path
                _validate_cache_key(identity)
                with log_verbose_step(
                    "read parquet",
                    verbose=self.verbose,
                    log=logger,
                    label=label,
                ):
                    df = self._load_dataframe_from_cache_path(identity, last_path)
                phase["format"] = _infer_dataset_format(
                    self.ctx.cached_foundry_client.cache.get_cache_dir(),
                    identity,
                )
                return DownloadResult(
                    df=df,
                    metadata=DownloadMetadata(
                        dataset_path_or_rid=dataset_path_or_rid,
                        branch=branch,
                        last_path=last_path,
                    ),
                )

        except BranchNotFoundError:
            logger.info(
                "[FOUNDRY] Branch [%s] not found for dataset [%s]",
                branch,
                dataset_path_or_rid,
            )
            raise BranchNotFoundErrorBase("FOUNDRY")
        except FileNotFoundError as exc:
            raise KeyError(str(exc)) from exc

    async def download_dataset(
        self, dataset_path_or_rid: str, branch: str
    ) -> DownloadResult:
        return await asyncify(self._download_dataset_sync)(
            dataset_path_or_rid, branch
        )

    async def download_for_branches(
        self, dataset_path_or_rid: str, branches: list[str]
    ) -> DownloadResult:
        for branch in branches:
            return await self.download_dataset(dataset_path_or_rid, branch=branch)

        raise BranchNotFoundErrorBase("FOUNDRY")

    def get_last_transaction(
        self, dataset_path_or_rid: str, branches: list[str]
    ) -> DataFrame:
        raise NotImplementedError()

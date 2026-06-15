import logging
from dataclasses import dataclass

from foundry_dev_tools import FoundryContext
from foundry_dev_tools.errors.dataset import BranchNotFoundError
from foundry_dev_tools.utils.caches.spark_caches import (
    _infer_dataset_format,
    _validate_cache_key,
)
from pyspark.sql import DataFrame, SparkSession
from asyncer import asyncify
from transforms.runner.data_source.base import (
    BranchNotFoundError as BranchNotFoundErrorBase,
)
from transforms.runner.data_source.base import DataSource
from transforms.runner.dataset_logging import (
    dataset_display_name,
    log_dataset_phase,
    try_row_count,
)

logger = logging.getLogger(__name__)


@dataclass
class FoundrySource(DataSource):
    ctx: FoundryContext
    session: SparkSession

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

    async def download_dataset(self, dataset_path_or_rid: str, branch: str) -> DataFrame:
        try:
            dataset_identity = await asyncify(self.ctx.cached_foundry_client._get_dataset_identity)(
                dataset_path_or_rid, branch
            )
            if dataset_identity.get("last_transaction") is None:
                raise BranchNotFoundErrorBase("FOUNDRY")

            label = dataset_display_name(
                dataset_path_or_rid, dataset_identity.get("dataset_path")
            )
            cached = dataset_identity in list(
                self.ctx.cached_foundry_client.cache.keys()
            )
            file_count: int | str = "unknown"
            if not cached:
                try:
                    files = await asyncify(
                        self.ctx.cached_foundry_client.api.list_dataset_files
                    )(
                        dataset_identity["dataset_rid"],
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
                last_path, dataset_identity = await asyncify(
                    self.ctx.cached_foundry_client.fetch_dataset
                )(dataset_path_or_rid, branch)
                self.last_path = last_path
                phase["path"] = last_path
                _validate_cache_key(dataset_identity)
                try:
                    inferred_format = _infer_dataset_format(
                        self.ctx.cached_foundry_client.cache.get_cache_dir(),
                        dataset_identity,
                    )
                    path = self.ctx.cached_foundry_client.cache._get_storage_location(
                        dataset_identity, inferred_format
                    )
                    phase["format"] = inferred_format
                    if inferred_format == "parquet":
                        try:
                            df = self.session.read.parquet(
                                str(path.joinpath("*.parquet"))
                            )
                        except Exception:
                            # Exception for partitioned parquet files - mostly for duckdb compat
                            df = self.session.read.parquet(
                                str(path.joinpath("**", "*.parquet"))
                            )
                    elif inferred_format == "csv":
                        try:
                            df = self.session.read.csv(str(path.joinpath("*.csv")))
                        except Exception:
                            df = self.session.read.csv(
                                str(path.joinpath("**", "*.csv"))
                            )
                    else:
                        raise NotImplementedError(
                            f"Format {inferred_format} is not supported"
                        )
                    phase["rows"] = try_row_count(df)
                    return df

                except FileNotFoundError as exc:
                    msg = f"{dataset_identity}"
                    raise KeyError(msg) from exc
        except BranchNotFoundError:
            logger.info(
                "[FOUNDRY] Branch [%s] not found for dataset [%s]",
                branch,
                dataset_path_or_rid,
            )
            raise BranchNotFoundErrorBase("FOUNDRY")

    async def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]):
        for branch in branches:
            return await self.download_dataset(dataset_path_or_rid, branch=branch)

        raise BranchNotFoundErrorBase("FOUNDRY")

    def get_last_transaction(
        self, dataset_path_or_rid: str, branches: list[str]
    ) -> DataFrame:
        raise NotImplementedError()

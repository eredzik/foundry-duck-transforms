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


@dataclass
class FoundrySource(DataSource):
    ctx: FoundryContext
    session: SparkSession

    async def download_dataset(self, dataset_path_or_rid: str, branch: str) -> DataFrame:
        try:
            dataset_identity = await asyncify(self.ctx.cached_foundry_client._get_dataset_identity)(
                dataset_path_or_rid, branch
            )
            if dataset_identity.get("last_transaction") is None:
                raise BranchNotFoundErrorBase("FOUNDRY")

            last_path, dataset_identity = await asyncify(self.ctx.cached_foundry_client.fetch_dataset)(
                dataset_path_or_rid, branch
            )
            self.last_path = last_path
            _validate_cache_key(dataset_identity)
            try:
                inferred_format = _infer_dataset_format(
                    self.ctx.cached_foundry_client.cache.get_cache_dir(),
                    dataset_identity,
                )
                path = self.ctx.cached_foundry_client.cache._get_storage_location(
                    dataset_identity, inferred_format
                )
                if inferred_format == "parquet":
                    try:
                        return self.session.read.parquet(str(path.joinpath("*.parquet")))
                    except Exception as e:
                        
                        # Exception for partitioned parquet files - mostly for duckdb compat
                        return self.session.read.parquet(str(path.joinpath("**", "*.parquet")))
                elif inferred_format == "csv":
                    ds = self.ctx.get_dataset(dataset_path_or_rid)
                    df: "DataFrame" = ds._context.foundry_sql_server.query_foundry_sql(
                        f"select * from `{dataset_path_or_rid}`",
                        branch=branch,
                        return_type="spark",
                    ) 
                    return df
                raise NotImplementedError(f"Format {inferred_format} is not supported")

            except FileNotFoundError as exc:
                msg = f"{dataset_identity}"
                raise KeyError(msg) from exc
        except BranchNotFoundError:
            print(
                f"[FOUNDRY] Branch [{branch}] not found for dataset [{dataset_path_or_rid}]"
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

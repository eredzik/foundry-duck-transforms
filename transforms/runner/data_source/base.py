from abc import ABC
from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class DataSource(ABC):
    async def download_dataset(
        self,
        dataset_path_or_rid: str,
        branch: str,
    ) -> DataFrame:
        raise NotImplementedError()

    async def download_for_branches(
        self, dataset_path_or_rid: str, branches: list[str]
    ) -> DataFrame:
        raise NotImplementedError()

    def get_last_transaction(
        self, dataset_path_or_rid: str, branches: list[str]
    ) -> DataFrame:
        raise NotImplementedError()

    def download_latest_incremental_transaction(
        self, dataset_path_or_rid: str, branches: list[str], semantic_version: int
    ) -> "DataFrame":
        # TODO: Implement it
        raise NotImplementedError()


@dataclass
class BranchNotFoundError(Exception):
    source: str

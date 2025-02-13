from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from transforms.runner.data_source.base import BranchNotFoundError, DataSource
import logging
logger = logging.getLogger(__name__)


@dataclass
class LocalDataSource(DataSource):
    session: SparkSession
    output_dir: str = str((Path.home() / ".fndry_duck" / "local_output"))

    async def download_dataset(self, dataset_path_or_rid: str, branch: str):
        if not (Path(self.output_dir) / branch / dataset_path_or_rid).exists():
            raise BranchNotFoundError('LOCAL')

        return self.session.read.parquet(
            f"{self.output_dir}/{branch}/{dataset_path_or_rid}/*.parquet"
        )

    async def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]):
        for branch in branches:
            try:
                return await self.download_dataset(dataset_path_or_rid, branch=branch)

            except BranchNotFoundError:
                logger.info(f"[LOCAL]Branch [{branch}] not found for dataset [{dataset_path_or_rid}]")
        raise BranchNotFoundError(source='LOCAL')

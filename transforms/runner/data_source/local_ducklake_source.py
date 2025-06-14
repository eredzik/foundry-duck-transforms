from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from transforms.runner.data_source.base import BranchNotFoundError, DataSource
import logging
logger = logging.getLogger(__name__)


@dataclass
class LocalDucklakeSource(DataSource):
    session: SparkSession
    output_dir: str = str((Path.home() / ".fndry_duck" / "local_output"))

    async def download_dataset(self, dataset_path_or_rid: str, branch: str):
        # TODO: Implement

    async def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]):
        # TODO: Implement
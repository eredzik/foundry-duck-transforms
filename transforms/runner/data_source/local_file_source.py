from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from transforms.runner.data_source.base import BranchNotFoundError, DataSource
from transforms.runner.dataset_logging import (
    dataset_display_name,
    log_dataset_phase,
    try_row_count,
)
import logging

logger = logging.getLogger(__name__)


@dataclass
class LocalDataSource(DataSource):
    session: SparkSession
    output_dir: str = str((Path.home() / ".fndry_duck" / "local_output"))

    def resolve_dataset_label(
        self, dataset_path_or_rid: str, branch: str = "master"
    ) -> str:
        return dataset_display_name(dataset_path_or_rid)

    async def download_dataset(self, dataset_path_or_rid: str, branch: str):
        label = self.resolve_dataset_label(dataset_path_or_rid)
        local_path = Path(self.output_dir) / branch / dataset_path_or_rid
        if not local_path.exists():
            raise BranchNotFoundError("LOCAL")

        with log_dataset_phase(
            "local read",
            label,
            log=logger,
            branch=branch,
            source="local",
            path=str(local_path),
        ) as phase:
            df = self.session.read.parquet(f"{local_path}/*.parquet")
            phase["rows"] = try_row_count(df)
            return df

    async def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]):
        label = self.resolve_dataset_label(dataset_path_or_rid)
        for branch in branches:
            try:
                return await self.download_dataset(dataset_path_or_rid, branch=branch)

            except BranchNotFoundError:
                logger.info(
                    "[LOCAL] Branch [%s] not found for dataset %s",
                    branch,
                    label,
                )
        raise BranchNotFoundError(source="LOCAL")

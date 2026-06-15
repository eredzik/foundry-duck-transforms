import logging
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Callable

import duckdb
from pyspark.sql import DataFrame

from ...generate_types import generate_from_spark
from transforms.runner.dataset_logging import (
    dataset_display_name,
    log_dataset_phase,
    try_row_count,
)

from .local_file_sink import LocalFileSink

logger = logging.getLogger(__name__)


def sanitize_dataset_name(path_or_rid: str) -> str:
    sha_addon = sha256(path_or_rid.encode()).hexdigest()[:2]
    raw_name = path_or_rid.split("/")[-1]
    dataset_name = re.sub("[^a-zA-Z0-9_]", "_", raw_name).lower()
    return f"{dataset_name}_{sha_addon}"


@dataclass
class LocalFileSinkWithDuck(LocalFileSink):
    duckdb_path: str = str((Path.home() / ".fndry_duck" / "analytical_db.db"))
    get_dataset_dataset_name: Callable[[str], str] = sanitize_dataset_name
    resolve_dataset_label: Callable[[str], str] | None = None
    verbose: bool = False

    def _dataset_label(self, dataset_path_or_rid: str) -> str:
        if self.resolve_dataset_label is not None:
            return self.resolve_dataset_label(dataset_path_or_rid)
        return dataset_display_name(dataset_path_or_rid)

    def __post_init__(self):
        Path(self.duckdb_path).parent.mkdir(parents=True, exist_ok=True)

    def save_transaction(
        self,
        df: DataFrame,
        dataset_path_or_rid: str,
    ) -> None:
        label = self._dataset_label(dataset_path_or_rid)
        rows = try_row_count(df, verbose=self.verbose)
        with log_dataset_phase(
            "local write",
            label,
            log=logger,
            branch=self.branch,
            rows=rows if rows is not None else None,
        ) as phase:
            result_path = Path(self.output_dir) / self.branch / dataset_path_or_rid
            result_path.mkdir(parents=True, exist_ok=True)
            super().save_transaction(
                df=df,
                dataset_path_or_rid=str(Path(dataset_path_or_rid) / "data.parquet"),
            )
            phase["path"] = str(result_path)
            self.conn = duckdb.connect(self.duckdb_path, config={})
            dataset_name = self.get_dataset_dataset_name(dataset_path_or_rid)
            generate_from_spark(dataset_name, df)
            parquet_path = self._resolve_parquet_path(dataset_path_or_rid)
            phase["duckdb_view"] = dataset_name
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {dataset_name} as select * from read_parquet('{parquet_path}')"
            )

    def _resolve_parquet_path(self, dataset_path_or_rid: str) -> str:
        dataset_dir = Path(self.output_dir) / self.branch / dataset_path_or_rid
        data_parquet = dataset_dir / "data.parquet"
        if data_parquet.is_file():
            return data_parquet.as_posix()
        if data_parquet.is_dir():
            return f"{data_parquet.as_posix()}/**/*.parquet"
        return f"{dataset_dir.as_posix()}/**/*.parquet"

    def save_incremental_transaction(
        self,
        df: DataFrame,
        dataset_path_or_rid: str,
        semantic_version: int,
    ):
        # TODO: Implement
        pass

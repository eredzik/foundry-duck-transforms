from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import duckdb
from pyspark.sql import DataFrame

from ...generate_types import generate_from_spark

from .local_file_sink import LocalFileSink


@dataclass
class LocalFileSinkWithDuck(LocalFileSink):
    duckdb_path: str = str((Path.home() / ".fndry_duck" / "analytical_db.db"))
    get_dataset_dataset_name: Callable[[str], str] = lambda x: x

    def __post_init__(self):
        Path(self.duckdb_path).parent.mkdir(parents=True, exist_ok=True)

    def save_transaction(
        self,
        df: DataFrame,
        dataset_path_or_rid: str,
    ) -> None:
        result_path = Path(self.output_dir) / self.branch / dataset_path_or_rid
        result_path.mkdir(parents=True, exist_ok=True)
        super().save_transaction(
            df=df,
            dataset_path_or_rid=str(Path(dataset_path_or_rid) / "data.parquet"),
        )
        self.conn = duckdb.connect(self.duckdb_path, config={})
        dataset_name = self.get_dataset_dataset_name(dataset_path_or_rid)
        generate_from_spark(dataset_name, df)
        self.conn.execute(
            f"CREATE OR REPLACE VIEW {dataset_name} as select * from read_parquet('{self.output_dir}/{self.branch}/{dataset_path_or_rid}/*.parquet')"
        )

    def save_incremental_transaction(
        self,
        df: DataFrame,
        dataset_path_or_rid: str,
        semantic_version: int,
    ):
        # TODO: Implement
        pass

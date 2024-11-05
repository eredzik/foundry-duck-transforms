from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import duckdb
from pyspark.sql import DataFrame

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
        super().save_transaction(df=df, dataset_path_or_rid=dataset_path_or_rid)
        self.conn = duckdb.connect(self.duckdb_path, config={})
        self.conn.execute(
            f"CREATE OR REPLACE VIEW {self.get_dataset_dataset_name(dataset_path_or_rid)} as select * from read_parquet('{self.output_dir}/{self.branch}/{dataset_path_or_rid}/*.parquet')" 
        )

    def save_incremental_transaction(
            self,
            df: DataFrame,
            dataset_path_or_rid:str,
            semantic_version: int,
    ):
        # TODO: Implement
        pass
    
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
        self.conn = duckdb.connect(self.duckdb_path)

    def save_transaction(
        self,
        df: DataFrame,
        dataset_path_or_rid: str,
    ) -> None:
        super().save_transaction(df=df, dataset_path_or_rid=dataset_path_or_rid)
        self.conn.execute(
            f"CREATE VIEW {self.get_dataset_dataset_name(dataset_path_or_rid)} IF NOT EXISTS as select * from load_parquet({self.output_dir}/{self.branch}/{dataset_path_or_rid})"
        )

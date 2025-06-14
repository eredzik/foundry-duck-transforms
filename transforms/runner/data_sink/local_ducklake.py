from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import duckdb
from pyspark.sql import DataFrame

from ...generate_types import generate_from_spark

from .local_file_sink import LocalFileSink

import tempfile

@dataclass
class LocalDucklakeSink(LocalFileSink):
    data_files_path: str = str((Path.home() / ".fndry_ducklake" / "data_files"))
    sqlite_meta_path: str = str((Path.home() / ".fndry_ducklake" / "metadata.sqlite"))
    temp_path: str = tempfile.mkdtemp()

    get_dataset_dataset_name: Callable[[str], str] = lambda x: x

    def __post_init__(self):
        Path(self.data_files_path).parent.mkdir(parents=True, exist_ok=True)

    def save_transaction(
        self,
        df: DataFrame,
        dataset_path_or_rid: str,
    ) -> None:
        temp_file =str(Path(self.temp_path) / "data.parquet")
        super().save_transaction(
            df=df,
            dataset_path_or_rid=temp_file,
        )
        self.conn = duckdb.connect(":memory:", config={})
        dataset_name = self.get_dataset_dataset_name(dataset_path_or_rid)
        generate_from_spark(dataset_name, df)
        self.conn.execute(f"""ATTACH 'ducklake:sqlite:{self.sqlite_meta_path}' AS ducklake (DATA_PATH '{self.data_files_path}');""")
        self.conn.execute(
            f"CREATE OR REPLACE TABLE {dataset_name} as select * from read_parquet('{temp_file}/*.parquet')"
        )

    def save_incremental_transaction(
        self,
        df: DataFrame,
        dataset_path_or_rid: str,
        semantic_version: int,
    ):
        # TODO: Implement
        pass

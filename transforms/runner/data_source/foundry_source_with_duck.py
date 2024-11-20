import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import duckdb

from .foundry_source import FoundrySource

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class FoundrySourceWithDuck(FoundrySource):
    duckdb_path: str = str((Path.home() / ".fndry_duck" / "analytical_db.db"))
    duckdb_path_sql: str = str((Path.home() / ".fndry_duck" / "analytical_db.sql"))
    get_dataset_dataset_name: Callable[[str], str] = lambda x: x

    def download_dataset(self, dataset_path_or_rid: str, branch: str) -> "DataFrame":
        df = super().download_dataset(dataset_path_or_rid, branch)
        self.conn = duckdb.connect(self.duckdb_path)
        sanitized_branch = re.sub("[^0-9a-zA-Z]+", "_", branch)
        
        self.conn.execute(f"create schema if not exists {sanitized_branch}")
        if self.last_path.endswith(".parquet"):
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {sanitized_branch}.{self.get_dataset_dataset_name(dataset_path_or_rid)} as select * from read_parquet('{self.last_path}/spark/*.parquet')"
            )
        elif self.last_path.endswith(".csv"):
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {sanitized_branch}.{self.get_dataset_dataset_name(dataset_path_or_rid)} as select * from read_csv('{self.last_path}/*.csv')"
            )
        else:
            raise NotImplementedError(
                f"Format {self.last_path.split('.')[-1]} is not supported"
            )
        with open(self.duckdb_path_sql, 'w') as f:
            for row in self.conn.query(f"select schema_name from information_schema.schemata where catalog_name = '{Path(self.duckdb_path).stem}'").fetchall():
                f.write(f"create schema if not exists {row[0]};\n")
            for row in self.conn.query("select sql from duckdb_views() where not internal").fetchall():
                qry :str= row[0]
                view_replacement = qry.replace('CREATE VIEW ', 'CREATE OR REPLACE VIEW ')
                f.write(f"{view_replacement};\n")
        self.conn.close()
        return df

    def download_latest_incremental_transaction(
        self, dataset_path_or_rid: str, branches: list[str], semantic_version: int
    ) -> "DataFrame":
        # TODO: Implement it
        raise NotImplementedError()

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import duckdb

from .foundry_source import FoundrySource


@dataclass
class FoundrySourceWithDuck(FoundrySource):
    duckdb_path: str = str((Path.home() / ".fndry_duck" / "analytical_db.db"))
    get_dataset_dataset_name: Callable[[str], str] = lambda x: x

    def download_dataset(self, dataset_path_or_rid: str, branch: str):
        df = super().download_dataset(dataset_path_or_rid, branch)
        self.conn = duckdb.connect(self.duckdb_path)
        sanitized_branch = re.sub('[^0-9a-zA-Z]+', '_', branch)
        self.conn.execute(f"create schema if not exists {sanitized_branch}")
        if self.last_path.endswith('.parquet'):
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {sanitized_branch}.{self.get_dataset_dataset_name(dataset_path_or_rid)} as select * from read_parquet('{self.last_path}')" 
            )
        elif self.last_path.endswith('.csv'):
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {sanitized_branch}.{self.get_dataset_dataset_name(dataset_path_or_rid)} as select * from read_csv('{self.last_path}')" 
            )
        else:
            raise NotImplementedError(f"Format {self.last_path.split('.')[-1]} is not supported")
        self.conn.close()
        return df
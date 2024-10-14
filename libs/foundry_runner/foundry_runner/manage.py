import re
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar

import duckdb
from foundry_dev_tools import FoundryContext

T = TypeVar("T")


@dataclass
class DatasetVersion:
    dataset_rid: str
    dataset_branch: str
    sanitized_rid: str
    sanitized_branch_name: str
    dataset_identity: str
    last_update: datetime


class FoundryManager:
    def __init__(
        self,
        duckdb_conn: duckdb.DuckDBPyConnection,
        ctx: FoundryContext | None = None,
        branch_name: str | None = None,
        fallback_branches: list[str] | None = None,
    ):
        self.ctx = ctx or FoundryContext()
        self.duckdb_conn = duckdb_conn
        self.branch_name = branch_name or "master"
        if fallback_branches:
            self.fallback_branches = fallback_branches
        else:
            self.fallback_branches = [] if self.branch_name == "master" else ["master"]

        duckdb_conn.execute(
            f"CREATE SCHEMA IF NOT EXISTS fndry_{sanitize(self.branch_name)}"
        )
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS work")
        duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
        duckdb_conn.execute("""
            CREATE TABLE IF NOT EXISTS datasets_versions (
                dataset_rid VARCHAR,
                dataset_branch VARCHAR,
                sanitized_rid VARCHAR,
                sanitized_branch_name VARCHAR,
                dataset_identity VARCHAR,
                last_update DATETIME
            ) """)

        for branch in self.fallback_branches:
            duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS fndry_{sanitize(branch)}")

        pass

    def collect_transform_inputs(self, transform: Transform[T]) -> None:
        return None

    def collect_transform_outputs(self, transform: Transform[T]) -> None:
        return None

    def get_dataset_from_foundry_into_duckdb(
        self,
        dataset_rid: str,
        update: bool=False,
    ) -> bool:
        identity = self.ctx.cached_foundry_client._get_dataset_identity(
            dataset_rid, branch=self.branch_name
        )
        meta = self.get_meta_for_dataset(dataset_rid, branch=self.branch_name)
        if meta and not update:
            return False
        
        with self.ctx.cached_foundry_client.api.download_dataset_files_temporary(dataset_rid=dataset_rid, view=self.branch_name, ) as temp_output:
            self.duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS fndry_{sanitize(self.branch_name)}")
            self.duckdb_conn.execute(f"CREATE TABLE IF NOT EXISTS fndry_{sanitize(self.branch_name)}.{identity['dataset_path']} AS SELECT * FROM read_parquet('{temp_output}')")
            self.duckdb_conn.execute(
                f"INSERT INTO meta.datasets_versions VALUES ('{dataset_rid}', '{self.branch_name}', '{identity['dataset_path']}', '{self.branch_name}', '{identity['dataset_path']}', '{datetime.now()}')"
            )




        return self.ctx.cached_foundry_client.load_dataset(
            dataset_rid, branch=self.branch_name
        )

    def get_meta_for_dataset(self, dataset_rid: str, branch: str = "master"):
        res = self.duckdb_conn.query(
            f"SELECT * FROM meta.datasets_versions WHERE dataset_rid = '{dataset_rid}' AND dataset_branch = '{branch}'"
        )
        res1: (
            tuple[
                str,
                str,
                str,
                str,
                str,
                datetime,
            ]
            | None
        ) = res.fetchone()
        if res1:
            return DatasetVersion(*res1)


def sanitize(branch_name: str) -> str:
    return re.sub("^[a-zA-Z0-9_]", "_", branch_name)

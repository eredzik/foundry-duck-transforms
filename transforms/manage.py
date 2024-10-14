import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TypeVar

import duckdb
from foundry_dev_tools import FoundryContext
from foundry_dev_tools.errors.dataset import BranchNotFoundError

from transforms.api import Transform

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

        self.duckdb_conn.execute(
            f"CREATE SCHEMA IF NOT EXISTS fndry_{sanitize(self.branch_name)}"
        )
        self.duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS work")
        self.duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
        self.duckdb_conn.execute("""
            CREATE TABLE IF NOT EXISTS meta.datasets_versions (
                dataset_rid VARCHAR,
                dataset_branch VARCHAR,
                sanitized_rid VARCHAR,
                sanitized_branch_name VARCHAR,
                dataset_identity VARCHAR,
                last_update DATETIME
            ) """)

        for branch in self.fallback_branches:
            self.duckdb_conn.execute(
                f"CREATE SCHEMA IF NOT EXISTS fndry_{sanitize(branch)}"
            )
        self.duckdb_conn.commit()

    def collect_transform_inputs(self, transform: Transform[T]) -> None:
        for input in transform.inputs.values():
            if input.branch is None:
                
                self.get_dataset_from_foundry_into_duckdb(
                    input.path_or_rid,
                    branch=input.branch,
                )
                return
            else:
                try:
                    self.get_dataset_from_foundry_into_duckdb(
                        input.path_or_rid,
                        branch=input.branch,
                    )
                    return
                except BranchNotFoundError as e:
                    for branch in self.fallback_branches:
                        self.get_dataset_from_foundry_into_duckdb(
                            input.path_or_rid,
                            branch=branch,
                        )
                        return
                    else:
                        raise e

        return

    def collect_transform_outputs(self, transform: Transform[T]) -> None:
        return None

    def get_dataset_from_foundry_into_duckdb(
        self,
        dataset_rid: str,
        branch: str | None,
        update: bool = False,
    ) -> bool:
        branch_to_use = branch or self.branch_name
        identity = self.ctx.cached_foundry_client._get_dataset_identity(
            dataset_rid, branch=branch_to_use
        )
        meta = self.get_meta_for_dataset(dataset_rid, branch=branch_to_use)
        if meta and not update:
            return False

        with self.ctx.cached_foundry_client.api.download_dataset_files_temporary(
            dataset_rid=dataset_rid,
            view=branch_to_use,
        ) as temp_output:
            sanitized_dataset_name = sanitize(identity["dataset_path"])
            sanitized_branch_name = sanitize(branch_to_use)
            self.duckdb_conn.execute(
                f"CREATE SCHEMA IF NOT EXISTS fndry_{sanitized_branch_name}"
            )
            temp_dataset_spark = Path(temp_output) / "spark/*"
            create_table_query = f"CREATE TABLE IF NOT EXISTS fndry_{sanitized_branch_name}.{sanitized_dataset_name} AS SELECT * FROM read_parquet('{temp_dataset_spark}')"
            self.duckdb_conn.execute(create_table_query)
            self.duckdb_conn.execute(
                f"INSERT INTO meta.datasets_versions VALUES ('{dataset_rid}', '{self.branch_name}', '{identity['dataset_path']}', '{self.branch_name}', '{identity['dataset_path']}', '{datetime.now()}')"
            )
            return True

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
    return re.sub("[^a-zA-Z0-9_]", "_", branch_name)

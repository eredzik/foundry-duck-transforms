import re
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TypedDict, TypeVar

import duckdb
from foundry_dev_tools import FoundryContext
from foundry_dev_tools.errors.dataset import BranchNotFoundError
from foundry_dev_tools.errors.meta import FoundryAPIError

from transforms.api import Transform

T = TypeVar("T")


@dataclass
class DatasetVersion:
    dataset_rid: str
    dataset_branch: str
    sanitized_rid: str
    sanitized_branch_name: str
    dataset_name: str
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
                dataset_name VARCHAR,
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
            # Input has pinned branch - don't try to fallback
            if input.branch is not None:
                self.get_dataset_from_foundry_into_duckdb(
                    input.path_or_rid,
                    branch=input.branch,
                )

            else:
                try:
                    # Try main branch name
                    self.get_dataset_from_foundry_into_duckdb(
                        input.path_or_rid,
                        branch=self.branch_name,
                    )

                except BranchNotFoundError as e:
                    for branch in self.fallback_branches:
                        # Try fallbacks and map back as view if found
                        created_dataset = self.get_dataset_from_foundry_into_duckdb(
                            input.path_or_rid,
                            branch=branch,
                        )
                        self.create_view(
                            src_schema=created_dataset["schema"],
                            src_table=created_dataset["tablename"],
                            target_schema=self.branch_name,
                            target_table=created_dataset["tablename"],
                        )
                        self.create_view(
                            src_schema=created_dataset["schema"],
                            src_table=created_dataset["tablename"],
                            target_schema=self.branch_name,
                            target_table=sanitize(input.path_or_rid),
                        )
                        break
                    else:
                        raise e

        return

    @contextmanager
    def download_file_to_temp_parquet(self, dataset_rid: str, branch: str):
        temp = tempfile.mkdtemp(suffix=f"foundry_dev_tools-{dataset_rid}")

        try:
            self.ctx.cached_foundry_client.api.download_dataset_files(
                dataset_rid=dataset_rid, view=branch, output_directory=temp
            )
            yield temp
        except FoundryAPIError:
            yield "TODO"
            print("TODO: Download dataset through sql")

    def collect_transform_outputs(self, transform: Transform[T]) -> None:
        return None

    def get_dataset_from_foundry_into_duckdb(
        self,
        dataset_rid: str,
        branch: str | None,
        update: bool = False,
    ) -> "DbDatasetInfo":
        branch_to_use = branch or self.branch_name
        identity = self.ctx.cached_foundry_client._get_dataset_identity(
            dataset_rid, branch=branch_to_use
        )
        meta = self.get_meta_for_dataset(dataset_rid, branch=branch_to_use)
        if meta and not update:
            return DbDatasetInfo(schema=sanitize(branch_to_use), tablename=dataset_rid)

        with self.download_file_to_temp_parquet(
            dataset_rid=dataset_rid,
            branch=branch_to_use,
        ) as temp_output:
            sanitized_rid = sanitize(dataset_rid)
            sanitized_dataset_name = sanitize(identity["dataset_path"].split("/")[-1])
            sanitized_branch_name = "fndry_" + sanitize(branch_to_use)
            self.duckdb_conn.execute(
                f"CREATE SCHEMA IF NOT EXISTS {sanitized_branch_name}"
            )
            temp_dataset_spark = Path(temp_output) / "spark/*"
            create_table_query = f"CREATE TABLE IF NOT EXISTS {sanitized_branch_name}.{sanitized_rid} AS SELECT * FROM read_parquet('{temp_dataset_spark}')"
            self.duckdb_conn.execute(create_table_query)

            self.duckdb_conn.execute(
                f"""INSERT INTO meta.datasets_versions by name (
                SELECT
                '{dataset_rid}' as dataset_rid,
                '{branch_to_use}' as dataset_branch,
                '{sanitized_rid}' as sanitized_rid,
                '{sanitized_branch_name}' as sanitized_branch_name,
                '{sanitized_rid}' as dataset_name,
                '{identity['last_transaction_rid']}' as dataset_identity,
                '{datetime.now()}' as last_update
            )
                """
            )
            self.create_view(
                src_schema=sanitized_branch_name,
                src_table=sanitized_rid,
                target_schema=sanitized_branch_name,
                target_table=sanitized_dataset_name,
            )

            return DbDatasetInfo(schema=sanitized_branch_name, tablename=dataset_rid)

    def create_view(
        self, src_schema: str, src_table: str, target_schema: str, target_table: str
    ):
        self.duckdb_conn.execute(f"""
            create or replace view {target_schema}.{target_table} as select * from {src_schema}.{src_table}
            """)

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
                str,
                datetime,
            ]
            | None
        ) = res.fetchone()
        if res1:
            return DatasetVersion(*res1)


def sanitize(branch_name: str) -> str:
    return re.sub("[^a-zA-Z0-9_]", "_", branch_name).lower()


class DbDatasetInfo(TypedDict):
    schema: str
    tablename: str

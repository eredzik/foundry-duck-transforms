from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb


@dataclass
class DatasetVersion:
    dataset_rid: str
    dataset_branch: str
    sanitized_rid: str
    sanitized_branch_name: str
    dataset_name: str
    dataset_identity: str
    last_update: datetime


@dataclass()
class DataManager:
    connection_url: str | Path = ":memory:"

    def __post_init__(self):
        self.conn = duckdb.connect(database=self.connection_url)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS datasets_versions (
                identifier VARCHAR,
                data_branch VARCHAR,
                dataset_schema VARCHAR,
                dataset_tablename VARCHAR,
                last_update DATETIME
            ) """)

    def load_parquet_into_database(
        self,
        identifiers: list[str],
        parquet_path: Path | str,
        branch: str,
        tablename: str,
    ):
        self.conn

    def load_dataset_into_database(
        self,
        from_specifier: str,
        identifiers: list[str],
        data_branch: str,
        schema: str,
        tablename: str,
    ):
        self.create_schema_if_not_exists(schema)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{tablename} AS
        SELECT *
        FROM {from_specifier}"""
        self.conn.execute(create_table_query)
        for identifier in identifiers:
            self.mark_in_metadata(
                identifier=identifier,
                data_branch=data_branch,
                dataset_schema=schema,
                dataset_tablename=tablename,
            )

    def mark_in_metadata(
        self,
        identifier: str,
        data_branch: str,
        dataset_schema: str,
        dataset_tablename: str,
    ):
        self.conn.execute(
            f"""INSERT INTO meta.datasets_versions by name (
                SELECT
                '{identifier}' as identifier,
                '{data_branch}' as data_branch,
                '{dataset_schema}' as dataset_schema,
                '{dataset_tablename}' as dataset_tablename,
            )
                """
        )

    def create_schema_if_not_exists(self, schema: str):
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def create_view(
        self, src_schema: str, src_table: str, target_schema: str, target_table: str
    ):
        self.conn.execute(f"""
            create or replace view {target_schema}.{target_table} as
            select * from {src_schema}.{src_table}
        """)

    def get_table_by_identifier_branch(self, data_branch: str, identifier: str):
        res = self.conn.query(
            f"""SELECT *
            FROM meta.datasets_versions
            WHERE
                dataset_rid = '{dataset_rid}' AND
                dataset_branch = '{branch}'
            """
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

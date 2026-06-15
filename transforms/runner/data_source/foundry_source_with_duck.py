from hashlib import sha256
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from transforms.runner.data_source.download_result import DownloadMetadata, DownloadResult
from transforms.runner.dataset_logging import log_dataset_phase, log_verbose_step

from .foundry_source import FoundrySource

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class FoundrySourceWithDuck(FoundrySource):
    duckdb_path: str = str((Path.home() / ".fndry_duck" / "analytical_db.db"))
    duckdb_path_sql: str = str((Path.home() / ".fndry_duck" / "analytical_db.sql"))
    _dataset_name_cache: dict[str, str] = field(default_factory=dict, repr=False)

    def get_dataset_name(
        self, dataset_path_or_rid: str, *, dataset_path: str | None = None
    ) -> str:
        cached = self._dataset_name_cache.get(dataset_path_or_rid)
        if cached is not None:
            return cached

        if dataset_path is None:
            dataset_path = str(self.ctx.get_dataset(dataset_path_or_rid).path)

        sha_addon = sha256(dataset_path.encode()).hexdigest()[:2]
        raw_name = dataset_path.split("/")[-1]
        dataset_name = re.sub("[^a-zA-Z0-9_]", "_", raw_name).lower()
        name = f"{dataset_name}_{sha_addon}"
        self._dataset_name_cache[dataset_path_or_rid] = name
        return name

    def _download_dataset_sync(
        self, dataset_path_or_rid: str, branch: str
    ) -> DownloadResult:
        result = super()._download_dataset_sync(dataset_path_or_rid, branch)
        if result.metadata is not None:
            cached_identity = self._get_cached_identity_if_on_disk(dataset_path_or_rid)
            with log_verbose_step(
                "dataset name",
                verbose=self.verbose,
                log=logger,
                dataset=dataset_path_or_rid,
            ):
                result.metadata.dataset_name = self.get_dataset_name(
                    dataset_path_or_rid,
                    dataset_path=(
                        cached_identity.get("dataset_path")
                        if cached_identity is not None
                        else None
                    ),
                )
        return result

    def register_duckdb_views(self, metadata_list: list[DownloadMetadata]) -> None:
        view_metadata = [m for m in metadata_list if not m.is_query_cache]
        if not view_metadata:
            return

        conn = duckdb.connect(self.duckdb_path)
        try:
            for meta in view_metadata:
                if meta.dataset_name is None:
                    continue
                label = self.resolve_dataset_label(
                    meta.dataset_path_or_rid, meta.branch
                )
                sanitized_branch = re.sub("[^0-9a-zA-Z]+", "_", meta.branch)
                with log_dataset_phase(
                    "duckdb view registration",
                    label,
                    log=logger,
                    branch=meta.branch,
                    view=f"{sanitized_branch}.{meta.dataset_name}",
                ):
                    conn.execute(f"create schema if not exists {sanitized_branch}")
                    posix_path = Path(meta.last_path).as_posix()
                    if meta.last_path.endswith(".parquet"):
                        conn.execute(
                            f"CREATE OR REPLACE VIEW {sanitized_branch}.{meta.dataset_name} "
                            f"as select * from read_parquet('{posix_path}/**/*.parquet')"
                        )
                    elif meta.last_path.endswith(".csv"):
                        conn.execute(
                            f"CREATE OR REPLACE VIEW {sanitized_branch}.{meta.dataset_name} "
                            f"as select * from read_csv('{posix_path}/*.csv')"
                        )
                    else:
                        raise NotImplementedError(
                            f"Format {meta.last_path.split('.')[-1]} is not supported"
                        )
            self._dump_duckdb_sql(conn)
        finally:
            conn.close()

    def _dump_duckdb_sql(self, conn: duckdb.DuckDBPyConnection) -> None:
        with open(self.duckdb_path_sql, "w") as f:
            for row in conn.query(
                f"select schema_name from information_schema.schemata "
                f"where catalog_name = '{Path(self.duckdb_path).stem}'"
            ).fetchall():
                f.write(f"create schema if not exists {row[0]};\n")
            for row in conn.query(
                "select sql from duckdb_views() where not internal"
            ).fetchall():
                qry: str = row[0]
                view_replacement = qry.replace(
                    "CREATE VIEW ", "CREATE OR REPLACE VIEW "
                )
                f.write(f"{view_replacement};\n")

    def download_latest_incremental_transaction(
        self, dataset_path_or_rid: str, branches: list[str], semantic_version: int
    ) -> "DataFrame":
        raise NotImplementedError()

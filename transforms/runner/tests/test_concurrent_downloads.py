import asyncio
import time
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession

from transforms.api.transform_df import Input, Output, Transform
from transforms.runner.data_source.base import DataSource
from transforms.runner.data_source.download_result import DownloadMetadata, DownloadResult
from transforms.runner.data_source.foundry_source_with_duck import FoundrySourceWithDuck
from transforms.runner.dataset_logging import log_verbose_step
from transforms.runner.exec_transform import TransformRunner
from transforms.runner.tests.test_exec_transform import MockDataSink


class SlowMockDataSource(DataSource):
    def __init__(self, data_dict: Dict[str, DataFrame], delay: float = 0.3):
        self.data = data_dict
        self.delay = delay

    async def download_for_branches(
        self, dataset_path_or_rid: str, branches: List[str]
    ) -> DownloadResult:
        await asyncio.sleep(self.delay)
        df = self.data[dataset_path_or_rid]
        return DownloadResult(
            df=df,
            metadata=DownloadMetadata(
                dataset_path_or_rid=dataset_path_or_rid,
                branch=branches[-1],
                last_path=f"/cache/{dataset_path_or_rid}.parquet",
                dataset_name=f"ds_{dataset_path_or_rid}",
            ),
        )

    async def download_dataset(
        self, dataset_path_or_rid: str, branch: str
    ) -> DownloadResult:
        return await self.download_for_branches(dataset_path_or_rid, [branch])


class IsolatedMetadataSource(DataSource):
    def __init__(self, delay: float = 0.05):
        self.delay = delay
        self.results: list[DownloadResult] = []

    async def download_for_branches(
        self, dataset_path_or_rid: str, branches: List[str]
    ) -> DownloadResult:
        await asyncio.sleep(self.delay)
        result = DownloadResult(
            df=MagicMock(),
            metadata=DownloadMetadata(
                dataset_path_or_rid=dataset_path_or_rid,
                branch=branches[-1],
                last_path=f"/unique/{dataset_path_or_rid}/{branches[-1]}.parquet",
                dataset_name=f"name_{dataset_path_or_rid}",
            ),
        )
        self.results.append(result)
        return result

    async def download_dataset(
        self, dataset_path_or_rid: str, branch: str
    ) -> DownloadResult:
        return await self.download_for_branches(dataset_path_or_rid, [branch])


def _multi_input_transform() -> Transform:
    def compute(input_a: DataFrame, input_b: DataFrame, input_c: DataFrame) -> DataFrame:
        return input_a

    return Transform(
        inputs={
            "input_a": Input(path_or_rid="ds_a"),
            "input_b": Input(path_or_rid="ds_b"),
            "input_c": Input(path_or_rid="ds_c"),
        },
        outputs={"output": Output(path_or_rid="out", checks=[])},
        transform=compute,
    )


@pytest.mark.asyncio
async def test_parallel_downloads_overlap_in_time(
    spark: SparkSession,
) -> None:
    data = {
        "ds_a": spark.createDataFrame([(1,)], ["v"]),
        "ds_b": spark.createDataFrame([(2,)], ["v"]),
        "ds_c": spark.createDataFrame([(3,)], ["v"]),
    }
    source = SlowMockDataSource(data, delay=0.3)
    runner = TransformRunner(
        sourcer=source,
        sink=MockDataSink(),
        fallback_branches=["master"],
        output_dir=Path("/tmp/test_output"),
        secrets_config_location=Path("/tmp/test_secrets"),
    )

    started = time.perf_counter()
    await runner.download_datasets(_multi_input_transform(), omit_checks=True, dry_run=False)
    elapsed = time.perf_counter() - started

    assert elapsed < 0.75, f"Expected parallel downloads (~0.3s), got {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_parallel_downloads_preserve_metadata() -> None:
    source = IsolatedMetadataSource(delay=0.05)
    runner = TransformRunner(
        sourcer=source,
        sink=MockDataSink(),
        fallback_branches=["master"],
        output_dir=Path("/tmp/test_output"),
        secrets_config_location=Path("/tmp/test_secrets"),
    )

    await runner.download_datasets(_multi_input_transform(), omit_checks=True, dry_run=False)

    by_rid = {
        r.metadata.dataset_path_or_rid: r.metadata.last_path
        for r in source.results
        if r.metadata is not None
    }
    assert by_rid == {
        "ds_a": "/unique/ds_a/master.parquet",
        "ds_b": "/unique/ds_b/master.parquet",
        "ds_c": "/unique/ds_c/master.parquet",
    }


def test_get_dataset_name_is_cached() -> None:
    ctx = MagicMock()
    dataset = MagicMock()
    dataset.path = "/path/to/My Dataset"
    ctx.get_dataset.return_value = dataset

    source = FoundrySourceWithDuck(ctx=ctx, session=MagicMock())

    first = source.get_dataset_name("ri.foundry.main.dataset.abc")
    second = source.get_dataset_name("ri.foundry.main.dataset.abc")

    assert first == second == "my_dataset_f7"
    ctx.get_dataset.assert_called_once_with("ri.foundry.main.dataset.abc")


def test_get_dataset_name_uses_provided_dataset_path() -> None:
    ctx = MagicMock()
    source = FoundrySourceWithDuck(ctx=ctx, session=MagicMock())

    name = source.get_dataset_name(
        "ri.foundry.main.dataset.abc",
        dataset_path="/path/to/My Dataset",
    )

    assert name == "my_dataset_f7"
    ctx.get_dataset.assert_not_called()


def test_register_duckdb_views_batch(tmp_path: Path) -> None:
    import duckdb

    db_path = tmp_path / "analytical_db.db"
    sql_path = tmp_path / "analytical_db.sql"
    parquet_dir = tmp_path / "dataset.parquet"
    parquet_dir.mkdir()
    duckdb.connect().execute(
        f"COPY (SELECT 1 AS v) TO '{parquet_dir.as_posix()}/part.parquet' (FORMAT PARQUET)"
    )

    source = FoundrySourceWithDuck(
        ctx=MagicMock(),
        session=MagicMock(),
        duckdb_path=str(db_path),
        duckdb_path_sql=str(sql_path),
    )

    cache_path = str(parquet_dir)
    metadata = [
        DownloadMetadata(
            dataset_path_or_rid="ri.foundry.main.dataset.a",
            branch="master",
            last_path=cache_path,
            dataset_name="dataset_a_ab",
        ),
        DownloadMetadata(
            dataset_path_or_rid="ri.foundry.main.dataset.b",
            branch="dev",
            last_path=cache_path,
            dataset_name="dataset_b_cd",
        ),
    ]

    source.register_duckdb_views(metadata)

    conn = duckdb.connect(str(db_path))
    views = {
        row[0]
        for row in conn.execute(
            "select view_name from duckdb_views() where not internal"
        ).fetchall()
    }
    conn.close()

    assert "dataset_a_ab" in views
    assert "dataset_b_cd" in views
    assert sql_path.exists()


def test_log_verbose_step_skips_without_verbose(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("INFO"):
        with log_verbose_step("test step", verbose=False):
            pass

    assert not any("[test step]" in record.message for record in caplog.records)


def test_log_verbose_step_logs_when_verbose(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("INFO"):
        with log_verbose_step("test step", verbose=True, label="example"):
            pass

    matching = [r for r in caplog.records if "[test step]" in r.message]
    assert len(matching) == 1
    assert "label=example" in matching[0].message

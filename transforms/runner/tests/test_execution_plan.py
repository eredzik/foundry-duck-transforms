import pytest
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from transforms.api.transform_df import Input, Output, Transform, TransformOutput
from transforms.engine.duckdb import init_sess
from transforms.runner.data_sink.base import DataSink
from transforms.runner.data_source.base import DataSource
from transforms.runner.data_source.download_result import DownloadResult
from transforms.runner.exec_transform import TransformRunner
from transforms.runner.execution_plan import format_execution_plan


class MockDataSource(DataSource):
    def __init__(self, data_dict: Dict[str, DataFrame]):
        self.data = data_dict

    async def download_for_branches(
        self, dataset_path_or_rid: str, branches: List[str]
    ) -> DownloadResult:
        df = self.data.get(dataset_path_or_rid)
        if df is None:
            raise ValueError(f"Dataset {dataset_path_or_rid} not found")
        return DownloadResult(df=df)

    async def download_latest_incremental_transaction(
        self, dataset_path_or_rid: str, branches: List[str], semantic_version: int
    ) -> DataFrame:
        df = self.data.get(f"{dataset_path_or_rid}_incremental")
        if df is None:
            raise ValueError(f"Incremental dataset {dataset_path_or_rid} not found")
        return df


class MockDataSink(DataSink):
    def __init__(self):
        self.saved_data: Dict[str, DataFrame] = {}

    def save_transaction(self, df: DataFrame, dataset_path_or_rid: str) -> None:
        self.saved_data[dataset_path_or_rid] = df

    def save_incremental_transaction(
        self, df: DataFrame, dataset_path_or_rid: str, semantic_version: int
    ) -> None:
        pass


def simple_transform(input1: DataFrame, input2: DataFrame) -> DataFrame:
    return input1.union(input2)


def multi_transform(
    input1: DataFrame, output1: TransformOutput, output2: TransformOutput
) -> None:
    output1.on_dataframe_write(input1.filter("value % 2 = 0"), "replace")
    output2.on_dataframe_write(input1.filter("value % 2 = 1"), "replace")
    return None


class SimpleTransform(Transform):
    def __init__(self):
        super().__init__(
            inputs={
                "input1": Input(path_or_rid="test_input1"),
                "input2": Input(path_or_rid="test_input2"),
            },
            outputs={"output": Output(path_or_rid="test_output", checks=[])},
            transform=simple_transform,
            multi_outputs=None,
        )


class MultiOutputTransform(Transform):
    def __init__(self):
        super().__init__(
            inputs={"input1": Input(path_or_rid="test_input1")},
            outputs={
                "output1": Output(path_or_rid="test_output1", checks=[]),
                "output2": Output(path_or_rid="test_output2", checks=[]),
            },
            transform=multi_transform,
            multi_outputs={},
        )


@pytest.fixture
def sample_data(spark: SparkSession) -> Dict[str, DataFrame]:
    df1 = spark.createDataFrame([(i,) for i in range(5)], ["value"])
    df2 = spark.createDataFrame([(i,) for i in range(5, 10)], ["value"])
    return {"test_input1": df1, "test_input2": df2}


@pytest.fixture
def transform_runner(sample_data: Dict[str, DataFrame]) -> TransformRunner:
    return TransformRunner(
        sourcer=MockDataSource(sample_data),
        sink=MockDataSink(),
        fallback_branches=["main"],
        output_dir=Path("/tmp/test_output"),
        secrets_config_location=Path("/tmp/test_secrets"),
    )


def test_format_execution_plan_contains_sql_and_explain() -> None:
    spark = init_sess()
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).filter("id > 0")

    plan = format_execution_plan(df)

    assert "=== SQL ===" in plan
    assert "=== EXPLAIN ===" in plan
    assert "SELECT" in plan.upper()


def test_format_execution_plan_with_label() -> None:
    spark = init_sess()
    df = spark.createDataFrame([(1,)], ["value"])

    plan = format_execution_plan(df, label="my_output")

    assert "=== output: my_output ===" in plan


def test_format_execution_plan_rejects_non_dataframe() -> None:
    with pytest.raises(ValueError, match="DataFrame-like"):
        format_execution_plan(None)


@pytest.mark.asyncio
async def test_explain_only_single_output(
    transform_runner: TransformRunner, capsys: pytest.CaptureFixture[str]
) -> None:
    transform = SimpleTransform()
    sources = await transform_runner.download_datasets(
        transform, omit_checks=True, dry_run=False
    )
    transform_runner.exec_transform(
        transform,
        omit_checks=True,
        dry_run=False,
        sources=sources,
        explain_only=True,
    )

    captured = capsys.readouterr()
    assert "=== SQL ===" in captured.out
    assert "=== EXPLAIN ===" in captured.out
    assert len(transform_runner.sink.saved_data) == 0


@pytest.mark.asyncio
async def test_explain_only_multi_output(
    transform_runner: TransformRunner, capsys: pytest.CaptureFixture[str]
) -> None:
    transform = MultiOutputTransform()
    sources = await transform_runner.download_datasets(
        transform, omit_checks=True, dry_run=False
    )
    transform_runner.exec_transform(
        transform,
        omit_checks=True,
        dry_run=False,
        sources=sources,
        explain_only=True,
    )

    captured = capsys.readouterr()
    assert "=== output: output1 ===" in captured.out
    assert "=== output: output2 ===" in captured.out
    assert len(transform_runner.sink.saved_data) == 0

import pytest
from pathlib import Path
from typing import Dict, List, Any, cast
from pyspark.sql import DataFrame, SparkSession, Row
from transforms.api.transform_df import Transform, TransformOutput, Input, Output
from transforms.runner.data_sink.base import DataSink
from transforms.runner.data_source.base import DataSource
from transforms.runner.exec_transform import TransformRunner
from transforms.engine.duckdb import init_sess

class MockDataSource(DataSource):
    def __init__(self, data_dict: Dict[str, DataFrame]):
        self.data = data_dict
    
    async def download_for_branches(self, dataset_path_or_rid: str, branches: List[str]) -> DataFrame:
        df = self.data.get(dataset_path_or_rid)
        if df is None:
            raise ValueError(f"Dataset {dataset_path_or_rid} not found")
        return df
    
    async def download_latest_incremental_transaction(self, dataset_path_or_rid: str, branches: List[str], semantic_version: int) -> DataFrame:
        df = self.data.get(f"{dataset_path_or_rid}_incremental")
        if df is None:
            raise ValueError(f"Incremental dataset {dataset_path_or_rid} not found")
        return df

class MockDataSink(DataSink):
    def __init__(self):
        self.saved_data: Dict[str, DataFrame] = {}
        self.saved_incremental: Dict[str, DataFrame] = {}
    
    def save_transaction(self, df: DataFrame, dataset_path_or_rid: str) -> None:
        self.saved_data[dataset_path_or_rid] = df
    
    def save_incremental_transaction(self, df: DataFrame, dataset_path_or_rid: str, semantic_version: int) -> None:
        self.saved_incremental[f"{dataset_path_or_rid}_{semantic_version}"] = df

def simple_transform(input1: DataFrame, input2: DataFrame) -> DataFrame:
    return input1.union(input2)

def multi_transform(input1: DataFrame, output1: TransformOutput, output2: TransformOutput) -> None:
    df1 = input1.filter("value % 2 = 0")
    df2 = input1.filter("value % 2 = 1")
    output1.on_dataframe_write(df1, "replace")
    output2.on_dataframe_write(df2, "replace")
    return None

class SimpleTransform(Transform):
    def __init__(self):
        inputs = {
            "input1": Input(path_or_rid="test_input1"),
            "input2": Input(path_or_rid="test_input2")
        }
        outputs = {
            "output": Output(path_or_rid="test_output", checks=[])
        }
        super().__init__(inputs=inputs, outputs=outputs, transform=simple_transform)

class MultiOutputTransform(Transform):
    def __init__(self):
        inputs = {
            "input1": Input(path_or_rid="test_input1")
        }
        outputs = {
            "output1": Output(path_or_rid="test_output1", checks=[]),
            "output2": Output(path_or_rid="test_output2", checks=[])
        }
        super().__init__(inputs=inputs, outputs=outputs, transform=multi_transform)
        self.multi_outputs = {}

@pytest.fixture
def sample_data(spark: SparkSession) -> Dict[str, DataFrame]:
    data1 = [(i,) for i in range(5)]
    data2 = [(i,) for i in range(5, 10)]
    df1 = spark.createDataFrame(data1, ["value"])
    df2 = spark.createDataFrame(data2, ["value"])
    return {
        "test_input1": df1,
        "test_input2": df2,
        "test_input1_incremental": df1.limit(2)
    }

@pytest.fixture
def transform_runner(sample_data: Dict[str, DataFrame]) -> TransformRunner:
    source = MockDataSource(sample_data)
    sink = MockDataSink()
    return TransformRunner(
        sourcer=source,
        sink=sink,
        fallback_branches=["main"],
        output_dir=Path("/tmp/test_output"),
        secrets_config_location=Path("/tmp/test_secrets")
    )

@pytest.mark.asyncio
async def test_simple_transform(transform_runner: TransformRunner, sample_data: Dict[str, DataFrame]) -> None:
    transform = SimpleTransform()
    sources = await transform_runner.download_datasets(transform, omit_checks=True, dry_run=False)
    transform_runner.exec_transform(transform, omit_checks=True, dry_run=False, sources=sources)
    
    # Verify the output
    output_df = transform_runner.sink.saved_data["test_output"]
    expected_count = (
        sample_data["test_input1"].count() + 
        sample_data["test_input2"].count()
    )
    assert output_df.count() == expected_count

@pytest.mark.asyncio
async def test_multi_output_transform(transform_runner: TransformRunner, sample_data: Dict[str, DataFrame]) -> None:
    transform = MultiOutputTransform()
    sources = await transform_runner.download_datasets(transform, omit_checks=True, dry_run=False)
    
    # Debug: Print sources before execution
    print(f"Sources before execution: {sources.keys()}")
    print(f"Transform outputs: {transform.outputs.keys()}")
    print(f"Transform multi_outputs: {transform.multi_outputs}")
    
    transform_runner.exec_transform(transform, omit_checks=True, dry_run=False, sources=sources)
    
    # Debug: Print saved data after execution
    print(f"Saved data keys: {transform_runner.sink.saved_data.keys()}")
    
    # Verify the outputs
    output1_df = transform_runner.sink.saved_data["test_output1"]
    output2_df = transform_runner.sink.saved_data["test_output2"]
    
    assert output1_df.count() == 3  # Even numbers
    assert output2_df.count() == 2  # Odd numbers
    
    even_values = [row["value"] for row in output1_df.collect()]
    odd_values = [row["value"] for row in output2_df.collect()]
    
    assert all(v % 2 == 0 for v in even_values)
    assert all(v % 2 == 1 for v in odd_values)

@pytest.mark.asyncio
async def test_dry_run(transform_runner: TransformRunner) -> None:
    transform = SimpleTransform()
    sources = await transform_runner.download_datasets(transform, omit_checks=True, dry_run=True)
    transform_runner.exec_transform(transform, omit_checks=True, dry_run=True, sources=sources)
    
    # Verify no data was saved
    assert len(transform_runner.sink.saved_data) == 0
    assert len(transform_runner.sink.saved_incremental) == 0 
import logging
from typing import Generator, TYPE_CHECKING

import pytest

from transforms.engine.duckdb import init_sess

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

@pytest.fixture(autouse=True)
def setup_logging():
    """Configure logging for tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

@pytest.fixture(scope="session")
def spark() -> "SparkSession":
    """Shared Spark-compatible session backed by DuckDB for all tests."""
    return init_sess()


@pytest.fixture(autouse=True)
def cleanup_spark(spark: "SparkSession") -> Generator[None, None, None]:
    """Cleanup Spark tables and views after each test."""
    yield
    for view in spark.catalog.listTables():
        spark.catalog.dropTempView(view.name)
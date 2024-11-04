from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
def init_sess()->SparkSession:
    from sqlframe import activate
    activate(engine="duckdb")
    from pyspark.sql import SparkSession

    sess: SparkSession = SparkSession.builder.appName("test").getOrCreate()  # type: ignore
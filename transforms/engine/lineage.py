# pyright: reportMissingImports=false, reportUnknownVariableType=false

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def init_sess() -> "SparkSession":
    """
    Initialize a Spark-free stub session that records expression/column lineage.
    """
    from pyspark_lineage_stub.sql.session import SparkSession  # type: ignore[import-untyped]

    sess = cast("SparkSession", SparkSession.builder.getOrCreate())
    return sess


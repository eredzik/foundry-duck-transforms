from __future__ import annotations

from pyspark.sql import DataFrame

from transforms.runner.data_sink.base import DataSink


class NoOpDataSink(DataSink):
    """
    Used in lineage mode to avoid writing any data.
    """

    def save_transaction(self, df: DataFrame, dataset_path_or_rid: str) -> None:  # noqa: ARG002
        return


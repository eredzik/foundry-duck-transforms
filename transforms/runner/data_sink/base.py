from abc import ABC

from pyspark.sql import DataFrame


class DataSink(ABC):

    def __init__(self):
        pass   

    def save_transaction(
        self,
        df: DataFrame,
        dataset_path_or_rid: str,
    ) -> None:
        raise NotImplementedError()

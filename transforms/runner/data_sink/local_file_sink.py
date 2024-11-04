from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame

from transforms.runner.data_sink.base import DataSink


@dataclass
class LocalFileSink(DataSink):
    output_dir: str | None =  str((Path.home() / ".fndry_duck" / "local_output"))
    

    def save_transaction(
        self,
        df: DataFrame,
        dataset_path_or_rid: str,
        branch: str,
    ) -> None:
        df.write.parquet(
            f"{self.output_dir}/{branch}/{dataset_path_or_rid}", mode="overwrite"
        )

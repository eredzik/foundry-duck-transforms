from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame

from transforms.runner.data_sink.base import DataSink


@dataclass
class LocalFileSink(DataSink):
    branch: str
    output_dir: str  =  str((Path.home() / ".fndry_duck" / "local_output"))

    def save_transaction(
        self,
        df: DataFrame,
        dataset_path_or_rid: str,
    ) -> None:
        result_path = Path(self.output_dir) / self.branch / dataset_path_or_rid
        result_path.mkdir(parents=True, exist_ok=True)
        df.write.parquet(
            str(result_path), mode="overwrite", compression="zstd"
        )

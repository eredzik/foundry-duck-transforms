from dataclasses import dataclass

from foundry_dev_tools import FoundryContext
from foundry_dev_tools.utils.caches.spark_caches import (
    _infer_dataset_format,
    _validate_cache_key,
)
from pyspark.sql import SparkSession

from transforms.runner.data_source.base import DataSource


@dataclass
class FoundrySource(DataSource):
    ctx: FoundryContext
    session: SparkSession
    def download_dataset(self, dataset_path_or_rid: str, branch: str):
        _, dataset_identity = self.ctx.cached_foundry_client.fetch_dataset(dataset_path_or_rid, branch)
        _validate_cache_key(dataset_identity)
        try:
            inferred_format = _infer_dataset_format(self.ctx.cached_foundry_client.cache.get_cache_dir(), dataset_identity)
            path = self.ctx.cached_foundry_client.cache._get_storage_location(dataset_identity, inferred_format)
            if inferred_format == "parquet":
                return 
                
            return self.session.read.parquet(str(path.joinpath("spark", "*")))
        except FileNotFoundError as exc:
            msg = f"{dataset_identity}"
            raise KeyError(msg) from exc

        

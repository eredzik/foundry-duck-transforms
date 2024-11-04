from dataclasses import dataclass

from foundry_dev_tools import FoundryContext
from foundry_dev_tools.errors.dataset import BranchNotFoundError
from foundry_dev_tools.utils.caches.spark_caches import (
    _infer_dataset_format,
    _validate_cache_key,
)
from pyspark.sql import DataFrame, SparkSession

from transforms.runner.data_source.base import (
    BranchNotFoundError as BranchNotFoundErrorBase,
)
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
                return self.session.read.parquet(str(path.joinpath("spark", "*")))
            raise NotImplementedError(f"Format {inferred_format} is not supported")
                
        except FileNotFoundError as exc:
            msg = f"{dataset_identity}"
            raise KeyError(msg) from exc

    def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]):
        
            
        for branch in branches:
            try:
                return self.download_dataset(
                    dataset_path_or_rid, branch=branch
                )
                
            except BranchNotFoundError:
                print(
                    f"Branch not found for dataset [{dataset_path_or_rid}]"
                )
                
        raise BranchNotFoundErrorBase()
    
    def get_last_transaction(self, dataset_path_or_rid: str, branches: list[str])->DataFrame:
        
        raise NotImplementedError()
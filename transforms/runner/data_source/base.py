from abc import ABC
from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class DataSource(ABC):
    def download_dataset(
        self,
        dataset_path_or_rid: str,
        branch: str,
    ) -> DataFrame:
        raise NotImplementedError()
    def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]) -> DataFrame:
        raise NotImplementedError()
    
    def get_last_transaction(self, dataset_path_or_rid: str, branches: list[str]) -> DataFrame:
        raise NotImplementedError()
    
class BranchNotFoundError(Exception):
    pass
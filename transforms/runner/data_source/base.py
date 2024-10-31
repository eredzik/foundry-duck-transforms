from abc import ABC
from dataclasses import dataclass

from foundry_dev_tools import FoundryContext


@dataclass
class DataSource(ABC):
    def download_dataset(
        self,
        dataset_path_or_rid: str,
        branch: str,
    ):
        raise NotImplementedError()

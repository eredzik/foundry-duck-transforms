from dataclasses import dataclass

from foundry_dev_tools import FoundryContext

from transforms.runner.data_source.base import DataSource


@dataclass
class FoundrySource(DataSource):
    ctx: FoundryContext

    def download_dataset(self, dataset_path_or_rid: str, branch: str):
        return self.ctx.cached_foundry_client.load_dataset(
            dataset_path_or_rid=dataset_path_or_rid,
            branch=branch,
        )

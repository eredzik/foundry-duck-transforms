from dataclasses import dataclass

from transforms.runner.data_source.base import BranchNotFoundError, DataSource


@dataclass
class MixedDataSource(DataSource):
    sources: dict[str, DataSource] 
    fallback_source: DataSource | None
    
    def download_dataset(self, dataset_path_or_rid: str, branch: str):
        source = self.sources.get(branch)
        if source is None:
            if self.fallback_source is None:
                raise BranchNotFoundError()
            source = self.fallback_source
        return source.download_dataset(dataset_path_or_rid, branch=branch)

        

    def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]):
        for branch in branches:
            try:
                return self.download_dataset(dataset_path_or_rid, branch=branch)

            except BranchNotFoundError:
                print(f"Branch not found for dataset [{dataset_path_or_rid}]")
        raise BranchNotFoundError()

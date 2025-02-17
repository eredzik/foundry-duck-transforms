from dataclasses import dataclass

from transforms.runner.data_source.base import BranchNotFoundError, DataSource
import logging
logger = logging.getLogger(__name__)

@dataclass
class MixedDataSource(DataSource):
    sources: dict[str, DataSource] 
    fallback_source: DataSource | None
    
    async def download_dataset(self, dataset_path_or_rid: str, branch: str):
        source = self.sources.get(branch)
        if source is None:
            if self.fallback_source is None:
                raise BranchNotFoundError('SOURCE')
            source = self.fallback_source
        return await source.download_dataset(dataset_path_or_rid, branch=branch)

        

    async def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]):
        for branch in branches:
            try:
                return await self.download_dataset(dataset_path_or_rid, branch=branch)

            except BranchNotFoundError as e:
                logger.info(f"[{e.source}] Branch [{branch}] not found for dataset [{dataset_path_or_rid}]")
        raise BranchNotFoundError(source="MIXED")

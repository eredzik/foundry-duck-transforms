from dataclasses import dataclass

from transforms.runner.data_source.base import BranchNotFoundError, DataSource
from transforms.runner.dataset_logging import dataset_display_name
import logging

logger = logging.getLogger(__name__)


@dataclass
class MixedDataSource(DataSource):
    sources: dict[str, DataSource]
    fallback_source: DataSource | None

    def resolve_dataset_label(self, dataset_path_or_rid: str, branch: str = "master") -> str:
        if self.fallback_source and hasattr(
            self.fallback_source, "resolve_dataset_label"
        ):
            return self.fallback_source.resolve_dataset_label(
                dataset_path_or_rid, branch
            )
        return dataset_display_name(dataset_path_or_rid)

    async def download_dataset(self, dataset_path_or_rid: str, branch: str):
        source = self.sources.get(branch)
        if source is None:
            if self.fallback_source is None:
                raise BranchNotFoundError("SOURCE")
            source = self.fallback_source
        return await source.download_dataset(dataset_path_or_rid, branch=branch)

    async def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]):
        label = self.resolve_dataset_label(dataset_path_or_rid, branches[-1])
        logger.info(
            "Resolving dataset %s across branches: %s",
            label,
            ", ".join(branches),
        )
        for branch in branches:
            try:
                return await self.download_dataset(dataset_path_or_rid, branch=branch)

            except BranchNotFoundError as e:
                logger.info(
                    "[%s] Branch [%s] not found for dataset %s",
                    e.source,
                    branch,
                    label,
                )
        raise BranchNotFoundError(source="MIXED")

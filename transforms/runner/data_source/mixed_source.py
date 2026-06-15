from dataclasses import dataclass

from transforms.runner.data_source.base import BranchNotFoundError, DataSource
from transforms.runner.data_source.download_result import DownloadMetadata
from transforms.runner.dataset_logging import dataset_display_name
from transforms.runner.progress import get_current_task_id, get_progress
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
        progress = get_progress()
        task_id = get_current_task_id()

        if progress is None:
            logger.info(
                "Resolving dataset %s across branches: %s",
                label,
                ", ".join(branches),
            )

        for branch in branches:
            if progress is not None and task_id is not None:
                progress.update_task(
                    task_id,
                    phase="trying branch",
                    branch=branch,
                )
            try:
                return await self.download_dataset(dataset_path_or_rid, branch=branch)

            except BranchNotFoundError as e:
                if progress is not None and task_id is not None:
                    progress.update_task(
                        task_id,
                        phase=f"branch not found ({e.source})",
                        branch=branch,
                    )
                elif progress is None:
                    logger.info(
                        "[%s] Branch [%s] not found for dataset %s",
                        e.source,
                        branch,
                        label,
                    )
        raise BranchNotFoundError(source="MIXED")

    def register_duckdb_views(self, metadata_list: list[DownloadMetadata]) -> None:
        if self.fallback_source is not None and hasattr(
            self.fallback_source, "register_duckdb_views"
        ):
            self.fallback_source.register_duckdb_views(metadata_list)

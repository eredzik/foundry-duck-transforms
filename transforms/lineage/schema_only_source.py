from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from transforms.runner.data_source.base import DataSource


@dataclass
class SchemaOnlyDataSource(DataSource):
    """
    Wrap a real DataSource and return stub DataFrames with matching column names.

    The upstream DataSource is used only to infer column names (schema). Returned
    DataFrames are created from the lineage-mode SparkSession.
    """

    upstream: DataSource
    lineage_session: Any
    fallback_branches: list[str]
    _cache: Optional[dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self._cache is None:
            self._cache = {}

    def register_output(self, dataset_path_or_rid: str, df: Any) -> None:
        assert self._cache is not None
        self._cache[dataset_path_or_rid] = df

    async def download_dataset(self, dataset_path_or_rid: str, branch: str):
        assert self._cache is not None
        cached = self._cache.get(dataset_path_or_rid)
        if cached is not None:
            return cached
        df = await self.upstream.download_dataset(dataset_path_or_rid, branch=branch)
        cols = getattr(df, "columns", None)
        if cols is None:
            raise ValueError(f"Upstream DataFrame for {dataset_path_or_rid} has no .columns")
        return self.lineage_session.createDataFrame([], schema=list(cols))

    async def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]):
        assert self._cache is not None
        cached = self._cache.get(dataset_path_or_rid)
        if cached is not None:
            return cached
        df = await self.upstream.download_for_branches(dataset_path_or_rid, branches=branches)
        cols = getattr(df, "columns", None)
        if cols is None:
            raise ValueError(f"Upstream DataFrame for {dataset_path_or_rid} has no .columns")
        return self.lineage_session.createDataFrame([], schema=list(cols))


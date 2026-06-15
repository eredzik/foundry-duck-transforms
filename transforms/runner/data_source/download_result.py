from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class DownloadMetadata:
    dataset_path_or_rid: str
    branch: str
    last_path: str
    dataset_name: str | None = None
    is_query_cache: bool = False


@dataclass
class DownloadResult:
    df: DataFrame
    metadata: DownloadMetadata | None = None

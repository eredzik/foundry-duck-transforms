from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest


pytest.importorskip("pyspark_lineage_stub")


from transforms.api import Input, Output, transform_df  # noqa: E402
from transforms.lineage.collector import GlobalLineageCollector  # noqa: E402
from transforms.lineage.patch_pyspark_imports import patch_pyspark_imports_for_lineage  # noqa: E402
from transforms.lineage.schema_only_source import SchemaOnlyDataSource  # noqa: E402
from transforms.lineage.sink import NoOpDataSink  # noqa: E402
from transforms.runner.exec_transform import TransformRunner  # noqa: E402


@dataclass
class _UpstreamDF:
    columns: list[str]


class _UpstreamSource:
    async def download_dataset(self, dataset_path_or_rid: str, branch: str):
        return _UpstreamDF(columns=["a", "b"])

    async def download_for_branches(self, dataset_path_or_rid: str, branches: list[str]):
        return _UpstreamDF(columns=["a", "b"])


@transform_df(output=Output("out_ds"), inp=Input("in_ds"))
def _t(inp):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    return inp.select((F.col("a") + 1).alias("x")).withColumn("y", F.col("x") * 2)


def test_global_collector_records_ops_and_columns(tmp_path: Path) -> None:
    patch_pyspark_imports_for_lineage()

    from transforms.engine.lineage import init_sess

    lineage_session = init_sess()

    sourcer = SchemaOnlyDataSource(
        upstream=_UpstreamSource(),  # type: ignore[arg-type]
        lineage_session=lineage_session,
        fallback_branches=["dev"],
    )
    runner = TransformRunner(sourcer=sourcer, sink=NoOpDataSink(), fallback_branches=["dev"])

    collector = GlobalLineageCollector()
    runner.exec_transform(_t, omit_checks=True, dry_run=True, lineage_collector=collector, lineage_mode=True)

    out = collector.to_dict()
    assert "datasets" in out
    assert "out_ds" in out["datasets"]
    assert out["datasets"]["out_ds"]["columns"] == ["x", "y"]
    assert out["datasets"]["out_ds"]["graph"]["ops"]


def test_collector_writes_json(tmp_path: Path) -> None:
    collector = GlobalLineageCollector()
    collector._datasets["ds"] = {"columns": ["a"], "graph": {"ops": [1]}}  # type: ignore[attr-defined]
    collector.record_transform(transform_id="t0", name="compute", inputs=["in"], outputs=["out"])
    out_path = tmp_path / "lineage.json"
    collector.write_json(out_path)
    assert out_path.exists()
    assert out_path.read_text().strip().startswith("{")


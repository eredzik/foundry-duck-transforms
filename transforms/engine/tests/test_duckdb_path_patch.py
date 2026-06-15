from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from transforms.engine.duckdb import (
    _duckdb_sql_path,
    _duckdb_sql_paths,
    _patch_sqlframe_duckdb_paths,
)


def test_duckdb_sql_path_leaves_posix_unchanged() -> None:
    assert _duckdb_sql_path("/foo/bar/*.parquet") == "/foo/bar/*.parquet"


def test_duckdb_sql_paths_handles_lists() -> None:
    assert _duckdb_sql_paths(["/a.parquet", "/b.parquet"]) == ["/a.parquet", "/b.parquet"]
    assert _duckdb_sql_paths(None) is None


def test_reader_load_normalizes_paths_before_sqlframe() -> None:
    from sqlframe.duckdb.readwriter import DuckDBDataFrameReader

    captured: dict[str, Any] = {}

    def fake_load(
        self: Any,
        path: Any = None,
        format: Any = None,
        schema: Any = None,
        **options: Any,
    ):
        captured["path"] = path
        return "ok"

    DuckDBDataFrameReader.load = fake_load  # type: ignore[method-assign]
    _patch_sqlframe_duckdb_paths()

    reader = DuckDBDataFrameReader(MagicMock())
    reader.load(r"\tmp\data\*.parquet", format="parquet")
    assert captured["path"] == Path(r"\tmp\data\*.parquet").as_posix()


def test_writer_write_normalizes_paths_before_sqlframe() -> None:
    from sqlframe.duckdb.readwriter import DuckDBDataFrameWriter

    captured: dict[str, Any] = {}

    def fake_write(self: Any, path: str, mode: Any = None, **options: Any):
        captured["path"] = path

    DuckDBDataFrameWriter._write = fake_write  # type: ignore[method-assign]
    _patch_sqlframe_duckdb_paths()

    writer = DuckDBDataFrameWriter(MagicMock())
    writer._write(r"\tmp\out.parquet", mode="overwrite")
    assert captured["path"] == Path(r"\tmp\out.parquet").as_posix()

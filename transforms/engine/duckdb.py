from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _duckdb_sql_path(path: str) -> str:
    """Normalize a filesystem path for embedding in DuckDB SQL strings."""
    return Path(path).as_posix()


def _duckdb_sql_paths(path: Any) -> Any:
    if path is None:
        return None
    if isinstance(path, (list, tuple)):
        return type(path)(_duckdb_sql_path(p) for p in path)
    return _duckdb_sql_path(path)


def _patch_sqlframe_duckdb_paths() -> None:
    """
    Normalize filesystem paths to POSIX before SQLFrame embeds them in DuckDB SQL.

    On Windows, backslashes in paths (e.g. C:\\Users\\...) are interpreted as escape
    sequences inside SQL string literals (\\r -> carriage return).
    """
    try:
        from sqlframe.duckdb.readwriter import (  # type: ignore[import]
            DuckDBDataFrameReader,
            DuckDBDataFrameWriter,
        )
    except Exception:
        return

    if getattr(DuckDBDataFrameReader.load, "_duckdb_path_patch", False):
        return

    _orig_load = DuckDBDataFrameReader.load
    _orig_write = DuckDBDataFrameWriter._write

    def load(
        self: Any,
        path: Any = None,
        format: Any = None,
        schema: Any = None,
        **options: Any,
    ):
        return _orig_load(
            self,
            path=_duckdb_sql_paths(path),
            format=format,
            schema=schema,
            **options,
        )

    load._duckdb_path_patch = True  # type: ignore[attr-defined]

    def _write(self: Any, path: str, mode: Any = None, **options: Any):
        return _orig_write(self, _duckdb_sql_path(path), mode, **options)

    _write._duckdb_path_patch = True  # type: ignore[attr-defined]

    DuckDBDataFrameReader.load = load  # type: ignore[method-assign]
    DuckDBDataFrameWriter._write = _write  # type: ignore[method-assign]


def _patch_sqlframe_writer_save() -> None:
    """
    Runtime patch for SQLFrame DuckDB writer to support df.write.format(...).save(...)
    and df.write.save(...).
    """
    try:
        from sqlframe.duckdb.readwriter import DuckDBDataFrameWriter  # type: ignore[import]
        from sqlframe.base.readerwriter import _infer_format  # type: ignore[import]
    except Exception:
        # If sqlframe is not available, or the import shape changes, fail gracefully.
        return

    if hasattr(DuckDBDataFrameWriter, "save"):
        # Newer sqlframe versions might already support this API; nothing to do.
        return

    def save(
        self: Any,
        path: str,
        format: str | None = None,
        mode: str | None = None,
        **options: Any,
    ) -> None:
        posix_path = _duckdb_sql_path(path)
        # Prefer explicit format, then writer.state_format_to_write, then infer from path.
        fmt = format or getattr(self, "_state_format_to_write", None)
        if fmt is None:
            fmt = _infer_format(posix_path)

        effective_mode = mode or getattr(self, "_mode", None)

        # Ensure parent directories exist to avoid DuckDB IO errors when opening the file.
        p = Path(posix_path).expanduser()
        if str(p).endswith("spark"):
            p = p / "data.parquet"

        parent = p.parent

        parent.mkdir(parents=True, exist_ok=True)

        # Delegate to the engine-specific _write implementation
        self._write(path=posix_path, mode=effective_mode, format=fmt, **options)  # type: ignore[call-arg]

    setattr(DuckDBDataFrameWriter, "save", save)


def init_sess() -> "SparkSession":
    """Initialize a DuckDB session that mimics the SparkSession interface."""
    from sqlframe import activate

    activate(engine="duckdb")
    _patch_sqlframe_duckdb_paths()
    _patch_sqlframe_writer_save()

    from pyspark.sql import SparkSession

    sess = cast("SparkSession", SparkSession.builder.appName("test").getOrCreate())
    return sess

from pathlib import Path
from typing import TYPE_CHECKING, cast, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


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
        # Prefer explicit format, then writer.state_format_to_write, then infer from path.
        fmt = format or getattr(self, "_state_format_to_write", None)
        if fmt is None:
            fmt = _infer_format(path)

        effective_mode = mode or getattr(self, "_mode", None)

        # Ensure parent directories exist to avoid DuckDB IO errors when opening the file.
        p = Path(path).expanduser()
        if str(p).endswith("spark"):
            p = p / 'data.parquet'
        
        parent = p.parent
        
        parent.mkdir(parents=True, exist_ok=True)

        # Delegate to the engine-specific _write implementation
        self._write(path=path, mode=effective_mode, format=fmt, **options)  # type: ignore[call-arg]

    setattr(DuckDBDataFrameWriter, "save", save)


def init_sess() -> "SparkSession":
    """Initialize a DuckDB session that mimics the SparkSession interface."""
    from sqlframe import activate

    activate(engine="duckdb")
    _patch_sqlframe_writer_save()

    from pyspark.sql import SparkSession

    sess = cast("SparkSession", SparkSession.builder.appName("test").getOrCreate())
    return sess
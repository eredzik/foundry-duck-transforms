import io
from contextlib import redirect_stdout
from typing import Any

from pyspark.sql import DataFrame


def _get_sql(df: DataFrame) -> str:
    sql_fn = getattr(df, "sql", None)
    if callable(sql_fn):
        return sql_fn()  # type: ignore[operator]
    writer = getattr(df, "write", None)
    if writer is not None and hasattr(writer, "sql") and callable(writer.sql):
        return writer.sql()
    return "(SQL not available for this DataFrame)"


def _get_explain(df: DataFrame) -> str:
    if not hasattr(df, "explain") or not callable(df.explain):
        return "(EXPLAIN not available for this DataFrame)"
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain()
    return buf.getvalue().strip()


def format_execution_plan(df: Any, *, label: str | None = None) -> str:
    if not hasattr(df, "limit") or not hasattr(df, "collect"):
        raise ValueError("Expected a DataFrame-like object for execution plan")

    parts: list[str] = []
    if label is not None:
        parts.append(f"=== output: {label} ===")
    parts.append("=== SQL ===")
    parts.append(_get_sql(df))
    parts.append("")
    parts.append("=== EXPLAIN ===")
    parts.append(_get_explain(df))
    return "\n".join(parts)

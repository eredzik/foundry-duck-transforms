from typing import Any

from transforms.engine.duckdb import init_sess


def test_duckdb_sqlframe_supports_writer_format_save() -> None:
    """
    Ensure that our runtime patch enables df.write.format(...).save(...)
    and df.write.save(...) (with format inference).

    This exercises the patched API lightly to guard against regressions
    in SQLFrame/DuckDB integration.
    """
    sess = init_sess()

    df = sess.range(0, 3).toDF("id")  # type: ignore[attr-defined]

    # Use a temporary path in the current working directory.
    base_path = "tmp_duckdb_format_save_test"

    # This should not raise for explicit format (overwrite to be idempotent)
    df.write.mode("overwrite").format("parquet").save(f"{base_path}.parquet")  # type: ignore[call-arg]

    # And this should not raise with format inferred from the path (overwrite as well)
    df.write.mode("overwrite").save(f"{base_path}.csv")  # type: ignore[call-arg]


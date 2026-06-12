import importlib
import sys
from types import ModuleType


def _alias_module(src: ModuleType, dst_name: str) -> None:
    sys.modules[dst_name] = src


def patch_pyspark_imports_for_lineage() -> None:
    """
    Redirect common `pyspark.sql.*` imports to `pyspark_lineage_stub.sql.*`.

    This lets user code written against `pyspark.sql` run in lineage mode without Spark/JVM.
    """
    try:
        stub_sql = importlib.import_module("pyspark_lineage_stub.sql")
        stub_root = importlib.import_module("pyspark_lineage_stub")
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "Missing dependency `pyspark-lineage-stub`.\n\n"
            "Install it into your current environment, e.g.:\n"
            "- local dev:  uv pip install -e /ABSOLUTE/PATH/TO/pyspark-lineage-stub\n"
            "- internal:   uv pip install pyspark-lineage-stub\n\n"
            "Then re-run:\n"
            "  python -m transforms.run lineage <module_or_package_path> <fallback_branches>\n"
        ) from e

    # Base package and sql package
    _alias_module(stub_root, "pyspark")
    _alias_module(stub_sql, "pyspark.sql")

    # Common submodules used by user transforms
    for sub in [
        "session",
        "functions",
        "column",
        "dataframe",
        "types",
    ]:
        try:
            mod = importlib.import_module(f"pyspark_lineage_stub.sql.{sub}")
        except Exception:
            continue
        _alias_module(mod, f"pyspark.sql.{sub}")


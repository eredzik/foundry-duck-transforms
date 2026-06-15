from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator

logger = logging.getLogger(__name__)


def dataset_display_name(
    path_or_rid: str,
    dataset_path: str | None = None,
) -> str:
    if dataset_path:
        name = dataset_path.rstrip("/").split("/")[-1]
        return f"{name} [{path_or_rid}]"
    if "/" in path_or_rid and not path_or_rid.startswith("ri."):
        name = path_or_rid.rstrip("/").split("/")[-1]
        return name
    return path_or_rid


def format_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(seconds, 60)
    return f"{int(minutes)}m {secs:.0f}s"


def format_row_count(rows: int | None) -> str:
    if rows is None:
        return "unknown rows"
    return f"{rows:,} rows"


def try_row_count(df: Any) -> int | None:
    if not hasattr(df, "count"):
        return None
    try:
        return df.count()
    except Exception:
        logger.debug("Could not count rows for dataset", exc_info=True)
        return None


@contextmanager
def log_dataset_phase(
    operation: str,
    label: str,
    *,
    log: logging.Logger = logger,
    **start_details: Any,
) -> Iterator[dict[str, Any]]:
    def _format_details(details: dict[str, Any]) -> str:
        return ", ".join(
            f"{key}={value}"
            for key, value in details.items()
            if value is not None
        )

    start_msg = f"Started {operation}: {label}"
    if formatted := _format_details(start_details):
        start_msg = f"{start_msg} ({formatted})"
    log.info(start_msg)

    started = time.perf_counter()
    context: dict[str, Any] = {}
    try:
        yield context
    finally:
        elapsed = time.perf_counter() - started
        end_details = {**start_details, **context, "elapsed": format_elapsed(elapsed)}
        log.info(f"Finished {operation}: {label} ({_format_details(end_details)})")

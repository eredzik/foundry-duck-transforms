from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator

from transforms.runner.progress import get_current_task_id, get_progress, is_progress_active

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


def _format_details(details: dict[str, Any]) -> str:
    return ", ".join(
        f"{key}={value}"
        for key, value in details.items()
        if value is not None
    )


@contextmanager
def log_verbose_step(
    step: str,
    *,
    verbose: bool = False,
    log: logging.Logger = logger,
    **details: Any,
) -> Iterator[None]:
    progress = get_progress()
    task_id = get_current_task_id()
    if is_progress_active() and verbose and progress is not None and task_id is not None:
        progress.update_task(task_id, phase=step, **details)
        yield
        return

    if not verbose:
        yield
        return

    started = time.perf_counter()
    try:
        yield
    finally:
        elapsed = format_elapsed(time.perf_counter() - started)
        suffix = f" ({_format_details(details)})" if details else ""
        log.info(f"  [{step}] {elapsed}{suffix}")


@contextmanager
def log_dataset_phase(
    operation: str,
    label: str,
    *,
    log: logging.Logger = logger,
    **start_details: Any,
) -> Iterator[dict[str, Any]]:
    progress = get_progress()
    task_id = get_current_task_id()
    use_progress = is_progress_active() and progress is not None and task_id is not None

    if not use_progress:
        start_msg = f"Started {operation}: {label}"
        if formatted := _format_details(start_details):
            start_msg = f"{start_msg} ({formatted})"
        log.info(start_msg)
    else:
        assert progress is not None and task_id is not None
        progress.update_task(task_id, phase=operation, **start_details)

    started = time.perf_counter()
    context: dict[str, Any] = {}
    failed = False
    try:
        yield context
    except Exception as exc:
        failed = True
        if use_progress:
            assert progress is not None and task_id is not None
            progress.fail_task(task_id, error=str(exc))
        raise
    finally:
        if use_progress and not failed:
            assert progress is not None and task_id is not None
            elapsed = format_elapsed(time.perf_counter() - started)
            end_details = {**start_details, **context}
            progress.update_task(
                task_id,
                phase=f"{operation} ({elapsed})",
                **end_details,
            )
        elif not use_progress and not failed:
            elapsed = time.perf_counter() - started
            end_details = {**start_details, **context, "elapsed": format_elapsed(elapsed)}
            log.info(f"Finished {operation}: {label} ({_format_details(end_details)})")

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar, Token
from enum import Enum
from typing import Any, Iterator

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)

logger = logging.getLogger(__name__)

current_progress: ContextVar[StartupProgress | None] = ContextVar(
    "current_progress", default=None
)
current_task_id: ContextVar[TaskID | None] = ContextVar("current_task_id", default=None)


def _format_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(seconds, 60)
    return f"{int(minutes)}m {secs:.0f}s"


class UiMode(str, Enum):
    plain = "plain"
    progress = "progress"


def is_progress_active() -> bool:
    progress = current_progress.get()
    return progress is not None and progress.active


def get_progress() -> StartupProgress | None:
    return current_progress.get()


def get_current_task_id() -> TaskID | None:
    return current_task_id.get()


class StartupProgress:
    def __init__(self, console: Console | None = None) -> None:
        self._console = console or Console(stderr=True)
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.fields[label]:<24}"),
            BarColumn(bar_width=30),
            TextColumn("{task.fields[status]}"),
            TimeElapsedColumn(),
            console=self._console,
            transient=False,
        )
        self._phase_tasks: dict[str, TaskID] = {}
        self._input_tasks: dict[str, TaskID] = {}
        self._task_started: dict[TaskID, float] = {}
        self._token: Token[StartupProgress | None] | None = None
        self.active = False

    def __enter__(self) -> StartupProgress:
        self._progress.start()
        self.active = True
        self._token = current_progress.set(self)
        return self

    def __exit__(self, *args: Any) -> None:
        self.active = False
        self._progress.stop()
        if self._token is not None:
            current_progress.reset(self._token)

    def _status_text(self, phase: str, details: dict[str, Any]) -> str:
        parts = [phase]
        if branch := details.get("branch"):
            parts.append(str(branch))
        if source := details.get("source"):
            parts.append(str(source))
        if extra := details.get("detail"):
            parts.append(str(extra))
        return " · ".join(parts)

    def start_phase(self, name: str, label: str | None = None) -> TaskID:
        display = label or name
        task_id = self._progress.add_task(
            display,
            total=1,
            label=display,
            status="running",
        )
        self._phase_tasks[name] = task_id
        self._task_started[task_id] = time.perf_counter()
        return task_id

    def complete_phase(self, name: str, **details: Any) -> None:
        task_id = self._phase_tasks.get(name)
        if task_id is None:
            return
        elapsed = time.perf_counter() - self._task_started.get(task_id, time.perf_counter())
        status = self._status_text(f"done {_format_elapsed(elapsed)}", details)
        self._progress.update(task_id, completed=1, status=status)

    def fail_phase(self, name: str, error: str) -> None:
        task_id = self._phase_tasks.get(name)
        if task_id is None:
            return
        self._progress.update(task_id, status=f"[red]failed · {error}")

    def start_input_task(self, argname: str, label: str) -> TaskID:
        task_id = self._progress.add_task(
            argname,
            total=1,
            label=label,
            status="starting",
        )
        self._input_tasks[argname] = task_id
        self._task_started[task_id] = time.perf_counter()
        return task_id

    def set_current_task(self, task_id: TaskID) -> Token[TaskID | None]:
        return current_task_id.set(task_id)

    def reset_current_task(self, token: Token[TaskID | None]) -> None:
        current_task_id.reset(token)

    def update_task(
        self,
        task_id: TaskID | None = None,
        phase: str = "",
        **details: Any,
    ) -> None:
        resolved = task_id if task_id is not None else get_current_task_id()
        if resolved is None:
            return
        status = self._status_text(phase, details) if phase else details.get("status", "")
        self._progress.update(resolved, status=status, **{k: v for k, v in details.items() if k in ("label",)})

    def complete_task(self, task_id: TaskID | None = None, **details: Any) -> None:
        resolved = task_id if task_id is not None else get_current_task_id()
        if resolved is None:
            return
        elapsed = time.perf_counter() - self._task_started.get(resolved, time.perf_counter())
        phase = details.pop("phase", f"done {_format_elapsed(elapsed)}")
        status = self._status_text(phase, details)
        self._progress.update(resolved, completed=1, status=status)

    def fail_task(self, task_id: TaskID | None = None, error: str = "error") -> None:
        resolved = task_id if task_id is not None else get_current_task_id()
        if resolved is None:
            return
        self._progress.update(resolved, status=f"[red]failed · {error}")


def configure_progress_logging() -> None:
    logging.getLogger().setLevel(logging.WARNING)


def restore_plain_logging() -> None:
    logging.getLogger().setLevel(logging.INFO)


@contextmanager
def progress_context(mode: UiMode) -> Iterator[StartupProgress | None]:
    if mode is UiMode.plain:
        yield None
        return

    if not sys.stdout.isatty():
        logger.warning(
            "Progress UI requires a TTY; falling back to plain logging. "
            "Pipe output to a terminal or use --ui plain."
        )
        yield None
        return

    configure_progress_logging()
    try:
        with StartupProgress() as progress:
            yield progress
    finally:
        restore_plain_logging()

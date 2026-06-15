import asyncio
import logging
from io import StringIO
from unittest.mock import patch

import pytest
from rich.console import Console

from transforms.runner.data_source.base import BranchNotFoundError
from transforms.runner.data_source.download_result import DownloadMetadata, DownloadResult
from transforms.runner.dataset_logging import log_dataset_phase, log_verbose_step
from transforms.runner.progress import StartupProgress, UiMode, is_progress_active, progress_context


@pytest.fixture
def progress_console() -> Console:
    return Console(file=StringIO(), force_terminal=True, width=120)


def test_plain_mode_unchanged(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO):
        with log_dataset_phase("download", "my_dataset", branch="dev"):
            pass
    assert "Started download: my_dataset" in caplog.text
    assert "Finished download: my_dataset" in caplog.text
    assert not is_progress_active()


def test_log_verbose_step_skips_without_verbose(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO):
        with log_verbose_step("test step", verbose=False):
            pass
    assert caplog.text == ""


def test_progress_context_plain_yields_none() -> None:
    with progress_context(UiMode.plain) as progress:
        assert progress is None
        assert not is_progress_active()


def test_progress_context_non_tty_falls_back(caplog: pytest.LogCaptureFixture) -> None:
    with patch("sys.stdout.isatty", return_value=False):
        with caplog.at_level(logging.WARNING):
            with progress_context(UiMode.progress) as progress:
                assert progress is None
    assert "falling back to plain logging" in caplog.text


def test_log_dataset_phase_updates_task_not_logs(
    progress_console: Console,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO):
        with StartupProgress(console=progress_console) as progress:
            task_id = progress.start_input_task("df1", "my_dataset")
            token = progress.set_current_task(task_id)
            try:
                with log_dataset_phase("download", "my_dataset", branch="dev"):
                    pass
            finally:
                progress.reset_current_task(token)

    assert "Started download" not in caplog.text
    assert "Finished download" not in caplog.text
    output = progress_console.file.getvalue()
    assert "my_dataset" in output
    assert "download" in output


def test_log_verbose_step_updates_task_when_verbose(
    progress_console: Console,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO):
        with StartupProgress(console=progress_console) as progress:
            task_id = progress.start_input_task("df1", "my_dataset")
            token = progress.set_current_task(task_id)
            try:
                with log_verbose_step("online identity", verbose=True, branch="dev"):
                    pass
            finally:
                progress.reset_current_task(token)

    assert "online identity" not in caplog.text
    output = progress_console.file.getvalue()
    assert "online identity" in output


def test_startup_progress_phase_lifecycle(progress_console: Console) -> None:
    with StartupProgress(console=progress_console) as progress:
        progress.start_phase("engine", "Engine init")
        progress.complete_phase("engine", engine="duckdb")

    output = progress_console.file.getvalue()
    assert "Engine init" in output
    assert "done" in output


def test_concurrent_input_tasks_isolated(progress_console: Console) -> None:
    async def download_one(
        progress: StartupProgress,
        argname: str,
        label: str,
        phase: str,
        delay: float,
    ) -> str:
        task_id = progress.start_input_task(argname, label)
        token = progress.set_current_task(task_id)
        try:
            progress.update_task(task_id, phase="starting")
            await asyncio.sleep(delay)
            progress.update_task(task_id, phase=phase)
            progress.complete_task(task_id, phase="loaded")
            return phase
        finally:
            progress.reset_current_task(token)

    async def run() -> list[str]:
        with StartupProgress(console=progress_console) as progress:
            return list(
                await asyncio.gather(
                    download_one(progress, "df_a", "dataset_a", "cache read", 0.05),
                    download_one(progress, "df_b", "dataset_b", "downloading", 0.02),
                    download_one(progress, "df_c", "dataset_c", "fetch files", 0.08),
                )
            )

    results = asyncio.run(run())
    assert sorted(results) == ["cache read", "downloading", "fetch files"]
    output = progress_console.file.getvalue()
    assert "dataset_a" in output
    assert "dataset_b" in output
    assert "dataset_c" in output


def test_fail_task_on_exception(progress_console: Console) -> None:
    with pytest.raises(ValueError, match="boom"):
        with StartupProgress(console=progress_console) as progress:
            task_id = progress.start_input_task("df1", "my_dataset")
            token = progress.set_current_task(task_id)
            try:
                with log_dataset_phase("download", "my_dataset"):
                    raise ValueError("boom")
            finally:
                progress.reset_current_task(token)

    output = progress_console.file.getvalue()
    assert "boom" in output or "failed" in output


class BranchTrackingSource:
    def __init__(self, available_branch: str) -> None:
        self.available_branch = available_branch

    async def download_dataset(
        self, dataset_path_or_rid: str, branch: str
    ) -> DownloadResult:
        if branch != self.available_branch:
            raise BranchNotFoundError("TEST")
        return DownloadResult(
            df=None,
            metadata=DownloadMetadata(
                dataset_path_or_rid=dataset_path_or_rid,
                branch=branch,
                last_path="/cache/test.parquet",
            ),
        )

    async def download_for_branches(
        self, dataset_path_or_rid: str, branches: list[str]
    ):
        from transforms.runner.data_source.mixed_source import MixedDataSource

        source = MixedDataSource(
            sources={self.available_branch: self},
            fallback_source=None,
        )
        return await source.download_for_branches(dataset_path_or_rid, branches)


@pytest.mark.asyncio
async def test_mixed_source_branch_updates_in_progress(progress_console: Console) -> None:
    from transforms.runner.data_source.mixed_source import MixedDataSource

    local_source = BranchTrackingSource("dev")

    with StartupProgress(console=progress_console) as progress:
        task_id = progress.start_input_task("df1", "my_dataset")
        token = progress.set_current_task(task_id)
        try:
            mixed = MixedDataSource(
                sources={"dev": local_source},
                fallback_source=None,
            )
            result = await mixed.download_for_branches(
                "ri.foundry.main.dataset.test",
                ["duck-fndry-dev", "dev"],
            )
            assert result.metadata is not None
            assert result.metadata.branch == "dev"
        finally:
            progress.reset_current_task(token)

    output = progress_console.file.getvalue()
    assert "trying branch" in output or "branch not found" in output

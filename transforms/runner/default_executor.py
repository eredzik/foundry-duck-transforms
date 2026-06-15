import importlib.util
import logging
import sys

from typing import Any

from foundry_dev_tools import FoundryContext
from pyspark.sql import SparkSession

from transforms.config import get_settings
from transforms.api.transform_df import Transform
from transforms.runner.dataset_logging import log_verbose_step
from transforms.runner.exec_transform import TransformRunner
from transforms.runner.progress import get_progress

from .data_sink.local_file_sink_with_duck import LocalFileSinkWithDuck
from .data_source.base import DataSource
from .data_source.foundry_source_with_duck import FoundrySourceWithDuck
from .data_source.local_file_source import LocalDataSource
from .data_source.mixed_source import MixedDataSource

logger = logging.getLogger(__name__)


def execute_with_default_foundry(
    transform_to_run: str,
    fallback_branches: str,
    omit_checks: bool,
    session: SparkSession,
    dry_run: bool,
    local_dev_branch_name: str,
    transform_name: str | None = None,
    verbose: bool = False,
):
    progress = get_progress()

    if progress is not None:
        progress.start_phase("import", "Import transform")
    with log_verbose_step(
        "import transform",
        verbose=verbose,
        log=logger,
        path=transform_to_run,
    ):
        mod = import_from_path("transform", transform_to_run)
    if progress is not None:
        progress.complete_phase("import")

    transforms: dict[str, Transform | Any] = {}
    for name, item in mod.__dict__.items():
        if isinstance(item, Transform):
            transforms[name] = item

    if not transforms:
        print("file has no transforms")
        return

    if transform_name is not None:
        if transform_name not in transforms:
            print(
                f"Transform '{transform_name}' not found in module. "
                f"Available transforms: {list(transforms.keys())}"
            )
            return
        selected_transform: Transform = transforms[transform_name]  # type: ignore[assignment]
    else:
        if len(transforms) > 1:
            print("There is more than one transform specified. Please specify its name.")
            print("names are", list(transforms.keys()))
            return
        selected_transform = list(transforms.values())[0]  # type: ignore[assignment]

    branches = fallback_branches.split(",")
    all_branches = [local_dev_branch_name] + branches

    settings = get_settings()

    if progress is not None:
        progress.start_phase("context", "FoundryContext")
    with log_verbose_step("FoundryContext", verbose=verbose, log=logger):
        foundry_ctx = FoundryContext()
    if progress is not None:
        progress.complete_phase("context")

    if progress is not None:
        progress.start_phase("setup", "Setup runner")
    with log_verbose_step("setup runner", verbose=verbose, log=logger):
        foundry_source = FoundrySourceWithDuck(
            ctx=foundry_ctx,
            session=session,
            settings=settings,
            verbose=verbose,
        )
        local_source = LocalDataSource(session=session, verbose=verbose)
        sources_mapping: dict[str, DataSource] = {b: foundry_source for b in branches}
        sources_mapping[local_dev_branch_name] = local_source

        runner = TransformRunner(
            sink=LocalFileSinkWithDuck(
                branch=local_dev_branch_name,
                resolve_dataset_label=foundry_source.resolve_dataset_label,
                verbose=verbose,
            ),
            sourcer=MixedDataSource(
                sources=sources_mapping,
                fallback_source=foundry_source,
            ),
            fallback_branches=all_branches,
            verbose=verbose,
        )
    if progress is not None:
        progress.complete_phase("setup")

    runner.exec_transform(
        selected_transform,
        omit_checks=omit_checks,
        dry_run=dry_run,
    )


def import_from_path(module_name: str, file_path: str):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)  # type: ignore
    sys.modules[module_name] = module  # Register the module in sys.modules
    spec.loader.exec_module(  # type:ignore
        module
    )  # Execute the module in its own namespace
    return module

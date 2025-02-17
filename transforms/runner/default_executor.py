import importlib.util
import sys
from hashlib import sha256
from typing import Any

from foundry_dev_tools import FoundryContext
from pyspark.sql import SparkSession

from transforms.api.transform_df import Transform
from transforms.runner.exec_transform import TransformRunner

from .data_sink.local_file_sink_with_duck import LocalFileSinkWithDuck
from .data_source.base import DataSource
from .data_source.foundry_source_with_duck import FoundrySourceWithDuck
from .data_source.local_file_source import LocalDataSource
from .data_source.mixed_source import MixedDataSource


def execute_with_default_foundry(
    transform_to_run: str,
    fallback_branches: str,
    omit_checks: bool,
    session: SparkSession,
    dry_run: bool,
    local_dev_branch_name: str,
):
    mod = import_from_path("transform", transform_to_run)
    transforms: dict[str, Transform | Any] = {}
    for name, item in mod.__dict__.items():
        if isinstance(item, Transform):
            transforms[name] = item
    if len(transforms) > 1:
        print("There is more than one transform specified. Please specify its name")
        print("names are", list(transforms.keys()))
        return
    if len(transforms) == 0:
        print("file has no transforms")
        return

    branches = fallback_branches.split(",")
    all_branches = [local_dev_branch_name] + branches
    fndry_ctx = FoundryContext()

    def get_dataset_name(dataset_path_or_rid: str) -> str:
        dataset_path = str(fndry_ctx.get_dataset(dataset_path_or_rid).path)
        sha_addon = sha256(dataset_path.encode()).hexdigest()[:2]
        dataset_name = dataset_path.split("/")[-1]
        return f"{dataset_name}_{sha_addon}"

    foundry_source = FoundrySourceWithDuck(
        ctx=FoundryContext(), session=session, get_dataset_dataset_name=get_dataset_name
    )
    local_source = LocalDataSource(session=session)
    sources_mapping: dict[str, DataSource] = {b: foundry_source for b in branches}
    sources_mapping[local_dev_branch_name] = local_source

    TransformRunner(
        sink=LocalFileSinkWithDuck(
            branch=local_dev_branch_name, get_dataset_dataset_name=get_dataset_name
        ),
        sourcer=MixedDataSource(
            sources=sources_mapping,
            fallback_source=foundry_source,
        ),
        fallback_branches=all_branches,
    ).exec_transform(
        list(transforms.values())[0],
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

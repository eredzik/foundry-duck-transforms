import importlib.util
import sys
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Any

import typer
from typing_extensions import Annotated

from .runner.data_sink.local_file_sink_with_duck import LocalFileSinkWithDuck
from .runner.data_source.local_file_source import LocalDataSource
from .runner.data_source.mixed_source import MixedDataSource


class Engine(str, Enum):
    spark = "spark"
    duckdb = "duckdb"


if __name__ == "__main__":
    from foundry_dev_tools import FoundryContext

    from transforms.api.transform_df import Transform
    from transforms.runner.data_source.foundry_source import FoundrySource
    from transforms.runner.exec_transform import TransformRunner

    def import_from_path(module_name: str, file_path: str):
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)  # type: ignore
        sys.modules[module_name] = module  # Register the module in sys.modules
        spec.loader.exec_module(  # type:ignore
            module
        )  # Execute the module in its own namespace
        return module

    def traverse_to_setup_and_add_to_path(module_name: str) -> None:
        parent = Path(module_name).parent
        files = parent.glob("setup.py")
        if len(list(files)) == 0:
            return traverse_to_setup_and_add_to_path(str(parent))
        else:
            sys.path.insert(0, str(parent))
            return

    def main(
        transform_to_run: str,
        fallback_branches: str,
        omit_checks: Annotated[
            bool, typer.Option(help="Disables checks running")
        ] = False,
        engine: Annotated[
            Engine,
            typer.Option(help="Engine to use for the transformation"),
        ] = Engine.spark,
        dry_run: Annotated[
            bool, typer.Option(help="Dry run the transformation")
        ] = False,
        local_dev_branch_name: Annotated[
            str, typer.Option(help="Branch name for local development")
        ] = "duck-fndry-dev",
    ):
        if engine == "duckdb":
            from transforms.engine.duckdb import init_sess

            session = init_sess()
        else:
            from transforms.engine.spark import init_sess

            session = init_sess()

        traverse_to_setup_and_add_to_path(transform_to_run)
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
        fndry_ctx= FoundryContext()
        foundry_source = FoundrySource(ctx=FoundryContext(), session=session)
        local_source = LocalDataSource(session=session)
        def get_dataset_name(dataset_path_or_rid: str) -> str:
            dataset_path = str(fndry_ctx.get_dataset(dataset_path_or_rid).path)
            sha_addon = sha256(dataset_path.encode()).hexdigest()[:2]
            dataset_name = dataset_path.split("/")[-1]
            return f"{dataset_name}_{sha_addon}"
        TransformRunner(
            sink=LocalFileSinkWithDuck(branch=local_dev_branch_name, get_dataset_dataset_name=get_dataset_name),
            sourcer=MixedDataSource(
                sources={b: foundry_source for b in branches},
                fallback_source=local_source,
            ),
            fallback_branches=all_branches,
        ).exec_transform(
            list(transforms.values())[0],
            omit_checks=omit_checks,
            dry_run=dry_run,
        )

    typer.run(main)

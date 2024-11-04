import importlib.util
import sys
from pathlib import Path
from typing import Any, Literal

import typer
from typing_extensions import Annotated

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
            Literal["spark", "duckdb"],
            typer.Option(help="Engine to use for the transformation"),
        ] = "spark",
        dry_run: Annotated[
            bool, typer.Option(help="Dry run the transformation")
        ] = False,
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
        TransformRunner(fallback_branches=branches).exec_transform(
            list(transforms.values())[0],
            data_sourcer=FoundrySource(ctx=FoundryContext(), session=session),
            omit_checks=omit_checks,
            dry_run=dry_run,
        )

    typer.run(main)

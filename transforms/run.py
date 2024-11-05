import sys
from enum import Enum
from pathlib import Path

import typer
from typing_extensions import Annotated


class Engine(str, Enum):
    spark = "spark"
    duckdb = "duckdb"


if __name__ == "__main__":

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
        traverse_to_setup_and_add_to_path(transform_to_run)
        if engine == "duckdb":
            from transforms.engine.duckdb import init_sess

            session = init_sess()
        else:
            from transforms.engine.spark import init_sess

            session = init_sess()
        from .runner.default_executor import execute_with_default_foundry

        execute_with_default_foundry(
            dry_run=dry_run,
            fallback_branches=fallback_branches,
            session=session,
            local_dev_branch_name=local_dev_branch_name,
            omit_checks=omit_checks,
            transform_to_run=transform_to_run
        )

    typer.run(main)

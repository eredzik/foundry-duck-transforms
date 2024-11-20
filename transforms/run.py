import sys
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger(__name__)

class Engine(str, Enum):
    spark = "spark"
    duckdb = "duckdb"
    sparksail = "spark-sail"


def find_path_where_there_is_setup(module_name: str) -> str:
    parent = Path(module_name).parent
    files = parent.glob("setup.py")
    if str(parent) == module_name:
        raise Exception("Root reached, no setup.py found")
    if len(list(files)) == 0:
        return find_path_where_there_is_setup(str(parent))
    else:
        return str(parent)


@contextmanager
def traverse_to_setup_and_add_to_path(module_name: str):
    path = find_path_where_there_is_setup(module_name=module_name)
    sys.path.insert(0, str(path))
    try:
        sys.path.insert(0, str(path))
        yield
    finally:
        sys.path.remove(str(path))


if __name__ == "__main__":

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
        sail_server_url: Annotated[
            Optional[str], typer.Option(help="Sail server url")
        ] = None,
        dry_run: Annotated[
            bool, typer.Option(help="Dry run the transformation")
        ] = False,
        local_dev_branch_name: Annotated[
            str, typer.Option(help="Branch name for local development")
        ] = "duck-fndry-dev",
    ):
        with traverse_to_setup_and_add_to_path(transform_to_run):
            logger.info(f"Starting engine {engine}")
            if engine == engine.duckdb:
                from transforms.engine.duckdb import init_sess

                session = init_sess()
            if engine == Engine.sparksail:
                from transforms.engine.spark_sail import init_sess

                session = init_sess(sail_server_url)
            else:
                from transforms.engine.spark import init_sess

                session = init_sess()
            logger.info(f"Started engine {engine}")
            from .runner.default_executor import execute_with_default_foundry

            execute_with_default_foundry(
                dry_run=dry_run,
                fallback_branches=fallback_branches,
                session=session,
                local_dev_branch_name=local_dev_branch_name,
                omit_checks=omit_checks,
                transform_to_run=transform_to_run,
            )

    typer.run(main)

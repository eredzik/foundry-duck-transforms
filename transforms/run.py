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


app = typer.Typer(add_completion=False)

def _run_transform_file(
    transform_to_run: str,
    fallback_branches: str,
    omit_checks: bool,
    transform_name: str | None,
    engine: Engine,
    sail_server_url: str | None,
    dry_run: bool,
    local_dev_branch_name: str,
) -> None:
    with traverse_to_setup_and_add_to_path(transform_to_run):
        logger.info(f"Starting engine {engine}")
        if engine == Engine.duckdb:
            from transforms.engine.duckdb import init_sess

            session = init_sess()
        elif engine == Engine.sparksail:
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
            transform_name=transform_name,
            session=session,
            local_dev_branch_name=local_dev_branch_name,
            omit_checks=omit_checks,
            transform_to_run=transform_to_run,
        )


@app.callback(invoke_without_command=True)
def main() -> None:
    """
    Entry point.

    We keep backwards compatibility by rewriting argv in `__main__` so that:
    - `python -m transforms.run my_transform.py dev,master` becomes `... run my_transform.py dev,master`
    - `python -m transforms.run lineage ...` continues to work normally
    """
    return


@app.command()
def run(
    transform_to_run: str,
    fallback_branches: str,
    omit_checks: Annotated[bool, typer.Option(help="Disables checks running")] = False,
    transform_name: Annotated[
        Optional[str],
        typer.Option(
            "--transform-name",
            help="Name of the transform function in the module to execute "
            "(required if the module defines multiple transforms)",
        ),
    ] = None,
    engine: Annotated[Engine, typer.Option(help="Engine to use for the transformation")] = Engine.spark,
    sail_server_url: Annotated[Optional[str], typer.Option(help="Sail server url")] = None,
    dry_run: Annotated[bool, typer.Option(help="Dry run the transformation")] = False,
    local_dev_branch_name: Annotated[
        str, typer.Option(help="Branch name for local development")
    ] = "duck-fndry-dev",
) -> None:
    _run_transform_file(
        transform_to_run=transform_to_run,
        fallback_branches=fallback_branches,
        omit_checks=omit_checks,
        transform_name=transform_name,
        engine=engine,
        sail_server_url=sail_server_url,
        dry_run=dry_run,
        local_dev_branch_name=local_dev_branch_name,
    )


@app.command()
def lineage(
    module_or_package: str,
    fallback_branches: str,
    lineage_out: Annotated[
        Path, typer.Option(help="Where to write lineage JSON")
    ] = Path.home() / ".fndry_duck" / "lineage.json",
    ui: Annotated[bool, typer.Option(help="Start the lineage UI server")] = True,
    port: Annotated[int, typer.Option(help="UI server port")] = 3000,
    local_dev_branch_name: Annotated[
        str, typer.Option(help="Branch name for local development")
    ] = "duck-fndry-dev",
) -> None:
    with traverse_to_setup_and_add_to_path(module_or_package):
        from transforms.runner.default_executor import execute_lineage

        execute_lineage(
            module_or_package=module_or_package,
            fallback_branches=fallback_branches,
            local_dev_branch_name=local_dev_branch_name,
            lineage_out=lineage_out,
            ui=ui,
            port=port,
        )


if __name__ == "__main__":
    # Backwards compatibility: if first arg isn't a known command/option,
    # treat it as the legacy `run` invocation.
    known_commands = {"run", "lineage"}
    if len(sys.argv) > 1:
        first = sys.argv[1]
        if not first.startswith("-") and first not in known_commands:
            sys.argv.insert(1, "run")
    app()

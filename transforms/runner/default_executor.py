import importlib.util
import importlib
import sys
import ast
import shutil
import tempfile
from pathlib import Path

from typing import Any

from foundry_dev_tools import FoundryContext
from pyspark.sql import SparkSession

from transforms.api.pipeline import Pipeline
from transforms.api.transform_df import Transform
from transforms.runner.exec_transform import TransformRunner

from .data_sink.local_file_sink_with_duck import LocalFileSinkWithDuck
from .data_source.base import DataSource
from .data_source.foundry_source_with_duck import FoundrySourceWithDuck
from .data_source.local_file_source import LocalDataSource
from .data_source.mixed_source import MixedDataSource


def _rewrite_for_lineage_ids_inplace(path: Path) -> None:
    """
    Best-effort rewrite of Python sources to give DataFrames stable human ids.

    This mirrors the behavior of `pyspark-lineage-stub rewrite/run`:
    - Rewrites imports: pyspark.sql -> pyspark_lineage_stub.sql
    - Wraps assignments with naming.assign("<file>:<var>", <expr>) so dataframe ids become variable names.
    """

    DF_METHODS: set[str] = {
        "createDataFrame",
        "parquet",
        "select",
        "withColumn",
        "drop",
        "filter",
        "where",
        "join",
        "agg",
    }

    def rewrite_text(src: str) -> str:
        rewritten = src.replace("pyspark.sql", "pyspark_lineage_stub.sql")
        try:
            tree = ast.parse(rewritten)
        except SyntaxError:
            return rewritten

        assign_name = "__pls_assign"

        class _Transformer(ast.NodeTransformer):
            def _call_attr(self, call: ast.Call) -> str | None:
                func = call.func
                if isinstance(func, ast.Attribute):
                    return func.attr
                return None

            def visit_Assign(self, node: ast.Assign) -> ast.AST:
                self.generic_visit(node)
                if not isinstance(node.value, ast.Call):
                    return node

                attr = self._call_attr(node.value)
                if attr is not None and attr not in DF_METHODS:
                    return node

                # Case 1: single target name: users = ...
                if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                    var_name = node.targets[0].id
                    node.value = ast.Call(
                        func=ast.Name(id=assign_name, ctx=ast.Load()),
                        args=[ast.Constant(value=var_name), node.value],
                        keywords=[],
                    )
                    return node

                # Case 2: tuple/list unpacking: left, right = call()
                if len(node.targets) == 1 and isinstance(node.targets[0], (ast.Tuple, ast.List)):
                    elts = node.targets[0].elts
                    if all(isinstance(e, ast.Name) for e in elts):
                        tmp_name = "__pls_tmp"
                        tmp_assign = ast.Assign(
                            targets=[ast.Name(id=tmp_name, ctx=ast.Store())], value=node.value
                        )
                        result: list[ast.stmt] = [tmp_assign]
                        for idx, elt in enumerate(elts):
                            assert isinstance(elt, ast.Name)
                            result.append(
                                ast.Assign(
                                    targets=[ast.Name(id=elt.id, ctx=ast.Store())],
                                    value=ast.Call(
                                        func=ast.Name(id=assign_name, ctx=ast.Load()),
                                        args=[
                                            ast.Constant(value=elt.id),
                                            ast.Subscript(
                                                value=ast.Name(id=tmp_name, ctx=ast.Load()),
                                                slice=ast.Constant(value=idx),
                                                ctx=ast.Load(),
                                            ),
                                        ],
                                        keywords=[],
                                    ),
                                )
                            )
                        return result  # type: ignore[return-value]

                # Case 3: multi-target assignment: a = b = call()
                if len(node.targets) > 1 and all(isinstance(t, ast.Name) for t in node.targets):
                    tmp_name = "__pls_tmp"
                    tmp_assign = ast.Assign(
                        targets=[ast.Name(id=tmp_name, ctx=ast.Store())], value=node.value
                    )
                    result: list[ast.stmt] = [tmp_assign]
                    for idx, t in enumerate(node.targets):
                        assert isinstance(t, ast.Name)
                        rhs: ast.expr
                        if idx == 0:
                            rhs = ast.Name(id=tmp_name, ctx=ast.Load())
                        else:
                            rhs = ast.Call(
                                func=ast.Attribute(
                                    value=ast.Name(id=tmp_name, ctx=ast.Load()),
                                    attr="copy",
                                    ctx=ast.Load(),
                                ),
                                args=[],
                                keywords=[],
                            )
                        result.append(
                            ast.Assign(
                                targets=[ast.Name(id=t.id, ctx=ast.Store())],
                                value=ast.Call(
                                    func=ast.Name(id=assign_name, ctx=ast.Load()),
                                    args=[ast.Constant(value=t.id), rhs],
                                    keywords=[],
                                ),
                            )
                        )
                    return result  # type: ignore[return-value]

                return node

        tree = _Transformer().visit(tree)
        ast.fix_missing_locations(tree)

        # Ensure the helper import exists near the top.
        import_node = ast.ImportFrom(
            module="pyspark_lineage_stub.naming",
            names=[ast.alias(name="assign", asname=assign_name)],
            level=0,
        )
        tree.body.insert(0, import_node)
        return ast.unparse(tree)

    def rewrite_path(p: Path) -> None:
        if p.is_dir():
            for child in sorted(p.rglob("*.py")):
                rewrite_path(child)
            return
        if p.suffix != ".py":
            return
        original = p.read_text()
        rewritten = rewrite_text(original)
        if rewritten != original:
            p.write_text(rewritten)

    rewrite_path(path)


def _materialize_rewritten_tree_for_lineage(target: str) -> tuple[str, tempfile.TemporaryDirectory[str]] | tuple[str, None]:
    """
    If target is a filesystem module/package, copy + rewrite it in a temp tree.

    Returns (new_target, tempdir_or_None). Caller must keep the tempdir alive
    for as long as it needs to import/execute from the rewritten tree.
    """
    p = Path(target)
    if not p.exists():
        return target, None

    td = tempfile.TemporaryDirectory(prefix="fndry-lineage-rewrite-")
    td_path = Path(td.name)

    if p.is_dir():
        shutil.copytree(p, td_path / p.name, dirs_exist_ok=True)
        rewritten_root = td_path / p.name
        _rewrite_for_lineage_ids_inplace(rewritten_root)
        return str(rewritten_root), td

    # Single module file
    dst = td_path / p.name
    shutil.copy2(p, dst)
    _rewrite_for_lineage_ids_inplace(dst)
    return str(dst), td


def execute_with_default_foundry(
    transform_to_run: str,
    fallback_branches: str,
    omit_checks: bool,
    session: SparkSession,
    dry_run: bool,
    local_dev_branch_name: str,
    transform_name: str | None = None,
):
    mod = import_from_path("transform", transform_to_run)
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


    foundry_source = FoundrySourceWithDuck(
        ctx=FoundryContext(), session=session, 
    )
    local_source = LocalDataSource(session=session)
    sources_mapping: dict[str, DataSource] = {b: foundry_source for b in branches}
    sources_mapping[local_dev_branch_name] = local_source

    TransformRunner(
        sink=LocalFileSinkWithDuck(
            branch=local_dev_branch_name, 
        ),
        sourcer=MixedDataSource(
            sources=sources_mapping,
            fallback_source=foundry_source,
        ),
        fallback_branches=all_branches,
    ).exec_transform(
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


def _import_module_or_package(target: str) -> Any:
    """
    Import a dotted module name or a filesystem path to a package/module.

    - Dotted import: "my_pkg.transforms"
    - Package path:  "/abs/path/to/my_pkg" (must contain __init__.py)
    - Module path:   "/abs/path/to/mod.py"
    """
    p = Path(target)
    if p.exists():
        if p.is_dir():
            init_py = p / "__init__.py"
            if not init_py.exists():
                raise ValueError(f"Package path {target} is missing __init__.py")
            name = f"lineage_pkg_{abs(hash(str(p)))}"
            spec = importlib.util.spec_from_file_location(
                name, str(init_py), submodule_search_locations=[str(p)]
            )
        else:
            name = f"lineage_mod_{abs(hash(str(p)))}"
            spec = importlib.util.spec_from_file_location(name, str(p))
        if spec is None or spec.loader is None:
            raise ValueError(f"Failed to import {target}")
        mod = importlib.util.module_from_spec(spec)  # type: ignore[assignment]
        sys.modules[name] = mod
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod
    return importlib.import_module(target)


def execute_lineage(
    module_or_package: str,
    fallback_branches: str,
    local_dev_branch_name: str,
    lineage_out: Path,
    ui: bool,
    port: int,
) -> None:
    """
    Discover and execute all transforms in a module/package under lineage tracking.
    """
    # Schema session: DuckDB-backed Spark-compatible session (no JVM).
    from transforms.engine.duckdb import init_sess as init_schema_sess

    schema_session = init_schema_sess()

    branches = fallback_branches.split(",")
    all_branches = [local_dev_branch_name] + branches

    local_source = LocalDataSource(session=schema_session)
    foundry_source: DataSource | None
    try:
        foundry_source = FoundrySourceWithDuck(ctx=FoundryContext(), session=schema_session)
    except Exception:
        # Lineage mode should be usable without Foundry credentials (local-only).
        foundry_source = None

    sources_mapping: dict[str, DataSource] = {}
    for b in branches:
        if b == local_dev_branch_name or foundry_source is None:
            sources_mapping[b] = local_source
        else:
            sources_mapping[b] = foundry_source
    sources_mapping[local_dev_branch_name] = local_source

    from transforms.lineage.schema_only_source import SchemaOnlyDataSource
    from transforms.lineage.collector import GlobalLineageCollector
    from transforms.lineage.patch_pyspark_imports import patch_pyspark_imports_for_lineage
    from transforms.lineage.sink import NoOpDataSink

    # Patch pyspark imports before loading user transform modules.
    patch_pyspark_imports_for_lineage()

    from transforms.engine.lineage import init_sess as init_lineage_sess

    lineage_session = init_lineage_sess()
    collector = GlobalLineageCollector()

    sourcer = SchemaOnlyDataSource(
        upstream=MixedDataSource(sources=sources_mapping, fallback_source=foundry_source),
        lineage_session=lineage_session,
        fallback_branches=all_branches,
    )

    # Import target and discover transforms.
    rewritten_target, td = _materialize_rewritten_tree_for_lineage(module_or_package)
    try:
        mod = _import_module_or_package(rewritten_target)
        pipeline = Pipeline()
        pipeline.discover_transforms(mod)
        transforms = pipeline._transforms
        if not transforms:
            raise ValueError(f"No transforms discovered in {module_or_package}")
    finally:
        # tempdir can be removed after discovery; transforms hold function refs.
        # keep it around a bit longer (until after discovery) to be safe.
        if td is not None:
            td.cleanup()

    # Add transform-level relationships (dataset -> transform -> dataset) to the lineage output.
    transform_meta: dict[int, dict[str, Any]] = {}
    for idx, t in enumerate(transforms):
        t_inputs = [i.path_or_rid for i in t.inputs.values()]
        t_outputs = [o.path_or_rid for o in t.outputs.values()]
        fn = getattr(t, "transform", None)
        name = getattr(fn, "__name__", None) or f"transform_{idx}"
        module_name = getattr(fn, "__module__", None)
        file_name: str | None = None
        try:
            file_name = fn.__code__.co_filename  # type: ignore[union-attr]
        except Exception:
            file_name = None
        collector.record_transform(
            transform_id=f"t{idx}",
            name=name,
            inputs=t_inputs,
            outputs=t_outputs,
            module=module_name,
            file=file_name,
        )
        transform_meta[idx] = {
            "id": f"t{idx}",
            "name": name,
            "module": module_name,
            "file": file_name,
        }

    runner = TransformRunner(
        sink=NoOpDataSink(),
        sourcer=sourcer,
        fallback_branches=all_branches,
    )

    for idx, t in enumerate(transforms):
        meta = transform_meta.get(idx)
        if meta is not None:
            collector.start_transform(
                transform_id=meta["id"],
                name=meta["name"],
                file=meta["file"],
                module=meta["module"],
            )
        runner.exec_transform(
            t,
            omit_checks=True,
            dry_run=True,
            sources=None,
            lineage_collector=collector,
            lineage_mode=True,
        )
        collector.end_transform()

    lineage_out.parent.mkdir(parents=True, exist_ok=True)
    collector.write_json(lineage_out)

    if ui:
        from transforms.lineage.server import serve_lineage_ui

        serve_lineage_ui(lineage_json=collector.to_dict(), port=port)

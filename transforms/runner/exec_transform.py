import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Union, Dict, Optional
from asyncio import gather
from asyncer import syncify
from pyspark.sql import DataFrame

from transforms.api.transform_df import Transform, TransformOutput
from transforms.generate_types import generate_from_spark_batch
from transforms.runner.data_sink.base import DataSink
from transforms.runner.data_source.base import DataSource
from transforms.runner.data_source.download_result import DownloadMetadata, DownloadResult
from transforms.runner.dataset_logging import (
    dataset_display_name,
    format_elapsed,
    log_verbose_step,
)
from transforms.runner.progress import get_progress

from .exec_check import execute_check
import logging
logger = logging.getLogger(__name__)

@dataclass
class TransformRunner:
    sourcer: DataSource
    sink: DataSink
    fallback_branches: list[str] = field(default_factory=list)
    output_dir: Path = Path.home() / ".fndry_duck" / "output"
    secrets_config_location: Path = Path.home() / ".fndry_duck" / "secrets"
    verbose: bool = False
    
    def __post_init__(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _dataset_label(self, dataset_path_or_rid: str, branch: str | None = None) -> str:
        if hasattr(self.sourcer, "resolve_dataset_label"):
            return self.sourcer.resolve_dataset_label(
                dataset_path_or_rid,
                branch or (self.fallback_branches[-1] if self.fallback_branches else "master"),
            )
        return dataset_display_name(dataset_path_or_rid)

    async def _download_input(
        self,
        argname: str,
        dataset_path_or_rid: str,
        branches: list[str],
    ) -> DownloadResult:
        progress = get_progress()
        task_id = None
        task_token = None

        with log_verbose_step(
            "resolve label",
            verbose=self.verbose,
            log=logger,
            input=argname,
            dataset=dataset_path_or_rid,
        ):
            label = self._dataset_label(dataset_path_or_rid, branches[-1])

        if progress is not None:
            task_id = progress.start_input_task(argname, label)
            task_token = progress.set_current_task(task_id)
            progress.update_task(task_id, phase="loading", detail=", ".join(branches))

        try:
            if progress is None:
                logger.info(
                    "Loading input '%s': %s (branches: %s)",
                    argname,
                    label,
                    ", ".join(branches),
                )
            started = time.perf_counter()
            result = await self.sourcer.download_for_branches(dataset_path_or_rid, branches)
            if progress is not None and task_id is not None:
                progress.complete_task(
                    task_id,
                    phase=f"loaded {format_elapsed(time.perf_counter() - started)}",
                    branch=result.metadata.branch if result.metadata else branches[-1],
                )
            elif progress is None:
                logger.info(
                    "Loaded input '%s': %s in %s",
                    argname,
                    label,
                    format_elapsed(time.perf_counter() - started),
                )
            return result
        except Exception as exc:
            if progress is not None and task_id is not None:
                progress.fail_task(task_id, error=str(exc))
            raise
        finally:
            if progress is not None and task_token is not None:
                progress.reset_current_task(task_token)

    def _post_process_downloads(self, results: list[DownloadResult]) -> None:
        progress = get_progress()
        metadata_list: list[DownloadMetadata] = [
            r.metadata for r in results if r.metadata is not None
        ]
        type_entries = [
            (r.metadata.dataset_name, r.df)
            for r in results
            if r.metadata is not None and r.metadata.dataset_name is not None
        ]
        if type_entries:
            if progress is not None:
                progress.start_phase("types", "Generate types")
            with log_verbose_step(
                "generate types",
                verbose=self.verbose,
                log=logger,
                datasets=len(type_entries),
            ):
                generate_from_spark_batch(type_entries)
            if progress is not None:
                progress.complete_phase("types")
        if metadata_list and hasattr(self.sourcer, "register_duckdb_views"):
            if progress is not None:
                progress.start_phase("duckdb", "DuckDB views")
            with log_verbose_step(
                "duckdb views",
                verbose=self.verbose,
                log=logger,
                datasets=len(metadata_list),
            ):
                self.sourcer.register_duckdb_views(metadata_list)
            if progress is not None:
                progress.complete_phase("duckdb")

    async def download_datasets(self, transform: Transform, omit_checks: bool, dry_run: bool) -> Dict[str, Union[DataFrame, TransformOutput]]:
        sources: Dict[str, Union[DataFrame, TransformOutput]] = {}
        futures_list = [
            (
                argname,
                self._download_input(
                    argname,
                    input.path_or_rid,
                    [b for b in ([input.branch] + self.fallback_branches) if b is not None],
                ),
            )
            for argname, input in transform.inputs.items()
        ]
        futures = [fut for (_, fut) in futures_list]
        names = [name for (name, _) in futures_list]
        results: list[DownloadResult] = await gather(*futures)
        self._post_process_downloads(results)
        for argname, result in zip(names, results):
            sources[argname] = result.df
        return sources

    def _download_for_branches_df(
        self, dataset_path_or_rid: str, branches: list[str]
    ) -> DataFrame:
        result = syncify(self.sourcer.download_for_branches, raise_sync_error=False)(
            dataset_path_or_rid, branches
        )
        if result.metadata is not None:
            self._post_process_downloads([result])
        return result.df
            
    def exec_transform(self, transform: Transform, omit_checks: bool, dry_run: bool, sources: Optional[Dict[str, Union[DataFrame, TransformOutput]]] = None) -> None:
        if sources is None:
            sources = syncify(self.download_datasets, raise_sync_error=False)(transform, omit_checks, dry_run)
            
        if transform.multi_outputs is not None:
            impl_multi_outputs = {}
            for argname, output in transform.outputs.items():
                def on_dataframe_req(mode: Literal["current", "previous"]) -> DataFrame:
                    if mode == "current":
                        return self._download_for_branches_df(
                            output.path_or_rid, branches=self.fallback_branches
                        )
                    elif (transform.incremental_opts is not None) and (not dry_run):
                        return syncify(self.sourcer.download_latest_incremental_transaction)(
                            dataset_path_or_rid=output.path_or_rid,
                            branches=self.fallback_branches,
                            semantic_version=transform.incremental_opts.semantic_version
                        )
                    raise ValueError(f"Invalid mode {mode} or missing incremental options")

                def create_write_fn(
                    output_path: str,
                    output_argname: str,
                    output_checks: list,
                ):
                    def on_dataframe_write(df: DataFrame, mode: Literal["append", "replace"]) -> None:
                        if mode == "append":
                            raise NotImplementedError()
                        else:
                            if not omit_checks:
                                for check in output_checks:
                                    execute_check(df, check)
                            if (transform.incremental_opts is not None) and (not dry_run):
                                self.sink.save_incremental_transaction(
                                    df,
                                    output_path,
                                    transform.incremental_opts.semantic_version
                                )
                            else:
                                label = self._dataset_label(output_path)
                                logger.info(
                                    "Writing output '%s': %s",
                                    output_argname,
                                    label,
                                )
                                started = time.perf_counter()
                                self.sink.save_transaction(
                                    df=df,
                                    dataset_path_or_rid=output_path,
                                )
                                logger.info(
                                    "Finished writing output '%s': %s in %s",
                                    output_argname,
                                    label,
                                    format_elapsed(time.perf_counter() - started),
                                )
                    return on_dataframe_write

                output_df_impl = TransformOutput(
                    on_dataframe_req=on_dataframe_req,
                    on_dataframe_write=create_write_fn(
                        output.path_or_rid, argname, output.checks
                    ),
                )
                impl_multi_outputs[argname] = output_df_impl
                sources[argname] = output_df_impl
            
            transform.multi_outputs = impl_multi_outputs
        
        if transform.outputs:
            for argname, output in transform.outputs.items():
                if get_progress() is None:
                    logger.info(
                        "Prepared output '%s': %s",
                        argname,
                        self._dataset_label(output.path_or_rid),
                    )

        transform_fn = transform.transform
        while hasattr(transform_fn, "__wrapped__"):
            transform_fn = transform_fn.__wrapped__
        transform_name = getattr(transform_fn, "__name__", "transform")

        progress = get_progress()
        if progress is not None:
            progress.start_phase("transform", f"Transform {transform_name}")
        elif not transform.outputs:
            pass
        logger.info("Starting transform '%s'", transform_name)
        transform_started = time.perf_counter()
        res = transform.transform(**sources)
        if progress is not None:
            progress.complete_phase(
                "transform",
                elapsed=format_elapsed(time.perf_counter() - transform_started),
            )
        logger.info(
            "Finished transform '%s' in %s",
            transform_name,
            format_elapsed(time.perf_counter() - transform_started),
        )
        
        if transform.multi_outputs is None:
            if not hasattr(res, "limit") or not hasattr(res, "collect"):
                raise ValueError("Transform without multi_outputs must return a DataFrame-like object")
            
            if not omit_checks:
                for check in transform.outputs["output"].checks:
                    logger.info(f"Running check {check.description}")
                    execute_check(res if not dry_run else res.limit(1), check)
                    logger.info(f"Check {check.description} finished")
            if not dry_run:
                output_path = transform.outputs["output"].path_or_rid
                label = self._dataset_label(output_path)
                progress = get_progress()
                if progress is not None:
                    progress.start_phase("write", f"Write {label}")
                if progress is None:
                    logger.info(
                        "Writing output: %s",
                        label,
                    )
                started = time.perf_counter()
                self.sink.save_transaction(df=res, dataset_path_or_rid=output_path)
                if progress is not None:
                    progress.complete_phase(
                        "write",
                        elapsed=format_elapsed(time.perf_counter() - started),
                    )
                elif progress is None:
                    logger.info(
                        "Finished writing output: %s in %s",
                        label,
                        format_elapsed(time.perf_counter() - started),
                    )
            else:
                res.limit(1).collect()
        else:
            if transform.incremental_opts is not None:
                logger.info("Finished transform successfully")
                raise NotImplementedError("Not yet implemented saving for incremental dataset")
            else:
                logger.info("Finished transform successfully")
                
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from pyspark.sql import DataFrame

from transforms.api.transform_df import OutputDf, Transform
from transforms.runner.data_sink.base import DataSink
from transforms.runner.data_source.base import DataSource

from .exec_check import execute_check


@dataclass
class TransformRunner:
    sourcer:DataSource
    sink:DataSink
    fallback_branches: list[str] = field(default_factory=list)
    output_dir: Path = Path.home() / ".fndry_duck" / "output"
    secrets_config_location :Path =  Path.home() / ".fndry_duck" / "secrets"
    
    def __post_init__(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def exec_transform(self, transform: Transform, omit_checks: bool, dry_run:bool) -> None:
        sources = {}

        for argname, input in transform.inputs.items():
            branches = [
                b for b in ([input.branch] + self.fallback_branches) if b is not None
            ]
            sources[argname] = self.sourcer.download_for_branches(
                input.path_or_rid, branches=branches
            )
        if transform.external_systems is not None:
            for external_system in transform.external_systems:
                transform.external_systems[external_system].secrets_config_location = str(self.secrets_config_location)
                sources[external_system] = transform.external_systems[external_system]
        if transform.multi_outputs is not None:
            impl_multi_outputs = {}
            for argname, output in transform.outputs.items():
                def on_dataframe_req(mode: Literal["current", "previous"]) -> DataFrame:
                    if mode == "current":
                        return self.sourcer.download_for_branches(
                output.path_or_rid, branches=self.fallback_branches)
                    else:
                        raise NotImplementedError()
                def on_dataframe_write(df: DataFrame, mode: Literal["append", "replace"]):
                    if mode == "append":
                        raise NotImplementedError()
                    else:
                        if not omit_checks:
                            for check in output.checks: 
                                execute_check(res, check)
                        
                        self.sink.save_transaction(df = df,dataset_path_or_rid=output.path_or_rid) 
                    
                impl_multi_outputs[argname] = OutputDf(on_dataframe_req=on_dataframe_req, on_dataframe_write=on_dataframe_write)
            transform.multi_outputs = impl_multi_outputs
            
        

        res = transform.transform(**sources).cache()
        
        
        if transform.multi_outputs is None:
            res: DataFrame
            if not omit_checks:
                for check in transform.outputs["output"].checks:
                    execute_check(res if not dry_run else res.limit(1), check)
            if not (dry_run):
                self.sink.save_transaction(df = res,dataset_path_or_rid=transform.outputs['output'].path_or_rid) 
                
            
            else:
                res.limit(1).collect()
        else:
            if transform.incremental_opts is not None:
                raise NotImplementedError("Not yet implemented saving for incremental dataset")
            else:
                print("Finished transform successfully")
                return
                

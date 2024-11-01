from dataclasses import dataclass

from .transform_df import Transform


@dataclass
class IncrementalTransformOpts:
    semantic_version: int = 1
    snapshot_inputs: list[str] | None = None
    allow_retention: bool = False
    strict_append: bool = False
    require_incremental: bool = False
    v2_semantics: bool = False


class IncrementalInput:
    def __init__(self, path_or_rid: str):
        self.path_or_rid = path_or_rid

    def dataframe(self,):
        pass
        

def incremental(
    require_incremental: bool = False,
    semantic_version: int = 1,
    snapshot_inputs: list[str] | None = None,
    allow_retention: bool = False,
    strict_append: bool = False,
    v2_semantics: bool = False,
):
    def _incremental(transform: Transform):
        if (snapshot_inputs is not None):
            for input in snapshot_inputs:
                if input not in transform.inputs:
                    raise ValueError(
                        f"Input {input} is not defined in transform {transform}"
                    )
        transform.incremental_opts = IncrementalTransformOpts(
            semantic_version=semantic_version,
            snapshot_inputs=snapshot_inputs,
            allow_retention=allow_retention,
            strict_append=strict_append,
            require_incremental=require_incremental,
            v2_semantics=v2_semantics,
        )
        return transform
    return _incremental
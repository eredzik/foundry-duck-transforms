from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Concatenate, Literal, ParamSpec

from pyspark.sql import DataFrame, SparkSession

from .external.systems import ExternalSystemReq

if TYPE_CHECKING:
    from .incremental_transform import IncrementalTransformOpts

from .check import Check


@dataclass
class Input:
    path_or_rid: str
    checks: Check | None | list[Check] = None
    branch: str | None = None


@dataclass
class Output:
    path_or_rid: str
    checks: Check | None | list[Check] = None

@dataclass
class Context:
    session: SparkSession
    is_incremental: bool = False
class Transform:
    def __init__(
        self,
        inputs: dict[str, Input],
        outputs: dict[str, Output],
        transform: Callable[..., Any],
        multi_outputs: dict[str, "OutputDf"] | None = None,
        incremental_opts: "IncrementalTransformOpts | None" = None,
        external_systems: dict[str, "ExternalSystemReq"] | None = None,
    ):
        self.inputs = inputs
        self.outputs = outputs
        self.transform = transform
        self.multi_outputs = multi_outputs
        self.incremental_opts = incremental_opts
        self.external_systems = external_systems



DecoratorParamSpec = ParamSpec(
    "DecoratorParamSpec",
)


class InputDf:
    def __init__(self, df: DataFrame):
        self.df = df

    def dataframe(
        self,
    ):
        return self.df


class OutputDf:
    def __init__(
        self,
        on_dataframe_req: Callable[[Literal["current", "previous"]], DataFrame],
        on_dataframe_write: Callable[[DataFrame, Literal["append", "replace"]], None],
    ):
        self.on_dataframe_req = on_dataframe_req
        self.on_dataframe_write = on_dataframe_write
        self.mode_state: Literal['replace', "append"] = "replace" 

    def dataframe(self, mode: Literal["current", "previous"]) -> DataFrame:
        return self.on_dataframe_req(mode)
    def mode(self, mode: Literal["append", "replace"]):
        self.mode_state = mode
        return self

    def write(self, df: DataFrame):
        return self.on_dataframe_write(df, self.mode_state)


def transform(**kwargs: Input | Output):
    def _transform(transform: Callable[..., Any]):
        inputs: dict[str, Input] = {}
        outputs: dict[str, Output] = {}

        for key, arg in kwargs.items():
            if isinstance(arg, Input):
                inputs[key] = arg

            if isinstance(arg, Output):
                outputs[key] = arg
                
        
        def transformed_transform(**kwargs: DataFrame) -> None:
            new_kwargs = {key: InputDf(df=value) for key, value in kwargs.items()}
            return transform(**new_kwargs)

        return Transform(
            inputs=inputs,
            outputs=outputs,
            transform=transformed_transform,
            multi_outputs={},
        )

    return _transform


def transform_df(output: Output, **kwargs: Input):
    TParams = ParamSpec("TParams")

    def _transform_df(transform: Callable[Concatenate[TParams], DataFrame]):
        return Transform(
            inputs=kwargs,
            outputs={"output": output},
            transform=transform,
            multi_outputs=None,
        )

    return _transform_df

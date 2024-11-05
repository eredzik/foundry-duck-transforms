from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Concatenate, Literal, ParamSpec

from pyspark.sql import DataFrame, SparkSession

from transforms.external.systems import Source

if TYPE_CHECKING:
    from .incremental_transform import IncrementalTransformOpts

from .check import Check


@dataclass
class Input:
    path_or_rid: str
    checks: Check | None | list[Check] = None
    branch: str | None = None



class Output:
    def __init__(self, path_or_rid: str, checks: Check | None | list[Check] = None):
        self.path_or_rid = path_or_rid
        if checks is None:
            checks = []
        elif isinstance(checks, Check):
            checks = [checks]
        else:
            self.checks = checks
        


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
        multi_outputs: dict[str, "TransformOutput"] | None = None,
        incremental_opts: "IncrementalTransformOpts | None" = None,
        external_systems: dict[str, "Source"] | None = None,
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


class TransformInput:
    def __init__(self, df: DataFrame):
        self.df = df

    def dataframe(
        self,
    ):
        return self.df


class TransformOutput:
    def __init__(
        self,
        on_dataframe_req: Callable[[Literal["current", "previous"]], DataFrame],
        on_dataframe_write: Callable[[DataFrame, Literal["append", "replace"]], None],
    ):
        self.on_dataframe_req = on_dataframe_req
        self.on_dataframe_write = on_dataframe_write
        self.mode_state: Literal["replace", "append"] = "replace"

    def dataframe(self, mode: Literal["current", "previous"]) -> DataFrame:
        return self.on_dataframe_req(mode)

    def mode(self, mode: Literal["append", "replace"]):
        self.mode_state = mode
        return self

    def write_dataframe(self, df: DataFrame):
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

        def transformed_transform(**kwargs: DataFrame | Source | TransformOutput) -> None:
            new_kwargs: dict[str, TransformInput|Source| TransformOutput] = {}
            for key, value in kwargs.items():
                if isinstance(value, Source):
                    new_kwargs[key] = value
                elif isinstance(value, TransformOutput):
                    new_kwargs[key] = value
                else:
                    new_kwargs[key] = TransformInput(df=value)
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

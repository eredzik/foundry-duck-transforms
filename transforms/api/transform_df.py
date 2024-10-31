from dataclasses import dataclass
from typing import Callable, Concatenate, Generic, ParamSpec

from pyspark.sql import DataFrame

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


TransformParamSpec = ParamSpec("TransformParamSpec")


@dataclass
class Transform(Generic[TransformParamSpec]):
    inputs: dict[str, Input]
    outputs: list[Output]
    transform: Callable[TransformParamSpec, DataFrame]


DecoratorParamSpec = ParamSpec(
    "DecoratorParamSpec",
)


def transform_df(output: Output, **kwargs: Input):
    TParams = ParamSpec("TParams")

    def _transform_df(transform: Callable[Concatenate[TParams], DataFrame]):
        return Transform(inputs=kwargs, outputs=[output], transform=transform)

    return _transform_df

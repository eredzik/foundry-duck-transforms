from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Concatenate, Generic, ParamSpec

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class Input:
    path_or_rid: str

@dataclass
class Output:
    path_or_rid:str

TransformParamSpec = ParamSpec("TransformParamSpec")
@dataclass
class Transform(Generic[TransformParamSpec]):
    inputs: dict[str,Input]
    outputs: list[Output]
    transform: Callable[TransformParamSpec, DataFrame]

DecoratorParamSpec = ParamSpec("DecoratorParamSpec", )

def transform_df(output: Output, **kwargs: Input):
    TParams = ParamSpec("TParams")

    # TODO: Add verification if params are not overlapping in output
    def _transform_df(transform: Callable[Concatenate[TParams], DataFrame]):
        # TODO: Add verification if all params are correctly specified
        return Transform(inputs=kwargs, outputs=[output], transform=transform)
    return _transform_df

@transform_df(output=Output("dataset"), df1=Input("dataset"), df2=Input("dataset"))
def transformed(df1: DataFrame, df2: DataFrame):
    return df1


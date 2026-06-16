import operator as op
from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import Expectation


@dataclass
class NumericPropertyExpectation(Expectation):
    colname: str
    agg_name: str
    agg_fn: Callable[[Column], Column]
    operator: Callable[[float, float], bool]
    value: float

    def run(self, dataframe_to_verify: DataFrame) -> None:
        agg_col = self.agg_fn(F.col(self.colname)).alias(self.agg_name)
        result = dataframe_to_verify.select(agg_col).collect()[0][0]
        if result is None:
            raise AssertionError(f"Property {self.agg_name} for {self.colname} is None")
        if not self.operator(result, self.value):
            raise AssertionError(
                f"{self.agg_name}({self.colname})={result} is not {self.operator.__name__} {self.value}"
            )


class NumericPropertyBuilder:
    def __init__(self, colname: str, agg_name: str, agg_fn: Callable[[Column], Column]):
        self.colname = colname
        self.agg_name = agg_name
        self.agg_fn = agg_fn

    def gt(self, value: int | float) -> Expectation:
        return NumericPropertyExpectation(self.colname, self.agg_name, self.agg_fn, op.gt, value)

    def gte(self, value: int | float) -> Expectation:
        return NumericPropertyExpectation(self.colname, self.agg_name, self.agg_fn, op.ge, value)

    def lt(self, value: int | float) -> Expectation:
        return NumericPropertyExpectation(self.colname, self.agg_name, self.agg_fn, op.lt, value)

    def lte(self, value: int | float) -> Expectation:
        return NumericPropertyExpectation(self.colname, self.agg_name, self.agg_fn, op.le, value)

    def equals(self, value: int | float) -> Expectation:
        return NumericPropertyExpectation(self.colname, self.agg_name, self.agg_fn, op.eq, value)


class ColumnPropertyBuilder:
    def __init__(self, colname: str):
        self.colname = colname

    def null_percentage(self) -> NumericPropertyBuilder:
        return NumericPropertyBuilder(
            self.colname,
            "null_percentage",
            lambda c: F.avg(F.when(c.isNull(), F.lit(1.0)).otherwise(F.lit(0.0))),
        )

    def null_count(self) -> NumericPropertyBuilder:
        return NumericPropertyBuilder(
            self.colname,
            "null_count",
            lambda c: F.sum(F.when(c.isNull(), F.lit(1)).otherwise(F.lit(0))),
        )

    def distinct_count(self) -> NumericPropertyBuilder:
        return NumericPropertyBuilder(self.colname, "distinct_count", F.countDistinct)

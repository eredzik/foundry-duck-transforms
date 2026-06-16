from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import Expectation


class GroupedExpectationBuilder:
    def __init__(self, *cols: str):
        self.cols = list(cols)

    def is_unique(self) -> Expectation:
        return GroupedExpectationIsUnique(self.cols)

    def count(self) -> "GroupedCountBuilder":
        return GroupedCountBuilder(self.cols)

    def col(self, colname: str) -> "GroupedColumnPropertyBuilder":
        return GroupedColumnPropertyBuilder(self.cols, colname)


@dataclass
class GroupedExpectationIsUnique(Expectation):
    cols: list[str]

    def run(self, dataframe_to_verify: "DataFrame"):
        res = (
            dataframe_to_verify.groupBy(*self.cols)
            .count()
            .filter(F.col("count") > 1)
            .collect()
        )
        if len(res) > 0:
            raise AssertionError(
                f"Grouped expectations are not unique, example issues: {res}"
            )


@dataclass
class GroupedCountExpectation(Expectation):
    cols: list[str]
    operator: Callable[[Column, Column], Column]
    value: int

    def run(self, dataframe_to_verify: "DataFrame"):
        grouped = dataframe_to_verify.groupBy(*self.cols).count()
        bad = grouped.filter(~self.operator(F.col("count"), F.lit(self.value))).limit(10).collect()
        if len(bad) > 0:
            raise AssertionError(f"Grouped count expectation failed, example issues: {bad}")


class GroupedCountBuilder:
    def __init__(self, cols: list[str]):
        self.cols = cols

    def gt(self, value: int) -> Expectation:
        return GroupedCountExpectation(self.cols, lambda a, b: a > b, value)

    def gte(self, value: int) -> Expectation:
        return GroupedCountExpectation(self.cols, lambda a, b: a >= b, value)

    def lt(self, value: int) -> Expectation:
        return GroupedCountExpectation(self.cols, lambda a, b: a < b, value)

    def lte(self, value: int) -> Expectation:
        return GroupedCountExpectation(self.cols, lambda a, b: a <= b, value)

    def eq(self, value: int) -> Expectation:
        return GroupedCountExpectation(self.cols, lambda a, b: a == b, value)

    def equals(self, value: int) -> Expectation:
        return GroupedCountExpectation(self.cols, lambda a, b: a == b, value)


@dataclass
class GroupedDistinctCountExpectation(Expectation):
    cols: list[str]
    colname: str
    operator: Callable[[Column, Column], Column]
    value: int

    def run(self, dataframe_to_verify: "DataFrame"):
        grouped = dataframe_to_verify.groupBy(*self.cols).agg(
            F.countDistinct(F.col(self.colname)).alias("result")
        )
        bad = grouped.filter(~self.operator(F.col("result"), F.lit(self.value))).limit(10).collect()
        if len(bad) > 0:
            raise AssertionError(f"Grouped distinct_count expectation failed, example issues: {bad}")


class GroupedDistinctCountBuilder:
    def __init__(self, cols: list[str], colname: str):
        self.cols = cols
        self.colname = colname

    def gt(self, value: int) -> Expectation:
        return GroupedDistinctCountExpectation(self.cols, self.colname, lambda a, b: a > b, value)

    def gte(self, value: int) -> Expectation:
        return GroupedDistinctCountExpectation(self.cols, self.colname, lambda a, b: a >= b, value)

    def lt(self, value: int) -> Expectation:
        return GroupedDistinctCountExpectation(self.cols, self.colname, lambda a, b: a < b, value)

    def lte(self, value: int) -> Expectation:
        return GroupedDistinctCountExpectation(self.cols, self.colname, lambda a, b: a <= b, value)

    def eq(self, value: int) -> Expectation:
        return GroupedDistinctCountExpectation(self.cols, self.colname, lambda a, b: a == b, value)

    def equals(self, value: int) -> Expectation:
        return GroupedDistinctCountExpectation(self.cols, self.colname, lambda a, b: a == b, value)


class GroupedColumnPropertyBuilder:
    def __init__(self, cols: list[str], colname: str):
        self.cols = cols
        self.colname = colname

    def distinct_count(self) -> GroupedDistinctCountBuilder:
        return GroupedDistinctCountBuilder(self.cols, self.colname)


group_by = GroupedExpectationBuilder

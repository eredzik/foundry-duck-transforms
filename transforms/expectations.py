from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import DataType


@dataclass
class Expecatation(ABC):
    @abstractmethod
    def run(self, dataframe_to_verify: "DataFrame") -> None:
        raise NotImplementedError("Expectation has to be implemented")


@dataclass
class SchemaExpectation(Expecatation):
    expected_schema: Mapping[str, "DataType"]

    def run(self, dataframe_to_verify: "DataFrame"):
        diff = set(dataframe_to_verify.columns).difference(self.expected_schema)
        if len(diff) > 0:
            raise AssertionError(
                f"Schema of the dataframe is not as expected. Missing columns: {diff}"
            )


@dataclass
class SchemaBuilder:
    equals = SchemaExpectation


@dataclass
class PrimaryKeyExpectation(Expecatation):
    pk: str

    def run(self, dataframe_to_verify: "DataFrame"):
        from pyspark.sql import functions as F

        res = dataframe_to_verify.select(
            (F.count_distinct(self.pk) != F.count(self.pk)).alias("result"),
        ).collect()
        if not res[0][0]:
            raise AssertionError("Primary key is not unique")


@dataclass
class ColExpectationIsIn(Expecatation):
    col: str
    values_arr: list[str]

    def run(self, dataframe_to_verify: "DataFrame"):
        from pyspark.sql import functions as F

        cnt = dataframe_to_verify.filter(~F.col(self.col).isin(self.values_arr)).count()
        if cnt > 0:
            raise AssertionError(f"Column {self.col} is not in {self.values_arr}")


@dataclass
class ColExpectationBuilder:
    col: str

    def is_in(self, *values_arr: str) -> Expecatation:
        return ColExpectationIsIn(self.col, list(values_arr))


schema = SchemaBuilder
primary_key = PrimaryKeyExpectation
col = ColExpectationBuilder

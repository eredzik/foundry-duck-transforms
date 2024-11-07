
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import Expectation


class GroupedExpectationBuilder():
    def __init__(self, *cols: str):
        self.cols = cols
@dataclass
class GroupedExpectationIsUnique(Expectation):
    cols: list[str]
    def run(self, dataframe_to_verify: "DataFrame"):
        
        res = dataframe_to_verify.groupBy(*self.cols).count().filter(F.col("count") > 1).collect()
        if len(res) > 0:
            raise AssertionError(f"Grouped expectations are not unique, example issues: {res}")

group_by = GroupedExpectationBuilder
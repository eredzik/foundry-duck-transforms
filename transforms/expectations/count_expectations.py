import operator as op
from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame

from .base import Expectation


@dataclass
class CountExpectation(Expectation):
    reference_count: int
    operator: Callable[[int, int], bool]

    def run(self, dataframe_to_verify: "DataFrame"):
        
        cnt = dataframe_to_verify.count()
        if cnt < self.reference_count: 
            raise AssertionError(f"Count is not {self.operator.__name__} than {self.reference_count}")

class CountExpectationBuilder:
    
    def gte(self, value: int) -> CountExpectation:
        return CountExpectation(value, op.ge)
    def lte(self, value: int) -> CountExpectation:
        return CountExpectation(value, op.le)
    def eq(self, value: int) -> CountExpectation:
        return CountExpectation(value, op.eq)
    def neq(self, value: int) -> CountExpectation:
        return CountExpectation(value, op.ne)
    def gt(self, value: int) -> CountExpectation:
        return CountExpectation(value, op.gt)
    def lt(self, value: int) -> CountExpectation:
        return CountExpectation(value, op.lt)

count = CountExpectationBuilder
    


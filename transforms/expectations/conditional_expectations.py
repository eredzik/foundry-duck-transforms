
from typing_extensions import Self
from pyspark.sql import DataFrame
from .base import Expectation



class ConditionalExpectationBuilder(Expectation):
    def __init__(self, when_pairs: list[tuple[Expectation, Expectation]]):
        self.when_pairs = when_pairs
        self.otherwise_expr: Expectation | None = None
        
    def when(self, when_expr: Expectation, then_expr: Expectation) -> Self:
        self.when_pairs.append((when_expr, then_expr))

        return self
    def otherwise(self, otherwise_expr: Expectation) -> Self:
        self.otherwise_expr = otherwise_expr
        return self
    def run(self, dataframe_to_verify: "DataFrame") -> None:
        # TODO: implement
        pass

        

class ConditionalExpectationStart:
    def __init__(self, when_expr: Expectation,
    then_expr: Expectation):
        ConditionalExpectationStart(when_expr, then_expr)

    

when = ConditionalExpectationStart

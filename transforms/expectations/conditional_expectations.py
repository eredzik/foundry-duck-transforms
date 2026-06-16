from collections.abc import Callable
from typing_extensions import Self
from pyspark.sql import Column
from pyspark.sql import DataFrame
from .base import Expectation
from .colexpects import ColExpectation, ColExpectationIsIn, OpComparisonExpectation



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

    def _expectation_to_column(self, expectation: Expectation) -> Callable[[DataFrame], Column]:
        if isinstance(expectation, ColExpectationIsIn):
            from pyspark.sql import functions as F

            return lambda _df: F.col(expectation.col).isin(expectation.values_arr)
        if isinstance(expectation, OpComparisonExpectation):
            from pyspark.sql import functions as F

            return lambda _df: expectation.operator(F.col(expectation.colname), expectation.value)
        if isinstance(expectation, ColExpectation):
            return lambda df: expectation.operation(df)["result"]
        raise TypeError(
            "Conditional expectations currently support only row-level column expectations"
        )

    def run(self, dataframe_to_verify: "DataFrame") -> None:
        if len(self.when_pairs) == 0:
            raise AssertionError("Conditional expectation must contain at least one when clause")

        remaining_df = dataframe_to_verify
        for when_expr, then_expr in self.when_pairs:
            when_fn = self._expectation_to_column(when_expr)
            matched_df = remaining_df.filter(when_fn(remaining_df))
            if matched_df.count() > 0:
                then_expr.run(matched_df)
            remaining_df = remaining_df.filter(~when_fn(remaining_df))

        if self.otherwise_expr is not None and remaining_df.count() > 0:
            self.otherwise_expr.run(remaining_df)

        

class ConditionalExpectationStart:
    def __init__(self, when_expr: Expectation,
    then_expr: Expectation):
        self._builder = ConditionalExpectationBuilder([(when_expr, then_expr)])

    def when(self, when_expr: Expectation, then_expr: Expectation) -> ConditionalExpectationBuilder:
        return self._builder.when(when_expr, then_expr)

    def otherwise(self, otherwise_expr: Expectation) -> ConditionalExpectationBuilder:
        return self._builder.otherwise(otherwise_expr)

    def run(self, dataframe_to_verify: "DataFrame") -> None:
        self._builder.run(dataframe_to_verify)

    

when = ConditionalExpectationStart

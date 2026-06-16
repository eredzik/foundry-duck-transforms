from .base import Expectation


class TrueExpectation(Expectation):
    def run(self, dataframe_to_verify):
        return


class FalseExpectation(Expectation):
    def run(self, dataframe_to_verify):
        raise AssertionError("Expectation always fails")


class NegateExpectation(Expectation):
    def __init__(self, expectation: Expectation):
        self.expectation = expectation

    def run(self, dataframe_to_verify):
        try:
            self.expectation.run(dataframe_to_verify)
        except AssertionError:
            return
        raise AssertionError("Negated expectation failed")


def true() -> Expectation:
    return TrueExpectation()


def false() -> Expectation:
    return FalseExpectation()


def negate(expectation: Expectation) -> Expectation:
    return NegateExpectation(expectation)


from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class Expectation(ABC):
    @abstractmethod
    def run(self, dataframe_to_verify: "DataFrame") -> None:
        raise NotImplementedError("Expectation has to be implemented")
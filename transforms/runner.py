from dataclasses import dataclass
from typing import Literal

from sqlframe import activate

from transforms.api.transform_df import Transform


@dataclass
class Runner:
    engine: Literal["spark", "duckdb"] = "duckdb"
    datastore: str | None = ":memory:"

    def __post_init__(self):
        activate(engine=self.engine)

    def run_transform(
        self,
        transform: Transform,
    ):
        pass

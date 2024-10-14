from dataclasses import dataclass
from typing import Literal

from ..expectations import Expectation


@dataclass
class Check:
    expectation: Expectation
    description: str
    on_error: Literal['FAIL', "WARN"] = "WARN"
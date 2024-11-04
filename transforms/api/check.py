from dataclasses import dataclass
from typing import Literal

from ..expectations.all import Expectation


@dataclass
class Check:
    expectation: Expectation
    description: str
    on_error: Literal['FAIL', "WARN"] = "WARN"
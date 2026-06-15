from typing import TypeAlias, Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from pyspark.sql import DataFrame


    ds_ds_a:TypeAlias = 'DataFrame[Literal["v"]]'

    ds_ds_b:TypeAlias = 'DataFrame[Literal["v"]]'

    ds_ds_c:TypeAlias = 'DataFrame[Literal["v"]]'

    name_ds_a:TypeAlias = 'DataFrame[Literal[]]'

    name_ds_b:TypeAlias = 'DataFrame[Literal[]]'

    name_ds_c:TypeAlias = 'DataFrame[Literal[]]'

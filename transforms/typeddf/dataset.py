from typing import (
    Any,
    Final,
    Generic,
    Literal,
    LiteralString,
    TypedDict,
    TypeVar,
    Union,
)

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

T = TypeVar("T", bound=LiteralString)

from py4j.java_gateway import JavaObject,

class TColumn(Generic[T], Column):
    def __init__(self, colname: T, jc:JavaObject):
        self.colname = colname
        super().__init__(jc)

TColumn('abc')

TColumn(
    "a",
)


def col[T: LiteralString](colname: T):
    expr = F.col()
    return TColumn(colname, F.col(colname))


def lit[T: LiteralString](litval: Any):
    return TColumn(
        colname="any",
    )


class Dataset(Generic[T], DataFrame):
    def __init__(self, *columns: TColumn, inner: DataFrame):
        self.columns_t: list[TColumn] = list(columns)

    def withColumn[T2: LiteralString](self, colname: T2, col: TColumn):
        newcols: list[Union[T, T2]] = [*self.columns_t, colname]
        newdf = super().withColumn(colname, col)
        return new

    def alias(self, alias: LiteralString):
        newdf = super().alias(alias)
        return newdf

    def select(self, *colnames: T):
        newdf = super().select(colnames)
        return Dataset(*colnames)

    def __getattr__(self, name: T):
        return super().__getattr__(name)


df1 = Dataset("a", "bcd")
df2 = df1.withColumn("c")
df3 = df2.select("a")
df4 = df3.alias("abcd")
df3.bcd

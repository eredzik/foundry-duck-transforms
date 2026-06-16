import pytest

from transforms.engine.spark import init_sess
from transforms.expectations import (
    all,
    col,
    count,
    false,
    group_by,
    negate,
    primary_key,
    schema,
    true,
    when,
)


@pytest.fixture(scope="session")
def spark():
    return init_sess()


def test_count_uses_operator(spark) -> None:
    df = spark.createDataFrame([(1,), (2,), (3,)], ["a"])
    count().gte(3).run(df)
    with pytest.raises(AssertionError):
        count().lt(3).run(df)


def test_primary_key_checks_nulls(spark) -> None:
    df = spark.createDataFrame([(1,), (None,)], ["id"])
    with pytest.raises(AssertionError):
        primary_key("id").run(df)


def test_column_helpers(spark) -> None:
    df = spark.createDataFrame([(1, "x"), (2, None)], ["id", "value"])
    id_type = next(f.dataType for f in df.schema.fields if f.name == "id")
    col("id").exists().run(df)
    col("id").has_type(id_type).run(df)
    col("value").null_count().equals(1).run(df)
    col("value").null_percentage().gte(0.5).run(df)
    col("id").distinct_count().equals(2).run(df)
    with pytest.raises(AssertionError):
        col("value").non_null().run(df)


def test_group_by_count_and_distinct(spark) -> None:
    df = spark.createDataFrame(
        [("A", "x"), ("A", "y"), ("B", "z")], ["grp", "val"]
    )
    group_by("grp").count().gte(1).run(df)
    group_by("grp").col("val").distinct_count().gte(1).run(df)


def test_schema_subset(spark) -> None:
    df = spark.createDataFrame([(1, "x")], ["id", "name"])
    id_type = next(f.dataType for f in df.schema.fields if f.name == "id")
    name_type = next(f.dataType for f in df.schema.fields if f.name == "name")
    schema().contains({"id": id_type}).run(df)
    schema().is_subset_of({"id": id_type, "name": name_type}).run(df)


def test_when_otherwise(spark) -> None:
    df = spark.createDataFrame([(1, "a"), (0, "b")], ["flag", "category"])
    cond = when(
        col("flag").gt(0),
        col("category").is_in("a"),
    ).otherwise(col("category").is_in("b"))
    cond.run(df)


def test_operators(spark) -> None:
    df = spark.createDataFrame([(1,)], ["a"])
    true().run(df)
    negate(false()).run(df)
    all(col("a").gt(0), col("a").lt(5)).run(df)

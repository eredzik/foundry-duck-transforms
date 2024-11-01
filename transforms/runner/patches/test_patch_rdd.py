from typing import Iterable


def test_patch_rdd():
    from sqlframe import activate
    activate(engine="duckdb")
    from pyspark.sql import Row, SparkSession

    from transforms.runner.patches.rdd import patch_dataframe_rdd

    sess = SparkSession.builder.appName("test").getOrCreate()  # type: ignore
    sess: SparkSession
    df = sess.createDataFrame([(1, 2), (3, 4)], "a int, b int")
    df = patch_dataframe_rdd(df)
    def some_func(rows:Iterable[Row]):
        for row in rows:
            new_data = row.asDict()
            new_data['a'] = new_data['a'] + 1
            
            yield Row(**new_data)
    df2 = df.rdd.mapPartitions(some_func)
    assert df2 != df
    df2.toDF("someschema")
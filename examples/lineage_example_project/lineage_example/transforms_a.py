from transforms.api import Input, Output, transform_df


@transform_df(output=Output("example/out_a"), src=Input("example/in_users"))
def compute_a(src):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    # a -> x, y
    return src.select((F.col("user_id") + 1).alias("x")).withColumn("y", F.col("x") * 2)


from transforms.api import Input, Output, transform_df


@transform_df(output=Output("example/out_b"), upstream=Input("example/out_a"))
def compute_b(upstream):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    # y -> z
    return upstream.select(F.col("y").alias("z"))


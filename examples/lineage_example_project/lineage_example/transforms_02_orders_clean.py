from transforms.api import Input, Output, transform_df


@transform_df(output=Output("example/out_orders_clean"), orders=Input("example/in_orders"))
def orders_clean(orders):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    df1 = orders.select(
        F.col("order_id").alias("order_id"),
        F.col("user_id").alias("user_id"),
        F.col("amount").alias("amount"),
        F.col("order_date").alias("order_date"),
    )
    df2 = df1.withColumn("amount_usd", F.col("amount") * F.lit(1.0))
    df3 = df2.withColumn("is_large", F.when(F.col("amount_usd") >= F.lit(50.0), F.lit(1)).otherwise(F.lit(0)))
    df4 = df3.withColumn(
        "amount_bucket",
        F.when(F.col("amount_usd") >= F.lit(50.0), F.lit(5))
        .otherwise(F.when(F.col("amount_usd") >= F.lit(20.0), F.lit(2)).otherwise(F.lit(0))),
    )
    df5 = df4.withColumn("amount_bucket2", (F.col("amount_bucket") + F.lit(1)) * F.lit(3))
    df6 = df5.withColumn("order_key", F.concat(F.lit("o"), F.col("order_id")))
    df7 = df6.withColumn("user_key", F.concat(F.lit("u"), F.col("user_id")))
    df8 = df7.select(
        "order_id",
        "user_id",
        "amount_usd",
        "is_large",
        "amount_bucket2",
        "order_key",
        "user_key",
    )
    df9 = df8.withColumn("order_user", F.concat(F.col("order_key"), F.lit("#"), F.col("user_key")))
    df10 = df9.select(
        "order_id",
        "user_id",
        "amount_usd",
        "is_large",
        "amount_bucket2",
        "order_user",
    )
    return df10


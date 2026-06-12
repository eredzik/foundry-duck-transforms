from transforms.api import Input, Output, transform_df


@transform_df(output=Output("example/out_users_clean"), users=Input("example/in_users"))
def users_clean(users):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    df1 = users.select(
        F.col("user_id").alias("user_id"),
        F.col("name").alias("name"),
        F.col("country").alias("country"),
    )
    df2 = df1.withColumn("is_us", F.when(F.col("country") == F.lit("US"), F.lit(1)).otherwise(F.lit(0)))
    df3 = df2.withColumn(
        "user_bucket",
        F.when(F.col("user_id") == F.lit(1), F.lit(1))
        .otherwise(F.when(F.col("user_id") == F.lit(3), F.lit(1)).otherwise(F.lit(0))),
    )
    df4 = df3.withColumn("name_country", F.concat(F.col("name"), F.lit("-"), F.col("country")))
    df5 = df4.withColumn("user_id2", F.col("user_id") + F.lit(1000))
    df6 = df5.withColumn("bucket2", (F.col("user_bucket") + F.lit(1)) * F.lit(10))
    df7 = df6.withColumn("name_country2", F.concat(F.col("name_country"), F.lit("#"), F.col("user_id2")))
    df8 = df7.select("user_id", "country", "is_us", "user_bucket", "bucket2", "name_country2")
    df9 = df8.withColumn("bucket3", F.col("bucket2") + F.col("user_bucket"))
    df10 = df9.select("user_id", "country", "is_us", "user_bucket", "bucket3", "name_country2")
    return df10


from transforms.api import Input, Output, transform_df


@transform_df(output=Output("example/out_country_rollup"), user_metrics=Input("example/out_user_metrics"))
def country_rollup(user_metrics):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    df1 = user_metrics.select("user_id", "seg2", "score3", "active_label")
    df2 = df1.withColumn("country", F.col("seg2"))
    df3 = df2.withColumn("is_active_int", F.when(F.col("active_label") == F.lit("active"), F.lit(1)).otherwise(F.lit(0)))
    df4 = df3.withColumn("one", F.lit(1))
    df5 = df4.groupBy("country").agg(
        F.sum(F.col("one")).alias("users_cnt"),
        F.sum(F.col("is_active_int")).alias("active_cnt"),
    )
    # Avoid division; keep a comparable "rate-like" score.
    df6 = df5.withColumn("active_rate_like", F.col("active_cnt") * F.lit(100) + F.col("users_cnt"))
    df7 = df6.withColumn("rollup_key", F.concat(F.col("country"), F.lit("#"), F.col("users_cnt")))
    df8 = df7.withColumn("active_key", F.concat(F.col("rollup_key"), F.lit("|"), F.col("active_cnt")))
    df9 = df8.withColumn("rate_key", F.concat(F.col("active_key"), F.lit("|"), F.col("active_rate_like")))
    df10 = df9.select("country", "users_cnt", "active_cnt", "active_rate_like", "rate_key")
    return df10


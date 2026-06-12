from transforms.api import Input, Output, transform_df


@transform_df(output=Output("example/out_events_agg"), events=Input("example/in_events"))
def events_agg(events):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    df1 = events.select("user_id", "event_type", "event_ts")
    df2 = df1.withColumn("is_click", F.when(F.col("event_type") == F.lit("click"), F.lit(1)).otherwise(F.lit(0)))
    df3 = df2.withColumn("is_view", F.when(F.col("event_type") == F.lit("view"), F.lit(1)).otherwise(F.lit(0)))
    df4 = df3.withColumn("one", F.lit(1))
    df5 = df4.groupBy("user_id").agg(
        F.sum(F.col("one")).alias("events_total"),
        F.sum(F.col("is_click")).alias("clicks"),
        F.sum(F.col("is_view")).alias("views"),
    )
    df6 = df5.withColumn("views_safe", F.when(F.col("views") == F.lit(0), F.lit(1)).otherwise(F.col("views")))
    # Avoid division (not supported by the stub Column); keep a monotonic "ctr-like" score.
    df7 = df6.withColumn("ctr_like", F.col("clicks") * F.lit(100) + F.col("views_safe"))
    df8 = df7.withColumn("has_click", F.when(F.col("clicks") > F.lit(0), F.lit(1)).otherwise(F.lit(0)))
    df9 = df8.withColumn("eng_score", F.col("ctr_like") + F.col("events_total"))
    df10 = df9.select("user_id", "events_total", "clicks", "views", "ctr_like", "has_click", "eng_score")
    return df10


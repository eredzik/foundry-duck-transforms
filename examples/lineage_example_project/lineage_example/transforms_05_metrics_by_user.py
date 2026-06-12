from transforms.api import Input, Output, transform_df


@transform_df(
    output=Output("example/out_user_metrics"),
    user_orders=Input("example/out_user_orders"),
    events=Input("example/out_events_agg"),
)
def user_metrics(user_orders, events):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    o1 = user_orders.select("user_id", "order_id", "metric")
    o2 = o1.withColumn("one", F.lit(1))
    o3 = o2.groupBy("user_id").agg(
        F.sum(F.col("one")).alias("orders_cnt"),
        F.sum(F.col("metric")).alias("metric_sum"),
    )
    # Avoid division/modulo (not supported by stub Column); use threshold-based buckets.
    o4 = o3.withColumn(
        "orders_bucket",
        F.when(F.col("orders_cnt") >= F.lit(3), F.lit(3))
        .otherwise(F.when(F.col("orders_cnt") >= F.lit(2), F.lit(2)).otherwise(F.lit(1))),
    )
    o5 = o4.withColumn("metric_avg_like", F.col("metric_sum") + F.col("orders_cnt"))
    o6 = o5.withColumn("orders_seg", F.concat(F.lit("seg_"), F.col("orders_bucket")))
    o7 = o6.withColumn("metric2", F.col("metric_avg_like") * F.lit(10))
    o8 = o7.withColumn("metric3", F.col("metric2") + F.col("orders_cnt"))
    o9 = o8.select("user_id", "orders_cnt", "metric3", "orders_seg")
    o10 = o9.withColumn("orders_cnt_sq", F.col("orders_cnt") * F.col("orders_cnt"))

    e1 = events.select("user_id", "events_total", "clicks", "views", "ctr_like", "eng_score")
    e2 = e1.withColumn("active_int", F.when(F.col("events_total") > F.lit(0), F.lit(1)).otherwise(F.lit(0)))
    e3 = e2.withColumn("eng2", F.col("eng_score") + F.col("ctr"))
    e4 = e3.withColumn("eng3", F.col("eng2") * F.lit(2))
    e5 = e4.select("user_id", "events_total", "active_int", "eng3")
    e6 = e5.withColumn(
        "eng_bucket2",
        F.when(F.col("eng3") >= F.lit(300), F.lit(3))
        .otherwise(F.when(F.col("eng3") >= F.lit(200), F.lit(2)).otherwise(F.lit(1))),
    )
    e7 = e6.withColumn("eng_seg", F.concat(F.lit("e_"), F.col("eng_bucket2")))
    e8 = e7.select("user_id", "events_total", "active_int", "eng_seg")
    e9 = e8.withColumn("events2", F.col("events_total") * F.lit(3))
    e10 = e9.withColumn("events3", F.col("events2") + F.lit(1))

    j1 = o10.join(e10, on="user_id", how="left")
    j2 = j1.withColumn("seg2", F.concat(F.col("orders_seg"), F.lit("|"), F.col("eng_seg")))
    j3 = j2.withColumn("score", F.col("orders_cnt_sq") + F.col("events3"))
    j4 = j3.withColumn(
        "score_bucket",
        F.when(F.col("score") >= F.lit(50), F.lit(5))
        .otherwise(F.when(F.col("score") >= F.lit(20), F.lit(2)).otherwise(F.lit(0))),
    )
    j5 = j4.withColumn("score_label", F.concat(F.lit("s"), F.col("score_bucket")))
    j6 = j5.select("user_id", "seg2", "score_label", "active_int")
    j7 = j6.withColumn("active_label", F.when(F.col("active_int") == F.lit(1), F.lit("active")).otherwise(F.lit("inactive")))
    j8 = j7.withColumn("score2", F.concat(F.col("score_label"), F.lit("#"), F.col("active_label")))
    j9 = j8.withColumn("score3", F.concat(F.col("score2"), F.lit(":"), F.col("user_id")))
    j10 = j9.select("user_id", "seg2", "score3", "active_label")
    return j10


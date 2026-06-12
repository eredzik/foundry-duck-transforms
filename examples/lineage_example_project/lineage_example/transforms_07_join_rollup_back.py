from transforms.api import Input, Output, transform_df


@transform_df(
    output=Output("example/out_user_with_country"),
    user_metrics=Input("example/out_user_metrics"),
    country_rollup=Input("example/out_country_rollup"),
)
def join_rollup(user_metrics, country_rollup):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    u1 = user_metrics.select("user_id", "seg2", "score3", "active_label")
    u2 = u1.withColumn("country", F.col("seg2"))
    u3 = u2.withColumn("user_key", F.concat(F.lit("u"), F.col("user_id")))
    u4 = u3.withColumn("active_int", F.when(F.col("active_label") == F.lit("active"), F.lit(1)).otherwise(F.lit(0)))
    u5 = u4.withColumn("user_sig", F.concat(F.col("user_key"), F.lit("#"), F.col("active_int")))
    u6 = u5.select("user_id", "country", "score3", "user_sig")
    u7 = u6.withColumn("score_sig", F.concat(F.col("score3"), F.lit("|"), F.col("user_sig")))
    u8 = u7.withColumn("score_sig2", F.concat(F.col("score_sig"), F.lit("!"), F.col("country")))
    u9 = u8.select("user_id", "country", "score_sig2")
    u10 = u9.withColumn("country_key", F.concat(F.lit("c"), F.col("country")))

    c1 = country_rollup.select("country", "users_cnt", "active_rate_like", "rate_key")
    c2 = c1.withColumn("country_key", F.concat(F.lit("c"), F.col("country")))
    c3 = c2.withColumn("roll_sig", F.concat(F.col("rate_key"), F.lit("@"), F.col("users_cnt")))
    c4 = c3.withColumn("roll_sig2", F.concat(F.col("roll_sig"), F.lit("|"), F.col("active_rate_like")))
    c5 = c4.select("country_key", "roll_sig2")
    c6 = c5.withColumn("roll_sig3", F.concat(F.col("roll_sig2"), F.lit("#"), F.col("country_key")))
    c7 = c6.withColumn("roll_sig4", F.concat(F.col("roll_sig3"), F.lit("$"), F.col("roll_sig2")))
    c8 = c7.select("country_key", "roll_sig4")
    c9 = c8.withColumn("roll_sig5", F.concat(F.col("roll_sig4"), F.lit("%"), F.col("country_key")))
    c10 = c9.select("country_key", "roll_sig5")

    j1 = u10.join(c10, on="country_key", how="left")
    j2 = j1.withColumn("final_seg", F.concat(F.col("score_sig2"), F.lit("~"), F.col("roll_sig5")))
    j3 = j2.withColumn("label4", F.concat(F.col("final_seg"), F.lit("^"), F.col("user_id")))
    j4 = j3.withColumn("final_key", F.concat(F.col("user_id"), F.lit("@"), F.col("country")))
    j5 = j4.withColumn("final_key2", F.concat(F.col("final_key"), F.lit("#"), F.col("label4")))
    j6 = j5.select("user_id", "country", "final_seg", "label4", "final_key2")
    j7 = j6.withColumn("label5", F.concat(F.col("label4"), F.lit("|"), F.col("country")))
    j8 = j7.withColumn("label6", F.concat(F.col("label5"), F.lit("|"), F.col("user_id")))
    j9 = j8.withColumn("label7", F.concat(F.col("label6"), F.lit("|"), F.col("final_key2")))
    j10 = j9.select("user_id", "country", "final_seg", "label7")
    return j10


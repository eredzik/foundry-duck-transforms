from transforms.api import Input, Output, transform_df


@transform_df(
    output=Output("example/out_final_facts"),
    users=Input("example/out_users_clean"),
    enriched=Input("example/out_user_with_country"),
)
def final_facts(users, enriched):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    u1 = users.select("user_id", "country", "is_us", "user_bucket")
    u2 = u1.withColumn("user_key", F.concat(F.lit("u"), F.col("user_id")))
    u3 = u2.withColumn("seg2", F.concat(F.col("country"), F.lit("|"), F.col("user_bucket")))
    u4 = u3.withColumn("seg3", F.concat(F.col("seg2"), F.lit("#"), F.col("user_key")))
    u5 = u4.withColumn("seg4", F.concat(F.col("seg3"), F.lit("|"), F.col("is_us")))
    u6 = u5.withColumn("bucket2", (F.col("user_bucket") + F.lit(1)) * F.lit(2))
    u7 = u6.withColumn("bucket3", F.col("bucket2") + F.col("user_id"))
    u8 = u7.select("user_id", "seg4", "bucket3")
    u9 = u8.withColumn("seg5", F.concat(F.col("seg4"), F.lit("~"), F.col("bucket3")))
    u10 = u9.select("user_id", "seg5")

    e1 = enriched.select("user_id", "final_seg", "label7")
    e2 = e1.withColumn("en_key", F.concat(F.lit("e"), F.col("user_id")))
    e3 = e2.withColumn("en2", F.concat(F.col("en_key"), F.lit("#"), F.col("final_seg")))
    e4 = e3.withColumn("en3", F.concat(F.col("en2"), F.lit("|"), F.col("label7")))
    e5 = e4.select("user_id", "en3")
    e6 = e5.withColumn("en4", F.concat(F.col("en3"), F.lit("~"), F.col("user_id")))
    e7 = e6.withColumn("en5", F.concat(F.col("en4"), F.lit("!"), F.col("en3")))
    e8 = e7.withColumn("en6", F.concat(F.col("en5"), F.lit("$"), F.col("user_id")))
    e9 = e8.withColumn("en7", F.concat(F.col("en6"), F.lit("%"), F.col("en5")))
    e10 = e9.select("user_id", "en7")

    j1 = u10.join(e10, on="user_id", how="left")
    j2 = j1.withColumn("has_enriched_int", F.lit(1))
    j3 = j2.withColumn("fact_id", F.concat(F.lit("f"), F.col("user_id")))
    j4 = j3.withColumn("fact_key", F.concat(F.col("fact_id"), F.lit("|"), F.col("seg5")))
    j5 = j4.withColumn("fact_key2", F.concat(F.col("fact_key"), F.lit("|"), F.col("has_enriched_int")))
    j6 = j5.withColumn("fact_key3", F.concat(F.col("fact_key2"), F.lit("|"), F.col("en7")))
    j7 = j6.withColumn("fact_key4", F.concat(F.col("fact_key3"), F.lit("#"), F.col("user_id")))
    j8 = j7.withColumn("fact_key5", F.concat(F.col("fact_key4"), F.lit("$"), F.col("fact_id")))
    j9 = j8.withColumn("checksum", F.col("has_enriched_int") + F.col("user_id"))
    j10 = j9.select("user_id", "fact_key5", "checksum")
    return j10


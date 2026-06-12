from transforms.api import Input, Output, transform_df


@transform_df(
    output=Output("example/out_user_orders"),
    users=Input("example/out_users_clean"),
    orders=Input("example/out_orders_clean"),
)
def user_orders(users, orders):  # type: ignore[no-untyped-def]
    from pyspark.sql import functions as F

    u1 = users.select("user_id", "country", "is_us", "user_bucket")
    u2 = u1.withColumn("user_key", F.concat(F.lit("u"), F.col("user_id")))
    u3 = u2.withColumn("country_user", F.concat(F.col("country"), F.lit(":"), F.col("user_key")))
    u4 = u3.withColumn("is_us2", F.col("is_us") * F.lit(10))
    u5 = u4.select("user_id", "user_key", "country_user", "is_us2")
    u6 = u5.withColumn("country_user2", F.concat(F.col("country_user"), F.lit("|"), F.col("is_us2")))
    u7 = u6.withColumn("bucket2", F.col("user_bucket") + F.lit(3))
    u8 = u7.withColumn("bucket3", F.col("bucket2") * F.lit(2))
    u9 = u8.select("user_id", "user_key", "country_user2", "bucket3")
    u10 = u9.withColumn("user_sig", F.concat(F.col("user_key"), F.lit("#"), F.col("bucket3")))

    o1 = orders.select("order_id", "user_id", "amount_usd", "is_large", "amount_bucket2", "order_user")
    o2 = o1.withColumn("order_key", F.concat(F.lit("o"), F.col("order_id")))
    o3 = o2.withColumn("order_sig", F.concat(F.col("order_key"), F.lit("#"), F.col("amount_bucket2")))
    o4 = o3.withColumn("large2", F.col("is_large") * F.lit(7))
    o5 = o4.select("order_id", "user_id", "order_sig", "large2", "order_user")
    o6 = o5.withColumn("order_user2", F.concat(F.col("order_user"), F.lit("|"), F.col("large2")))
    o7 = o6.withColumn("amt2", F.col("amount_usd") + F.lit(1))
    o8 = o7.withColumn("amt3", F.col("amt2") * F.lit(2))
    o9 = o8.select("order_id", "user_id", "order_sig", "order_user2", "amt3")
    o10 = o9.withColumn("order_edge", F.concat(F.col("order_sig"), F.lit("@"), F.col("order_user2")))

    j1 = o10.join(u10, on="user_id", how="left")
    j2 = j1.withColumn("uo_key", F.concat(F.col("user_sig"), F.lit("~"), F.col("order_edge")))
    j3 = j2.withColumn("u_amt", F.col("amt3") + F.col("bucket3"))
    j4 = j3.withColumn("u_amt2", F.col("u_amt") * F.lit(2))
    j5 = j4.select("user_id", "order_id", "uo_key", "u_amt2")
    j6 = j5.withColumn("uo_key2", F.concat(F.col("uo_key"), F.lit("#"), F.col("u_amt2")))
    j7 = j6.withColumn("order_plus", F.col("order_id") + F.lit(10000))
    j8 = j7.withColumn("edge", F.concat(F.col("uo_key2"), F.lit(":"), F.col("order_plus")))
    j9 = j8.select("user_id", "order_id", "edge", "u_amt2")
    j10 = j9.withColumn("metric", F.col("u_amt2") + F.lit(1))
    return j10


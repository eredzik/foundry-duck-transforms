from __future__ import annotations

from pathlib import Path


def main() -> None:
    """
    Generate local parquet inputs for the lineage example project.

    Writes to ~/.fndry_duck/local_output/<branch>/example/<dataset>/*.parquet
    """
    from pyspark.sql import SparkSession

    branch = "duck-fndry-dev"
    base_dir = Path.home() / ".fndry_duck" / "local_output" / branch / "example"
    base_dir.mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.master("local[1]").getOrCreate()

    users_dir = base_dir / "in_users"
    orders_dir = base_dir / "in_orders"
    events_dir = base_dir / "in_events"

    users_dir.mkdir(parents=True, exist_ok=True)
    orders_dir.mkdir(parents=True, exist_ok=True)
    events_dir.mkdir(parents=True, exist_ok=True)

    users = spark.createDataFrame(
        [
            (1, "Ada", "US"),
            (2, "Ben", "PL"),
            (3, "Cora", "US"),
        ],
        ["user_id", "name", "country"],
    )
    orders = spark.createDataFrame(
        [
            (100, 1, 25.0, "2026-03-01"),
            (101, 1, 40.0, "2026-03-02"),
            (102, 2, 15.0, "2026-03-02"),
            (103, 3, 80.0, "2026-03-03"),
        ],
        ["order_id", "user_id", "amount", "order_date"],
    )
    events = spark.createDataFrame(
        [
            (1, "view", "2026-03-01T10:00:00Z"),
            (1, "click", "2026-03-01T10:01:00Z"),
            (2, "view", "2026-03-02T09:00:00Z"),
            (3, "view", "2026-03-03T12:00:00Z"),
        ],
        ["user_id", "event_type", "event_ts"],
    )

    users.write.mode("overwrite").parquet(str(users_dir))
    orders.write.mode("overwrite").parquet(str(orders_dir))
    events.write.mode("overwrite").parquet(str(events_dir))

    print(f"Wrote input parquet to {users_dir}")
    print(f"Wrote input parquet to {orders_dir}")
    print(f"Wrote input parquet to {events_dir}")


if __name__ == "__main__":
    main()


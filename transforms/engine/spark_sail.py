from typing import Optional
from pyspark.sql import SparkSession


def init_sess(sail_server_url: Optional[str] = None) -> SparkSession:
    
    default_sail_server_url = "localhost:50051"
    
    spark = SparkSession.builder.remote(
        f"sc://{sail_server_url or default_sail_server_url}"
    ).getOrCreate()

    return spark

from typing import Optional
from pyspark.sql import SparkSession
# from pysail.spark import SparkConnectServer


def init_sess(sail_server_url: Optional[str]) -> SparkSession:
    # server = SparkConnectServer()
    # server.start()
    # _, port = server.listening_address
    default_sail_server_url = "localhost:50051"
    
    spark = SparkSession.builder.remote(
        f"sc://{sail_server_url or default_sail_server_url}"
    ).getOrCreate()

    return spark

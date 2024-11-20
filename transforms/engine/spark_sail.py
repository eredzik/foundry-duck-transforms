from pyspark.sql import SparkSession
from pysail.spark import SparkConnectServer

def init_sess() -> SparkSession:



    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address

    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()

    return spark
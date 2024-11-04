from pyspark.sql import SparkSession


def init_sess() -> SparkSession:
    
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.executor.memory", "6g")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.hostname", "localhost")
        .config("spark.jars.packages", "org.apache.spark:spark-connect_2.12:3.5.3")
        .getOrCreate()
    )
    
    return spark
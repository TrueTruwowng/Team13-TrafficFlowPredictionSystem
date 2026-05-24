from pyspark.sql import SparkSession
from config import SPARK_MASTER, SPARK_CONF

def get_spark(app_name="TrafficPipeline") -> SparkSession:
    builder = SparkSession.builder.master(SPARK_MASTER).appName(app_name)
    builder = builder.config("spark.sql.session.timeZone", "GMT+7")
    for k, v in SPARK_CONF.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()  
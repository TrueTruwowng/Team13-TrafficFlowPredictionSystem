from pyspark.sql import SparkSession
from config import SPARK_MASTER, SPARK_CONF

_PROMETHEUS_CONF = {
    "spark.eventLog.gcMetrics.youngGenerationGarbageCollectors": "G1 Young Generation",
    "spark.eventLog.gcMetrics.oldGenerationGarbageCollectors":   "G1 Old Generation",
    "spark.ui.prometheus.enabled": "true",
    "spark.sql.streaming.metricsEnabled": "true",
    "spark.metrics.conf.driver.sink.prometheusServlet.class":
        "org.apache.spark.metrics.sink.PrometheusServlet",
    "spark.metrics.conf.driver.sink.prometheusServlet.path": "/metrics/prometheus",
}


def get_spark(app_name="TrafficPipeline") -> SparkSession:
    builder = SparkSession.builder.master(SPARK_MASTER).appName(app_name)
    builder = builder.config("spark.sql.session.timeZone", "GMT+7")
    for k, v in {**SPARK_CONF, **_PROMETHEUS_CONF}.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()

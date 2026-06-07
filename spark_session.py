from pyspark.sql import SparkSession
from config import SPARK_MASTER, SPARK_CONF

# Spark expose /metrics/prometheus qua UI port (4040)
_PROMETHEUS_CONF = {
    "spark.eventLog.gcMetrics.youngGenerationGarbageCollectors": "G1 Young Generation",
    "spark.eventLog.gcMetrics.oldGenerationGarbageCollectors":   "G1 Old Generation",
    "spark.ui.prometheus.enabled": "true",
# 2. BẬT METRICS CHO STRUCTURED STREAMING (Thiếu cái này là ko có Input Rows/s đâu nha)
    "spark.sql.streaming.metricsEnabled": "true",
    
    # 3. Đăng ký Servlet để mở cổng /metrics/prometheus cho Driver
    "spark.metrics.conf.driver.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet",
    "spark.metrics.conf.driver.sink.prometheusServlet.path": "/metrics/prometheus",
}

def get_spark(app_name="TrafficPipeline") -> SparkSession:
    builder = SparkSession.builder.master(SPARK_MASTER).appName(app_name)
    builder = builder.config("spark.sql.session.timeZone", "GMT+7")
    for k, v in {**SPARK_CONF, **_PROMETHEUS_CONF}.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()

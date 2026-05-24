import sys
sys.path.insert(0, "/home/dis/Project")
from spark_session import get_spark
spark = get_spark("Check")
import os
from dotenv import load_dotenv
load_dotenv()
from config import GCS_BUCKET

BRONZE = f"gs://{GCS_BUCKET}/bronze/traffic"
SILVER = f"gs://{GCS_BUCKET}/silver"
GOLD   = f"gs://{GCS_BUCKET}/gold"

print("=== BRONZE count ===")
b = spark.read.format("delta").load(BRONZE)
print(b.count())
print("=== BRONZE schema ===")
b.printSchema()
print("=== BRONZE sample (5 rows) ===")
b.orderBy("recordDatetime").show(5, truncate=False)

print("=== SILVER count ===")
s = spark.read.format("delta").load(SILVER)
print(s.count())
print("=== SILVER sample (5 rows) ===")
s.orderBy("recordDatetime").show(5, truncate=False)

print("=== GOLD count ===")
g = spark.read.format("delta").load(GOLD)
print(g.count())
print("=== GOLD sample (5 rows) ===")
g.orderBy("recordDatetime").show(5, truncate=False)

spark.stop()

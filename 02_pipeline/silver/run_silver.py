# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver 변환: commerce_poc.bronze.events → commerce_poc.silver.*
# MAGIC `silver_stream.py`, `silver_check.py` 호출 (로직 없음)

# COMMAND ----------

from silver_check import check_silver
from silver_stream import run_silver
from silver_tables import create_tables
from validate_skus import load_known_skus

BRONZE_TABLE = "commerce_poc.bronze.events"
SCHEMA = "commerce_poc.silver"
CHECKPOINT = "/Volumes/commerce_poc/silver/checkpoints/silver_events"
SINCE = "2026-10-12T02:09:45.000Z"  # run_bronze.py와 같은 값 (send_events.py가 출력한 started_at)

# COMMAND ----------

# 스키마와 체크포인트 볼륨이 없으면 만든다 (Unity Catalog 권한 필요)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.checkpoints")
create_tables(spark, SCHEMA)

# COMMAND ----------

run_silver(spark, BRONZE_TABLE, SCHEMA, CHECKPOINT, load_known_skus(), SINCE)

# COMMAND ----------

# 비어 있으면 expected.json과 일치한다
print(check_silver(spark, SCHEMA, BRONZE_TABLE, SINCE) or "OK")

# COMMAND ----------

# 재실행 검증: 새 체크포인트로 Bronze를 처음부터 다시 읽어도 건수가 그대로여야 한다 (insert-only MERGE의 멱등성)
run_silver(spark, BRONZE_TABLE, SCHEMA, CHECKPOINT + "_rerun", load_known_skus(), SINCE)
print(check_silver(spark, SCHEMA, BRONZE_TABLE, SINCE) or "OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' AS t, COUNT(*) AS n FROM commerce_poc.silver.orders
# MAGIC UNION ALL SELECT 'order_items', COUNT(*) FROM commerce_poc.silver.order_items
# MAGIC UNION ALL SELECT 'payments', COUNT(*) FROM commerce_poc.silver.payments
# MAGIC UNION ALL SELECT 'order_cancellations', COUNT(*) FROM commerce_poc.silver.order_cancellations
# MAGIC UNION ALL SELECT 'refunds', COUNT(*) FROM commerce_poc.silver.refunds
# MAGIC UNION ALL SELECT 'quarantine', COUNT(*) FROM commerce_poc.silver.quarantine;

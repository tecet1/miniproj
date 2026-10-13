# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold 적재: commerce_poc.silver.* → commerce_poc.gold.*
# MAGIC `gold_build.py`, `gold_check.py` 호출 (로직 없음)

# COMMAND ----------

from gold_build import build_gold
from gold_check import check_gold

SILVER_SCHEMA = "commerce_poc.silver"
GOLD_SCHEMA = "commerce_poc.gold"

# COMMAND ----------

# 스키마가 없으면 만든다 (Unity Catalog 권한 필요)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# COMMAND ----------

build_gold(spark, SILVER_SCHEMA, GOLD_SCHEMA)

# COMMAND ----------

# 비어 있으면 expected.json과 일치한다
print(check_gold(spark, SILVER_SCHEMA, GOLD_SCHEMA) or "OK")

# COMMAND ----------

# 재실행 검증: 같은 Silver로 다시 적재해도 건수와 금액이 그대로여야 한다 (MERGE의 멱등성)
build_gold(spark, SILVER_SCHEMA, GOLD_SCHEMA)
print(check_gold(spark, SILVER_SCHEMA, GOLD_SCHEMA) or "OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fact_sales_item' AS t, COUNT(*) AS n FROM commerce_poc.gold.fact_sales_item
# MAGIC UNION ALL SELECT 'fact_refund', COUNT(*) FROM commerce_poc.gold.fact_refund
# MAGIC UNION ALL SELECT 'dim_date', COUNT(*) FROM commerce_poc.gold.dim_date
# MAGIC UNION ALL SELECT 'dim_channel', COUNT(*) FROM commerce_poc.gold.dim_channel
# MAGIC UNION ALL SELECT 'dim_product', COUNT(*) FROM commerce_poc.gold.dim_product
# MAGIC UNION ALL SELECT 'dim_payment_method', COUNT(*) FROM commerce_poc.gold.dim_payment_method
# MAGIC UNION ALL SELECT 'dim_refund_profile', COUNT(*) FROM commerce_poc.gold.dim_refund_profile;

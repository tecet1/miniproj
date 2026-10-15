# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver 변환: commerce_poc.bronze.events → commerce_poc.silver.*
# MAGIC `silver_stream.py`, `silver_check.py` 호출 (로직 없음)

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../quality"))   # 노트북 폴더 기준. Git folder에서 quality 모듈을 찾는다 (미검증)

from batch_check import make_after_batch
from silver_check import check_silver
from silver_stream import run_silver
from silver_tables import create_tables
from validate_skus import load_known_skus

BRONZE_TABLE = "commerce_poc.bronze.events"
SCHEMA = "commerce_poc.silver"
QUALITY_SCHEMA = "commerce_poc.quality"
CHECKPOINT = "/Volumes/commerce_poc/silver/checkpoints/silver_events"
# ADLS 저장 위치: 스키마의 MANAGED LOCATION. 테이블과 체크포인트 볼륨이 이 아래에 저장된다
STORAGE_ROOT = "abfss://commerce-poc@swleeteststorage01.dfs.core.windows.net/storage"
SILVER_PATH = f"{STORAGE_ROOT}/silver"
QUALITY_PATH = f"{STORAGE_ROOT}/quality"

# COMMAND ----------

# 스키마와 체크포인트 볼륨이 없으면 저장 위치를 지정해 만든다 (Unity Catalog 권한 필요). 스키마가 이미 있으면 위치는 바뀌지 않는다
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} MANAGED LOCATION '{SILVER_PATH}'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.checkpoints")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {QUALITY_SCHEMA} MANAGED LOCATION '{QUALITY_PATH}'")   # 증분 검사 결과(results) 저장 위치
create_tables(spark, SCHEMA)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Storage Root가 위 SILVER_PATH인지 확인한다
# MAGIC DESCRIBE SCHEMA EXTENDED commerce_poc.silver;

# COMMAND ----------

# 배치마다 "들어온 수 = 반영 + 중복 + 격리"를 확인해 commerce_poc.quality.results에 기록한다
run_silver(spark, BRONZE_TABLE, SCHEMA, CHECKPOINT, load_known_skus(), make_after_batch(SCHEMA))

# COMMAND ----------

# 비어 있으면 자체 정합성 검사를 통과한 것이다
print(check_silver(spark, SCHEMA, BRONZE_TABLE) or "OK")

# COMMAND ----------

# 재실행 검증: 새 체크포인트로 Bronze를 처음부터 다시 읽어도 건수가 그대로여야 한다 (insert-only MERGE의 멱등성)
run_silver(spark, BRONZE_TABLE, SCHEMA, CHECKPOINT + "_rerun", load_known_skus(), make_after_batch(SCHEMA))
print(check_silver(spark, SCHEMA, BRONZE_TABLE) or "OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' AS t, COUNT(*) AS n FROM commerce_poc.silver.orders
# MAGIC UNION ALL SELECT 'order_items', COUNT(*) FROM commerce_poc.silver.order_items
# MAGIC UNION ALL SELECT 'payments', COUNT(*) FROM commerce_poc.silver.payments
# MAGIC UNION ALL SELECT 'order_cancellations', COUNT(*) FROM commerce_poc.silver.order_cancellations
# MAGIC UNION ALL SELECT 'refunds', COUNT(*) FROM commerce_poc.silver.refunds
# MAGIC UNION ALL SELECT 'quarantine', COUNT(*) FROM commerce_poc.silver.quarantine;

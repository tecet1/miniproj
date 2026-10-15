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
from datetime import datetime

sys.path.append(os.path.abspath("../quality"))   # 노트북 폴더 기준. Git folder에서 quality 모듈을 찾는다 (미검증)

from batch_check import make_after_batch
from run_checks import check_layer
from silver_check import report_by_design
from silver_stream import run_silver
from silver_tables import create_tables
from validate_skus import load_known_skus

BRONZE_TABLE = "commerce_poc.bronze.events"
SCHEMA = "commerce_poc.silver"
QUALITY_SCHEMA = "commerce_poc.quality"
SCHEMAS = {"bronze": "commerce_poc.bronze", "silver": SCHEMA, "gold": "commerce_poc.gold"}
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

# 비어 있으면 Silver 집합 규칙(rules.json, layer = silver)을 모두 통과한 것이다. 결과는 quality.results에 남는다
print(check_layer(spark, SCHEMAS, "silver") or "OK")

# COMMAND ----------

# 보고(실패 아님): Silver가 일부러 그대로 두는 위반의 건수. 오류가 섞인 데이터에서는 0이 아닌 것이 정상이다.
# 업무 ID 중복은 Gold가 제외하고, 참조 누락은 격리된 주문 생성·결제 실패를 가리키는 환불·도착 순서 때문이다 (event_spec.md 1.3·6.6절)
print(report_by_design(spark, SCHEMA))

# COMMAND ----------

# 재실행 검증: 새 체크포인트로 Bronze를 처음부터 다시 읽어도 건수가 그대로여야 한다 (insert-only MERGE의 멱등성)
# 실행할 때마다 새 체크포인트 경로를 쓴다 (고정 경로면 두 번째부터는 이미 끝까지 읽은 체크포인트라 새로 읽는 행이 없다)
rerun_checkpoint = f"{CHECKPOINT}_rerun_{datetime.now():%Y%m%d%H%M%S}"
run_silver(spark, BRONZE_TABLE, SCHEMA, rerun_checkpoint, load_known_skus(), make_after_batch(SCHEMA))
print(check_layer(spark, SCHEMAS, "silver") or "OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' AS t, COUNT(*) AS n FROM commerce_poc.silver.orders
# MAGIC UNION ALL SELECT 'order_items', COUNT(*) FROM commerce_poc.silver.order_items
# MAGIC UNION ALL SELECT 'payments', COUNT(*) FROM commerce_poc.silver.payments
# MAGIC UNION ALL SELECT 'order_cancellations', COUNT(*) FROM commerce_poc.silver.order_cancellations
# MAGIC UNION ALL SELECT 'refunds', COUNT(*) FROM commerce_poc.silver.refunds
# MAGIC UNION ALL SELECT 'quarantine', COUNT(*) FROM commerce_poc.silver.quarantine;

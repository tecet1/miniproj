# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold 적재: commerce_poc.silver.* → commerce_poc.gold.*
# MAGIC `gold_build.py`, `gold_check.py`(제외 보고), `quality/run_checks.py`(규칙 검사) 호출 (로직 없음)

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../quality"))   # 노트북 폴더 기준. Git folder에서 quality 모듈을 찾는다 (미검증)

from gold_build import build_gold
from gold_check import report_exclusions
from run_checks import check_layer

SILVER_SCHEMA = "commerce_poc.silver"
GOLD_SCHEMA = "commerce_poc.gold"
SCHEMAS = {"bronze": "commerce_poc.bronze", "silver": SILVER_SCHEMA, "gold": GOLD_SCHEMA}
# ADLS 저장 위치: 스키마의 MANAGED LOCATION. Gold 테이블 6개가 이 아래에 저장된다
STORAGE_ROOT = "abfss://commerce-poc@swleeteststorage01.dfs.core.windows.net/storage"
GOLD_PATH = f"{STORAGE_ROOT}/gold"

# COMMAND ----------

# 스키마가 없으면 저장 위치를 지정해 만든다 (Unity Catalog 권한 필요). 이미 있으면 위치는 바뀌지 않는다
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA} MANAGED LOCATION '{GOLD_PATH}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Storage Root가 위 GOLD_PATH인지 확인한다
# MAGIC DESCRIBE SCHEMA EXTENDED commerce_poc.gold;

# COMMAND ----------

build_gold(spark, SILVER_SCHEMA, GOLD_SCHEMA)

# COMMAND ----------

# 비어 있으면 Gold 집합 규칙(rules.json, layer = gold)을 모두 통과한 것이다. 결과는 quality.results에 남는다
print(check_layer(spark, SCHEMAS, "gold") or "OK")

# COMMAND ----------

# 제외 보고: 업무 키 위반 주문(주문 생성 2건 이상, 결제 성공 2건 이상)과 무효 환불은 Gold에서 빠진다 (event_spec.md 6.1·6.6절). 0이 아니면 건수를 확인한다
print(report_exclusions(spark, SILVER_SCHEMA))

# COMMAND ----------

# 재실행 검증: 같은 Silver로 다시 적재해도 건수와 금액이 그대로여야 한다 (MERGE의 멱등성)
build_gold(spark, SILVER_SCHEMA, GOLD_SCHEMA)
print(check_layer(spark, SCHEMAS, "gold") or "OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fact_sales_item' AS t, COUNT(*) AS n FROM commerce_poc.gold.fact_sales_item
# MAGIC UNION ALL SELECT 'fact_refund', COUNT(*) FROM commerce_poc.gold.fact_refund
# MAGIC UNION ALL SELECT 'dim_channel', COUNT(*) FROM commerce_poc.gold.dim_channel
# MAGIC UNION ALL SELECT 'dim_product', COUNT(*) FROM commerce_poc.gold.dim_product
# MAGIC UNION ALL SELECT 'dim_payment_method', COUNT(*) FROM commerce_poc.gold.dim_payment_method
# MAGIC UNION ALL SELECT 'dim_refund_profile', COUNT(*) FROM commerce_poc.gold.dim_refund_profile;

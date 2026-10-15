# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver 상시 스트림: commerce_poc.bronze.events → commerce_poc.silver.*
# MAGIC 트리거는 `pipeline_config.json`. 배치가 끝날 때마다 금액 규칙·잔액 균형을 확인한다 (`batch_check.make_after_batch`). 테이블이 없으면 만든다 (`run_silver.py`를 한 번 돌렸다면 이미 있다).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../quality"))   # 노트북 폴더 기준 (미검증)
sys.path.append(os.path.abspath(".."))

from batch_check import make_after_batch
from silver_stream import run_silver
from silver_tables import create_tables
from stream_config import trigger_for
from validate_skus import load_known_skus

BRONZE_TABLE = "commerce_poc.bronze.events"
SCHEMA = "commerce_poc.silver"
CHECKPOINT = "/Volumes/commerce_poc/silver/checkpoints/silver_events"

# COMMAND ----------

create_tables(spark, SCHEMA)

# COMMAND ----------

run_silver(spark, BRONZE_TABLE, SCHEMA, CHECKPOINT, load_known_skus(), make_after_batch(SCHEMA), trigger_for("silver"))   # 끝나지 않는다

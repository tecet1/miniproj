# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold 상시 스트림: Silver 변경 피드(CDF) → commerce_poc.gold.*
# MAGIC 처음 한 번 `bootstrap`(변경 피드 켜기 + 전체 계산)을 하고 그 버전부터 증분으로 이어 간다. 트리거는 `pipeline_config.json`.
# MAGIC 체크포인트가 있으면 이어 읽고, 체크포인트를 지우면 다시 부트스트랩한다.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))   # 노트북 폴더 기준 (미검증)

from gold_incremental import bootstrap, run_gold_stream
from stream_config import trigger_seconds

SILVER_SCHEMA = "commerce_poc.silver"
GOLD_SCHEMA = "commerce_poc.gold"
RESULTS_TABLE = "commerce_poc.quality.results"
CHECKPOINT = "/Volumes/commerce_poc/gold/checkpoints/gold_incremental"

# COMMAND ----------

# 체크포인트가 없을 때만 부트스트랩한다. 있으면 스트림이 체크포인트 위치부터 이어 읽는다
spark.sql("CREATE VOLUME IF NOT EXISTS commerce_poc.gold.checkpoints")
has_checkpoint = os.path.isdir(CHECKPOINT) and len(os.listdir(CHECKPOINT)) > 0
starting_versions = None if has_checkpoint else bootstrap(spark, SILVER_SCHEMA, GOLD_SCHEMA)

# COMMAND ----------

run_gold_stream(spark, SILVER_SCHEMA, GOLD_SCHEMA, CHECKPOINT, starting_versions, trigger_seconds("gold") or 60, results_table=RESULTS_TABLE)   # 끝나지 않는다

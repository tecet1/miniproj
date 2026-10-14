# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 1년치 덤프: 생성 → Bronze 직접 적재
# MAGIC `build_dump.py`(이벤트 줄과 합친 기대값), `load_dump.py`(합성 행 + insert-only MERGE) 호출.
# MAGIC 전체 재적재 순서: Bronze·Silver·Gold 스키마 삭제 → 픽스처를 Function 경로로 주입 → 이 노트북 → 각 단계 노트북

# COMMAND ----------

from build_dump import main as build_dump
from load_dump import load_dump

TABLE = "commerce_poc.bronze.events"
DUMP_DIR = "/Volumes/commerce_poc/bronze/dump"

# COMMAND ----------

# 덤프 폴더(Volume). 스키마는 run_bronze.py가 만든다 (먼저 실행해 bronze.events가 있어야 한다)
spark.sql("CREATE VOLUME IF NOT EXISTS commerce_poc.bronze.dump")
build_dump(["--out", DUMP_DIR])

# COMMAND ----------

print(load_dump(spark, TABLE, f"{DUMP_DIR}/dump"), "rows in dump")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 합성 행(eh_partition = -1)의 채널별 건수. 기대 bronze_rows - 픽스처 24줄과 같아야 한다
# MAGIC SELECT channel, COUNT(*) AS rows FROM commerce_poc.bronze.events WHERE eh_partition = -1 GROUP BY channel ORDER BY channel;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PK 중복 0이어야 한다. 같은 노트북을 다시 실행해도 위 건수가 늘지 않아야 한다
# MAGIC SELECT COUNT(*) AS duplicate_pk FROM (
# MAGIC   SELECT eh_partition, eh_offset FROM commerce_poc.bronze.events GROUP BY eh_partition, eh_offset HAVING COUNT(*) > 1);

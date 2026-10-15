# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 집합 규칙 검사: 증분 실행과 결과 기록
# MAGIC `run_checks.run_quality_checks` 호출 (로직 없음). 결과는 `commerce_poc.quality.results`에 쌓인다.

# COMMAND ----------

from run_checks import failed, run_quality_checks

SCHEMAS = {"bronze": "commerce_poc.bronze", "silver": "commerce_poc.silver", "gold": "commerce_poc.gold"}
STORAGE_ROOT = "abfss://commerce-poc@swleeteststorage01.dfs.core.windows.net/storage"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS commerce_poc.quality MANAGED LOCATION '{STORAGE_ROOT}/quality'")

# COMMAND ----------

# 증분: 직전 실행 이후(10분 겹침) 들어온 행만 본다. 처음 실행하면 전체를 본다. error가 있으면 비어 있지 않다
rows = run_quality_checks(spark, SCHEMAS, mode="incremental")
print([f"{r['rule_id']}: {r['status']} ({r['violations']})" for r in failed(rows)] or "OK")

# COMMAND ----------

# 가끔 도는 전수 점검 (full 규칙 포함, 시간 제한 없음). 덤프처럼 received_at이 과거인 행은 여기서 본다
rows = run_quality_checks(spark, SCHEMAS, mode="full")
print([f"{r['rule_id']}: {r['status']} ({r['violations']})" for r in failed(rows)] or "OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT checked_at, mode, stage, rule_id, status, violations, batch_id, detail
# MAGIC FROM commerce_poc.quality.results ORDER BY checked_at DESC LIMIT 50;

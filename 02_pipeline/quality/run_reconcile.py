# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze 재계산 대조 (전수 배치)
# MAGIC `run_checks.run_quality_checks(mode="reconcile")` 호출 (로직 없음). Bronze 원본에서 Silver·Gold를 독립 경로로 다시 계산해 실제 테이블과 행 단위로 비교하고 결과를 `commerce_poc.quality.results`에 쌓는다.
# MAGIC 파이프라인(Bronze → Silver → Gold)이 다 따라잡은 뒤에 긴 주기(예: 하루 한 번)로 돌린다. 서버리스에서는 `localCheckpoint`가 없어 느릴 수 있다.

# COMMAND ----------

from run_checks import failed, run_quality_checks

SCHEMAS = {"bronze": "commerce_poc.bronze", "silver": "commerce_poc.silver", "gold": "commerce_poc.gold"}

# COMMAND ----------

# 규칙 8개 각각이 "재계산과 다른 행 수"를 기록한다. 0이면 pass. 0이 아니면 error로 남고 아래에 규칙 이름과 건수가 나온다
rows = run_quality_checks(spark, SCHEMAS, mode="reconcile")
print([f"{r['rule_id']}: {r['status']} ({r['violations']})" for r in failed(rows)] or "OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT checked_at, rule_id, status, violations, detail
# MAGIC FROM commerce_poc.quality.results WHERE mode = 'reconcile' ORDER BY checked_at DESC LIMIT 20;

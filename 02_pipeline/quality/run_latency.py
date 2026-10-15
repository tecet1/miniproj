# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 지연 관찰: 테스트 주문이 Bronze → Silver → Gold에 보이기까지
# MAGIC `latency_probe.py` 호출만 한다 (로직 없음). 관찰값이며 목표가 아니다. 결과는 `plans/07` 카드 7-G 메모에 날짜·트리거 설정과 함께 적는다.
# MAGIC
# MAGIC 사용: 새 테스트 주문(주문 생성 + 결제 성공)을 Function으로 보낸 **직후** 주문 번호를 넣고 실행한다.

# COMMAND ----------

from latency_probe import summarize, watch

ORDER_ID = ""   # 방금 보낸 테스트 주문 번호 (영숫자·밑줄·하이픈)
TIMEOUT_SECONDS = 600

# COMMAND ----------

seen = watch(lambda sql: spark.sql(sql).first()["n"], ORDER_ID, TIMEOUT_SECONDS)
print(summarize(seen))

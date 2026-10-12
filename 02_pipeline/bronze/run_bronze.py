# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze 적재: Event Hubs → commerce_poc.bronze.events
# MAGIC 로직은 `bronze_stream.py`에 있고 이 노트북은 호출만 한다. 아래 값은 비밀이 아니다.

# COMMAND ----------

from bronze_check import check_bronze
from bronze_stream import kafka_options, parse_envelope, read_events, write_bronze

EVENTHUB_FQDN = "EVH-training-demo-krc-swlee.servicebus.windows.net"
EVENTHUB_NAME = "commerce-events"
SERVICE_CREDENTIAL = "commerce_event_hubs_reader"
GROUP_ID = "databricks"
TABLE = "commerce_poc.bronze.events"
CHECKPOINT = "/Volumes/commerce_poc/bronze/checkpoints/bronze_events"
SINCE = "2026-10-12T02:09:45.000Z"  # send_events.py가 출력한 started_at

# COMMAND ----------

options = kafka_options(EVENTHUB_FQDN, EVENTHUB_NAME, SERVICE_CREDENTIAL, GROUP_ID)
write_bronze(parse_envelope(read_events(spark, options)), TABLE, CHECKPOINT)

# COMMAND ----------

display(spark.table(TABLE).groupBy("channel").count())

# COMMAND ----------

# 비어 있으면 fixtures와 일치한다
print(check_bronze(spark, TABLE, SINCE) or "OK")

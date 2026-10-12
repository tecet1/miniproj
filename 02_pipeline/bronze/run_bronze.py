# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze 적재: Event Hubs → commerce_poc.bronze.events
# MAGIC 로직은 `bronze_stream.py`에 있고 이 노트북은 호출만 한다. 아래 값은 비밀이 아니다.

# COMMAND ----------

from bronze_stream import kafka_options, parse_envelope, read_events, write_bronze

EVENTHUB_FQDN = "EVH-training-demo-krc-swlee.servicebus.windows.net"
EVENTHUB_NAME = "commerce-events"
SERVICE_CREDENTIAL = "commerce_event_hubs_reader"
GROUP_ID = "databricks"
TABLE = "commerce_poc.bronze.events"
CHECKPOINT = "/Volumes/commerce_poc/bronze/checkpoints/bronze_events"

# COMMAND ----------

options = kafka_options(EVENTHUB_FQDN, EVENTHUB_NAME, SERVICE_CREDENTIAL, GROUP_ID)
write_bronze(parse_envelope(read_events(spark, options)), TABLE, CHECKPOINT)

# COMMAND ----------

display(spark.table(TABLE).groupBy("channel").count())

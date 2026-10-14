# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze 적재: Event Hubs → commerce_poc.bronze.events
# MAGIC `bronze_stream.py` 호출

# COMMAND ----------

from bronze_check import check_bronze
from bronze_stream import kafka_options, parse_envelope, read_events, write_bronze

EVENTHUB_FQDN = "EVH-training-demo-krc-swlee.servicebus.windows.net"
EVENTHUB_NAME = "commerce-events"
SERVICE_CREDENTIAL = "commerce_event_hubs_reader"
GROUP_ID = "databricks"
TABLE = "commerce_poc.bronze.events"
CHECKPOINT = "/Volumes/commerce_poc/bronze/checkpoints/bronze_events"
# ADLS 저장 위치: 스키마의 MANAGED LOCATION. 테이블과 체크포인트 볼륨이 이 아래에 저장된다 (Silver·Gold는 같은 루트의 silver, gold)
STORAGE_ROOT = "abfss://commerce-poc@swleeteststorage01.dfs.core.windows.net/storage"
BRONZE_PATH = f"{STORAGE_ROOT}/bronze"

# COMMAND ----------

# 스키마와 체크포인트 볼륨을 저장 위치를 지정해 만든다. 스키마가 이미 있으면 위치는 바뀌지 않는다 (바꾸려면 스키마를 지우고 다시 만든다)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS commerce_poc.bronze MANAGED LOCATION '{BRONZE_PATH}'")
spark.sql("CREATE VOLUME IF NOT EXISTS commerce_poc.bronze.checkpoints")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Storage Root가 위 BRONZE_PATH인지 확인한다
# MAGIC DESCRIBE SCHEMA EXTENDED commerce_poc.bronze;

# COMMAND ----------

options = kafka_options(EVENTHUB_FQDN, EVENTHUB_NAME, SERVICE_CREDENTIAL, GROUP_ID)
write_bronze(parse_envelope(read_events(spark, options)), TABLE, CHECKPOINT)

# COMMAND ----------

display(spark.table(TABLE).groupBy("channel").count())

# COMMAND ----------

# 비어 있으면 fixtures와 일치한다
print(check_bronze(spark, TABLE) or "OK")


# COMMAND ----------

# MAGIC %sql
# MAGIC    SELECT channel, COUNT(*) FROM commerce_poc.bronze.events GROUP BY channel;
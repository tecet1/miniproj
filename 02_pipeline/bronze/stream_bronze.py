# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze 상시 스트림: Event Hubs → commerce_poc.bronze.events
# MAGIC 트리거는 `pipeline_config.json`. `bronze_stream.write_bronze`를 호출만 한다 (Job의 상시 태스크용. 한 번만 처리하려면 `run_bronze.py`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))   # 노트북 폴더 기준. stream_config 모듈 (미검증)

from bronze_stream import kafka_options, parse_envelope, read_events, write_bronze
from stream_config import trigger_for

EVENTHUB_FQDN = "EVH-training-demo-krc-swlee.servicebus.windows.net"
EVENTHUB_NAME = "commerce-events"
SERVICE_CREDENTIAL = "commerce_event_hubs_reader"
GROUP_ID = "databricks"
TABLE = "commerce_poc.bronze.events"
CHECKPOINT = "/Volumes/commerce_poc/bronze/checkpoints/bronze_events"   # run_bronze.py와 같은 체크포인트라 이어서 읽는다

# COMMAND ----------

options = kafka_options(EVENTHUB_FQDN, EVENTHUB_NAME, SERVICE_CREDENTIAL, GROUP_ID)
write_bronze(parse_envelope(read_events(spark, options)), TABLE, CHECKPOINT, trigger_for("bronze"))   # 끝나지 않는다 (중지하면 Job이 재시작한다)

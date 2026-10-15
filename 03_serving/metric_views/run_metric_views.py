# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Metric View 생성: commerce_poc.gold.* → commerce_poc.serving.*
# MAGIC `create_metric_views.py`, `metric_check.py` 호출 (로직 없음). 먼저 `02_pipeline/gold/run_gold.py`가 성공해야 한다.

# COMMAND ----------

import os
import sys

# metric_check가 02_pipeline/gold의 비교 함수를 다시 쓴다 (Git folder 안의 상대 위치)
sys.path.append(os.path.abspath("../../02_pipeline/gold"))

from create_metric_views import create_metric_views
from metric_check import check_metric_views

GOLD_SCHEMA = "commerce_poc.gold"
SERVING_SCHEMA = "commerce_poc.serving"

# COMMAND ----------

print(create_metric_views(spark, GOLD_SCHEMA, SERVING_SCHEMA))

# COMMAND ----------

# 비어 있으면 자체 정합성 검사를 통과한 것이다
print(check_metric_views(spark, SERVING_SCHEMA) or "OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT channel_name,
# MAGIC        MEASURE(total_sales) AS total_sales, MEASURE(refund_amount) AS refund_amount, MEASURE(net_sales) AS net_sales,
# MAGIC        MEASURE(refund_ratio) AS refund_ratio, MEASURE(item_count) AS item_count,
# MAGIC        MEASURE(paying_customers) AS paying_customers, MEASURE(average_order_value) AS average_order_value
# MAGIC FROM commerce_poc.serving.sales_metrics
# MAGIC GROUP BY channel_name
# MAGIC ORDER BY channel_name;

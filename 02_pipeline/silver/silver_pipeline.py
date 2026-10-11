"""Channel-A Bronze -> typed common events, quarantine, insert-only Delta writes."""

from functools import partial

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DecimalType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

from channel_a import inspect_raw_event


BUSINESS_KEY = ["channel_id", "event_id"]
SOURCE_COLUMNS = ["raw_payload", "source_event_hub", "partition", "offset",
                  "broker_timestamp", "bronze_ingested_at"]
MONEY = DecimalType(18, 0)


def fields(definitions) -> StructType:
    return StructType([StructField(name, kind, True) for name, kind in definitions])


# A single Delta struct has a fixed shape. Only fields for event_type have values.
ITEM_SCHEMA = fields([
    ("order_item_id", StringType()), ("sku_id", StringType()),
    ("quantity", IntegerType()), ("unit_price", MONEY), ("line_amount", MONEY),
    ("cancelled_quantity", IntegerType()), ("cancelled_amount", MONEY),
    ("refunded_quantity", IntegerType()), ("refunded_amount", MONEY),
])
DATA_SCHEMA = fields([
    ("order_amount", MONEY), ("cancellation_id", StringType()), ("cancel_reason", StringType()),
    ("payment_id", StringType()), ("paid_amount", MONEY), ("requested_amount", MONEY),
    ("payment_method", StringType()), ("failure_code", StringType()),
    ("refund_id", StringType()), ("refund_amount", MONEY),
    ("items", ArrayType(ITEM_SCHEMA, containsNull=True)),
])
COMMON_SCHEMA = fields([
    ("schema_version", StringType()), ("event_id", StringType()), ("channel_id", StringType()),
    ("event_type", StringType()), ("event_time", TimestampType()), ("order_id", StringType()),
    ("uid", StringType()), ("data", DATA_SCHEMA),
])
INSPECTION_SCHEMA = fields([(name, StringType()) for name in [
    "common_json", "source_channel_id", "source_event_id", "input_hash",
    "rule_id", "error_code", "error_message",
]])
SOURCE_SCHEMA = fields([
    ("raw_payload", StringType()), ("source_event_hub", StringType()),
    ("partition", IntegerType()), ("offset", LongType()),
    ("broker_timestamp", TimestampType()), ("bronze_ingested_at", TimestampType()),
])
SILVER_SCHEMA = StructType(COMMON_SCHEMA.fields + [
    StructField("business_hash", StringType(), True),
    *[field for field in SOURCE_SCHEMA if field.name != "raw_payload"],
    StructField("silver_processed_at", TimestampType(), True),
])
QUARANTINE_SCHEMA = fields([
    ("quarantine_id", StringType()), ("channel_id", StringType()), ("event_id", StringType()),
    ("rule_id", StringType()), ("error_code", StringType()), ("error_message", StringType()),
])
QUARANTINE_SCHEMA = StructType(QUARANTINE_SCHEMA.fields + SOURCE_SCHEMA.fields + [
    StructField("validated_at", TimestampType(), True),
])


def normalize_bronze(bronze: DataFrame, sku_mapping: dict) -> DataFrame:
    """Python mapping on workers, followed by explicit Spark physical types."""
    inspect = F.udf(partial(inspect_raw_event, sku_mapping=sku_mapping), INSPECTION_SCHEMA)
    checked = bronze.select(*SOURCE_COLUMNS).withColumn("inspection", inspect("raw_payload"))
    return (checked.select(*SOURCE_COLUMNS, "inspection.*")
            .withColumn("common", F.from_json("common_json", COMMON_SCHEMA,
                                             {"mode": "FAILFAST", "timeZone": "UTC"}))
            .withColumn("business_hash", F.sha2("common_json", 256)))


def split_candidates(normalized: DataFrame, existing: DataFrame):
    """Reject conflicting new keys; keep committed business values immutable."""
    invalid = normalized.where(F.col("error_code").isNotNull())
    valid = (normalized.where(F.col("error_code").isNull())
             .select("common.*", *SOURCE_COLUMNS, "business_hash", "input_hash"))
    stored = existing.select(*BUSINESS_KEY, F.col("business_hash").alias("stored_hash"))
    compared = valid.join(stored, BUSINESS_KEY, "left")
    changed = compared.where(F.col("stored_hash").isNotNull()
                             & (F.col("business_hash") != F.col("stored_hash")))
    unchanged = compared.where(F.col("business_hash") == F.col("stored_hash"))
    new = compared.where(F.col("stored_hash").isNull())
    conflicting_keys = (new.groupBy(*BUSINESS_KEY)
                        .agg(F.countDistinct("business_hash").alias("versions"))
                        .where("versions > 1").select(*BUSINESS_KEY))
    new_conflicts = new.join(conflicting_keys, BUSINESS_KEY, "inner")
    accepted = (new.join(conflicting_keys, BUSINESS_KEY, "left_anti")
                .unionByName(unchanged))
    conflicts = (changed.unionByName(new_conflicts)
                 .withColumn("rule_id", F.lit("DQ-01"))
                 .withColumn("error_code", F.lit("EVENT_KEY_CONFLICT"))
                 .withColumn("error_message", F.lit("Same business key has different common values")))
    return accepted, invalid, conflicts


def first_delivery(rows: DataFrame, keys: list[str]) -> DataFrame:
    order = Window.partitionBy(*keys).orderBy(
        F.col("bronze_ingested_at").asc_nulls_last(),
        F.col("source_event_hub"), F.col("partition"), F.col("offset"),
    )
    return rows.withColumn("delivery_rank", F.row_number().over(order)).where("delivery_rank = 1")


def quarantine_rows(rows: DataFrame, *, conflicts: bool = False) -> DataFrame:
    source_id = F.col("channel_id" if conflicts else "source_channel_id")
    event_id = F.col("event_id" if conflicts else "source_event_id")
    fingerprint = F.col("business_hash" if conflicts else "input_hash")
    return (rows.withColumn("quarantine_id", F.sha2(F.concat_ws(
                ":", "rule_id", "error_code", fingerprint), 256))
            .select("quarantine_id", source_id.alias("channel_id"), event_id.alias("event_id"),
                    "rule_id", "error_code", "error_message", *SOURCE_COLUMNS)
            .withColumn("validated_at", F.current_timestamp()))


def prepare_writes(normalized: DataFrame, existing: DataFrame):
    accepted, invalid, conflicts = split_candidates(normalized, existing)
    silver = (first_delivery(accepted, BUSINESS_KEY)
              .withColumn("silver_processed_at", F.current_timestamp())
              .select(*SILVER_SCHEMA.fieldNames()))
    quarantine = first_delivery(
        quarantine_rows(invalid).unionByName(quarantine_rows(conflicts, conflicts=True)),
        ["quarantine_id"],
    ).select(*QUARANTINE_SCHEMA.fieldNames())
    return silver, quarantine


def create_delta_table(spark, table: str, schema: StructType, *, if_not_exists: bool = False) -> None:
    """Explicit SQL DDL avoids writer save modes; existing data is never replaced."""
    table_sql = ".".join("`" + part.replace("`", "``") + "`" for part in table.split("."))
    columns = ", ".join(
        "`" + field.name.replace("`", "``") + "` " + field.dataType.simpleString()
        for field in schema
    )
    clause = "IF NOT EXISTS " if if_not_exists else ""
    spark.sql(f"CREATE TABLE {clause}{table_sql} ({columns}) USING DELTA")


def create_tables(spark, silver_table: str, quarantine_table: str) -> None:
    """Create absent targets without truncating or migrating existing tables."""
    for table, schema in [(silver_table, SILVER_SCHEMA), (quarantine_table, QUARANTINE_SCHEMA)]:
        create_delta_table(spark, table, schema, if_not_exists=True)
        actual = spark.table(table).schema
        expected_types = {field.name: field.dataType for field in schema}
        actual_types = {field.name: field.dataType for field in actual}
        if actual_types != expected_types:
            raise ValueError(f"TARGET_SCHEMA_MISMATCH: {table}; use a reviewed migration or a new table")


def merge_insert_only(rows: DataFrame, table: str, keys: list[str]) -> None:
    from delta.tables import DeltaTable

    if rows.isEmpty():
        return
    condition = " AND ".join(f"target.`{key}` = incoming.`{key}`" for key in keys)
    (DeltaTable.forName(rows.sparkSession, table).alias("target")
     .merge(rows.alias("incoming"), condition)
     .whenNotMatchedInsertAll().execute())


def write_batch(bronze: DataFrame, batch_id: int, *, sku_mapping: dict,
                silver_table: str, quarantine_table: str) -> None:
    """Single writer; two insert-only merges converge after partial-write retries."""
    if bronze.isEmpty():
        return
    normalized = normalize_bronze(bronze, sku_mapping).persist()
    silver = quarantine = None
    try:
        silver, quarantine = prepare_writes(normalized, bronze.sparkSession.table(silver_table))
        silver, quarantine = silver.persist(), quarantine.persist()
        # Freeze both decisions before either target changes. Never swallow write errors.
        accepted_count, quarantine_count = silver.count(), quarantine.count()
        merge_insert_only(quarantine, quarantine_table, ["quarantine_id"])
        merge_insert_only(silver, silver_table, BUSINESS_KEY)
        print({"batch_id": batch_id, "silver_candidates": accepted_count,
               "quarantine_candidates": quarantine_count})
    finally:
        normalized.unpersist()
        if silver is not None:
            silver.unpersist()
        if quarantine is not None:
            quarantine.unpersist()


def start_stream(spark, *, bronze_table: str, silver_table: str, quarantine_table: str,
                 checkpoint: str, sku_mapping: dict, live: bool = False):
    create_tables(spark, silver_table, quarantine_table)
    writer = (spark.readStream.table(bronze_table).writeStream
              .foreachBatch(partial(write_batch, sku_mapping=sku_mapping,
                                    silver_table=silver_table, quarantine_table=quarantine_table))
              .option("checkpointLocation", checkpoint))
    writer = writer.trigger(processingTime="5 seconds") if live else writer.trigger(availableNow=True)
    return writer.start()

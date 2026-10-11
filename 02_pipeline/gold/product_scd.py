"""Build a small product SCD2 from the complete change history, using stdlib only."""

import hashlib
import json
from datetime import datetime, timezone


ATTRIBUTES = ("product_name", "category", "brand")


def utc_time(value: str) -> str:
    time = datetime.fromisoformat(value)
    if time.tzinfo is None:
        raise ValueError("TIMEZONE_REQUIRED")
    return time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def product_key(sku_id: str, valid_from: str) -> str:
    identity = json.dumps([sku_id, valid_from], ensure_ascii=False)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def build_product_history(changes: list[dict]) -> list[dict]:
    """Rebuild deterministically; input must contain all historical changes, not only today's."""
    rows, seen = [], {}
    for change in changes:
        row = {}
        for field in ("sku_id", *ATTRIBUTES, "effective_from"):
            value = change.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"MISSING_OR_INVALID_FIELD: {field}")
            row[field] = value
        row["valid_from"] = utc_time(row.pop("effective_from"))
        key = (row["sku_id"], row["valid_from"])
        if key in seen:
            if seen[key] != row:
                raise ValueError(f"CONFLICTING_PRODUCT_CHANGE: {row['sku_id']}")
            continue
        seen[key] = row
        rows.append(row)
    rows.sort(key=lambda row: (row["sku_id"], datetime.fromisoformat(row["valid_from"])))
    history, latest = [], {}
    for row in rows:
        previous = latest.get(row["sku_id"])
        attributes_changed = previous is not None and any(previous[field] != row[field] for field in ATTRIBUTES)
        if previous is not None:
            if row["valid_from"] == previous["valid_from"]:
                if attributes_changed:
                    raise ValueError(f"CONFLICTING_PRODUCT_CHANGE: {row['sku_id']}")
                continue
            if not attributes_changed:
                continue
            previous["valid_to"] = row["valid_from"]
            previous["is_current"] = False
        version = {**row, "product_key": product_key(row["sku_id"], row["valid_from"]),
                   "valid_to": None, "is_current": True}
        history.append(version)
        latest[row["sku_id"]] = version
    return history


def lookup_product(history: list[dict], sku_id: str, event_time: str) -> dict:
    """Resolve [valid_from, valid_to); exactly one version must match."""
    time = datetime.fromisoformat(utc_time(event_time))
    matches = [row for row in history if row["sku_id"] == sku_id
               and datetime.fromisoformat(row["valid_from"]) <= time
               and (row["valid_to"] is None or time < datetime.fromisoformat(row["valid_to"]))]
    if len(matches) != 1:
        raise ValueError(f"PRODUCT_VERSION_MATCH_COUNT: {sku_id}: {len(matches)}")
    return dict(matches[0])


def write_delta_dimension(spark, history: list[dict], table_name: str = "commerce_poc.gold.dim_product") -> None:
    """Databricks entry point: one Delta-table overwrite from a full history, single writer."""
    if not history:
        raise ValueError("EMPTY_PRODUCT_HISTORY")
    from pyspark.sql import functions as F

    schema = ("sku_id string, product_name string, category string, brand string, "
              "valid_from string, product_key string, valid_to string, is_current boolean")
    frame = (spark.createDataFrame(history, schema)
             .withColumn("valid_from", F.to_timestamp("valid_from"))
             .withColumn("valid_to", F.to_timestamp("valid_to")))
    frame.write.format("delta").mode("overwrite").saveAsTable(table_name)

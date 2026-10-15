"""Gold SQL 테스트용 도우미: Silver 행을 만들고 sqlite에 올린다. Spark 없이 같은 SQL을 로컬에서 실행해 보기 위한 것이다.

  to_silver(events)  공통 이벤트 목록을 Silver 5개 테이블의 행으로 나눈다 (event_spec.md 1.3절 매핑)
  builders         합성 시나리오용 행 만들기: order, item, payment, cancel, refund
  connect(rows)    sqlite 메모리 DB에 `silver` 스키마를 붙여 행을 넣는다
  run(con, sql)    SELECT를 실행해 dict 목록으로 돌려준다
  sqlite_date(col) Spark의 KST 일자 변환(gold_sql.kst_date)을 sqlite로 바꾼 식. 이 식만 로컬과 Databricks가 다르다
"""
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

from convert_row import convert_row  # noqa: E402
from validate_skus import load_known_skus  # noqa: E402

KNOWN = load_known_skus(ROOT / "01_contracts" / "contracts" / "skus.json")
RECEIVED_AT = "2026-10-12T00:00:00.000Z"

# event_spec.md 1.3절의 Silver 컬럼 (Gold가 읽는 5개 테이블)
COLUMNS = {
    "orders": ["channel", "event_id", "order_id", "user_id", "shipping_fee", "order_created_at", "received_at"],
    "order_items": ["channel", "order_item_id", "order_id", "product_id", "product_name", "category", "quantity",
                    "unit_price", "item_discount"],
    "order_cancellations": ["channel", "event_id", "order_id", "user_id", "cancel_reason_code", "cancel_created_at",
                            "received_at"],
    "payments": ["channel", "event_id", "order_id", "user_id", "payment_id", "status", "payment_method",
                 "payment_amount", "payment_fail_reason_code", "payment_created_at", "received_at"],
    "refunds": ["channel", "event_id", "order_id", "user_id", "refund_id", "payment_id", "order_item_id",
                "refund_quantity", "refund_amount", "refund_type", "refund_reason_code", "refund_created_at",
                "received_at"],
}


def sqlite_date(column: str) -> str:
    """UTC 시각 문자열을 KST(+9시간) 날짜로 바꾸는 sqlite 식."""
    return f"date({column}, '+9 hours')"


def to_silver(events: list) -> dict:
    """공통 이벤트 목록을 Silver 행으로 나눈다. 시각은 UTC 문자열 그대로 둔다."""
    rows = {table: [] for table in COLUMNS}
    seen_items = set()   # Silver는 order_items를 (channel, order_item_id)로 한 번만 쌓는다 (silver_stream.build_order_items)
    for e in events:
        base = {"channel": e["channel"], "event_id": e["event_id"], "order_id": e["order_id"],
                "user_id": e["user_id"], "received_at": RECEIVED_AT}
        kind = e["event_type"]
        if kind == "ORDER_CREATED":
            rows["orders"].append({**base, "shipping_fee": e["shipping_fee"], "order_created_at": e["event_time"]})
            for i in e["items"]:
                if (e["channel"], i["order_item_id"]) not in seen_items:
                    seen_items.add((e["channel"], i["order_item_id"]))
                    rows["order_items"].append({**i, "channel": e["channel"], "order_id": e["order_id"]})
        elif kind == "ORDER_CANCELLED":
            rows["order_cancellations"].append({**base, "cancel_reason_code": e.get("cancel_reason_code"),
                                                "cancel_created_at": e["event_time"]})
        elif kind in ("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"):
            rows["payments"].append({**base, "payment_id": e["payment_id"],
                                     "status": "SUCCEEDED" if kind == "PAYMENT_SUCCEEDED" else "FAILED",
                                     "payment_method": e["payment_method"], "payment_amount": e["payment_amount"],
                                     "payment_fail_reason_code": e.get("fail_reason_code"),
                                     "payment_created_at": e["event_time"]})
        else:
            rows["refunds"].append({**base, "refund_id": e["refund_id"], "payment_id": e["payment_id"],
                                    "order_item_id": e["order_item_id"], "refund_quantity": e["refund_quantity"],
                                    "refund_amount": e["refund_amount"], "refund_type": e["refund_type"],
                                    "refund_reason_code": e.get("refund_reason_code"),
                                    "refund_created_at": e["event_time"]})
    return rows


# ── 합성 시나리오용 행 만들기 ─────────────────────────────────────────────────────────
def order(order_id, user="U1", shipping=0, at="2026-10-12T01:00:00.000Z", channel="A"):
    return {"channel": channel, "event_id": f"{order_id}-order", "order_id": order_id, "user_id": user,
            "shipping_fee": shipping, "order_created_at": at, "received_at": RECEIVED_AT}


def item(order_id, no=1, product="SKU-001", quantity=2, price=1000, discount=0, channel="A"):
    return {"channel": channel, "order_item_id": f"{order_id}-{no}", "order_id": order_id, "product_id": product,
            "product_name": None, "category": None, "quantity": quantity, "unit_price": price,
            "item_discount": discount}


def payment(order_id, no=1, status="SUCCEEDED", method="CARD", amount=2000, at="2026-10-12T01:00:30.000Z",
            user="U1", channel="A"):
    return {"channel": channel, "event_id": f"{order_id}-pay{no}", "order_id": order_id, "user_id": user,
            "payment_id": f"{order_id}-P{no}", "status": status, "payment_method": method,
            "payment_amount": amount, "payment_fail_reason_code": None if status == "SUCCEEDED" else "OTHER",
            "payment_created_at": at, "received_at": RECEIVED_AT}


def cancel(order_id, user="U1", at="2026-10-12T01:00:10.000Z", channel="A"):
    return {"channel": channel, "event_id": f"{order_id}-cancel", "order_id": order_id, "user_id": user,
            "cancel_reason_code": "OTHER", "cancel_created_at": at, "received_at": RECEIVED_AT}


def refund(order_id, no=1, item_no=1, payment_no=1, quantity=1, amount=1000, kind="RETURN", reason="DEFECT",
           at="2026-10-12T02:00:00.000Z", user="U1", channel="A"):
    return {"channel": channel, "event_id": f"{order_id}-ref{no}", "order_id": order_id, "user_id": user,
            "refund_id": f"{order_id}-R{no}", "payment_id": f"{order_id}-P{payment_no}",
            "order_item_id": f"{order_id}-{item_no}", "refund_quantity": quantity, "refund_amount": amount,
            "refund_type": kind, "refund_reason_code": reason, "refund_created_at": at, "received_at": RECEIVED_AT}


def rows_of(**tables) -> dict:
    """rows_of(orders=[...], payments=[...])처럼 일부만 주면 나머지는 빈 목록이다."""
    return {table: list(tables.get(table, [])) for table in COLUMNS}


# ── sqlite ───────────────────────────────────────────────────────────────────────
def connect(rows: dict) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute("ATTACH DATABASE ':memory:' AS silver")
    for table, columns in COLUMNS.items():
        con.execute(f"CREATE TABLE silver.{table} ({', '.join(columns)})")
        marks = ", ".join("?" for _ in columns)
        con.executemany(f"INSERT INTO silver.{table} VALUES ({marks})",
                        [[row.get(column) for column in columns] for row in rows[table]])
    return con


def run(con: sqlite3.Connection, sql: str) -> list:
    return [dict(row) for row in con.execute(sql).fetchall()]

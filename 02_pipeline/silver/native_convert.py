"""Bronze 행(channel, payload)을 공통 이벤트로 바꾼다. 파이썬 UDF 없이 Spark 표현식만 쓴다 (계획 07 카드 7-B).

[입력] Bronze 데이터프레임 (channel, payload 열이 있으면 된다), 기준 SKU 집합
[출력] 같은 데이터프레임 + 두 열
  result  struct(event_id, event_type, reason_code)  정상이면 reason_code가 NULL, 격리 대상이면 event_id·event_type이 NULL이고 사유가 있다
  e       struct(공통 이벤트 필드들, silver_tables.EVENT_SCHEMA와 같은 이름·의미. event_time은 TIMESTAMP, 정상일 때만 값이 있다)
[방식] 채널별 필드 매핑은 channel_maps.CHANNEL_MAPS(선언형 표)가 정한다. payload를 모든 값이 문자열인 스키마로 from_json하고,
  채널마다 같은 모양의 "정규화 구조체"로 바꾼 뒤 한 벌의 규칙으로 격리 사유를 정한다. 사유의 우선순위는 예전 파이썬 변환(channels/*, silver_row_to_event)과 같다.
    UNKNOWN_CHANNEL > MALFORMED_JSON > (이벤트 코드 누락) MISSING_REQUIRED > UNKNOWN_EVENT_TYPE > 공통 필드 > 유형별 필드 > INVALID_VALUE(값 범위·SKU·상품 ID 중복)
[한계 — 파이썬 변환과 다른 점] JSON 타입을 문자열로 읽으므로 "숫자가 문자열로 옴"(예: "5")은 정수로 받아들이고, ID 필드에 숫자가 오면 문자열로 받아들인다
  (파이썬은 INVALID_TYPE). 생성기·덤프의 오류는 숫자가 아닌 문자열("N/A")만 쓰므로 나타나지 않는다. 시각은 Spark 파서가 받아들이는 형식이면 된다.
Databricks(ANSI 모드)에서도 오류가 나지 않도록 try_to_timestamp와 정규식 가드를 쓴다.
pyspark는 Databricks와 로컬 테스트에서만 쓰므로 없어도 모듈을 불러올 수 있게 try/except로 import한다.
"""
try:
    import pyspark.sql.functions as F
except ImportError:  # pyspark 없는 로컬 테스트: 모듈만 불러오고 Spark 함수는 호출하지 않는다
    F = None

from channel_maps import CHANNEL_MAPS

INT_TEXT = "^-?[0-9]{1,15}$"
MISSING, INVALID_TYPE, INVALID_VALUE = "MISSING_REQUIRED", "INVALID_TYPE", "INVALID_VALUE"
ITEM_COLUMNS = ("id_raw", "id", "sku", "name", "category", "qty", "price", "discount")


# ── 작은 도구 ─────────────────────────────────────────────────────────────────────
def _get(col, dotted: str):
    """중첩 필드 경로("a.b")로 값을 꺼낸다."""
    for part in dotted.split("."):
        col = col.getField(part)
    return col


def _first(*pairs):
    """(조건, 사유) 쌍 중 처음으로 참인 조건의 사유. 모두 거짓이면 NULL."""
    expr = None
    for condition, reason in pairs:
        expr = F.when(condition, F.lit(reason)) if expr is None else expr.when(condition, F.lit(reason))
    return expr


def _int_reason(col):
    """필수 정수: 없으면 MISSING_REQUIRED, 숫자가 아니면 INVALID_TYPE, 맞으면 NULL."""
    return _first((col.isNull(), MISSING), (~col.rlike(INT_TEXT), INVALID_TYPE))


def _str_reason(col):
    """필수 문자열(ID 등): 없거나 빈 문자열이면 MISSING_REQUIRED."""
    return _first((col.isNull() | (col == ""), MISSING))


def _int(col):
    return F.when(col.rlike(INT_TEXT), col.cast("bigint"))


def _lookup(table: dict, col):
    """코드 표로 공통 값을 찾는다. 표에 없거나 값이 없으면 NULL (ANSI 모드에서도 오류가 나지 않게 try_element_at을 쓴다)."""
    mapping = F.create_map(*[F.lit(x) for pair in table.items() for x in pair])
    return F.try_element_at(mapping, col)


def _code(table: dict, col):
    """사유·결제수단·실패 코드: 표에 없거나 값이 없으면 OTHER (오류가 아니다)."""
    return F.coalesce(_lookup(table, col), F.lit("OTHER"))


# ── 채널별 정규화: 원본 -> 같은 모양의 구조체 x ──────────────────────────────────────────
def _item_struct(m: dict, i, order_no):
    """상품 행 하나를 정규화한다. 객체가 아닌 원소(NULL)는 NULL 그대로 둔다."""
    spec = m["item"]
    raw_id = None
    if isinstance(spec["id"], dict):                      # 주문 번호 + "-" + 순번 (C)
        seq = _get(i, spec["id"]["order_no_seq"])
        raw_id = seq
        item_id = F.when(order_no.isNotNull() & seq.rlike(INT_TEXT), F.concat(order_no, F.lit("-"), seq.cast("bigint").cast("string")))
    else:
        raw_id = _get(i, spec["id"])
        item_id = F.nullif(raw_id, F.lit(""))
    return F.when(i.isNotNull(), F.struct(
        raw_id.alias("id_raw"), item_id.alias("id"), F.nullif(_get(i, spec["sku"]), F.lit("")).alias("sku"),
        F.nullif(_get(i, spec["name"]), F.lit("")).alias("name"), F.nullif(_get(i, spec["category"]), F.lit("")).alias("category"),
        _get(i, spec["qty"]).alias("qty"), _get(i, spec["price"]).alias("price"), _get(i, spec["discount"]).alias("discount")))


def _normalize(channel: str):
    """채널 하나의 payload를 정규화 구조체로. 모든 채널이 같은 필드·타입을 가진다 (items 외에는 문자열)."""
    m = CHANNEL_MAPS[channel]
    p = F.from_json(F.col("payload"), m["schema"])
    f = {name: _get(p, path) for name, path in m["fields"].items() if name != "items"}
    order_no = F.nullif(f["order_id"], F.lit(""))
    refund_item = m["refund_item"]
    if isinstance(refund_item, dict):                      # C: ord_no + "-" + seq
        seq = _get(p, refund_item["order_no_seq"])
        refund_item_id = F.when(order_no.isNotNull() & seq.rlike(INT_TEXT), F.concat(order_no, F.lit("-"), seq.cast("bigint").cast("string")))
        refund_item_raw = seq
    else:
        refund_item_raw = _get(p, refund_item)
        refund_item_id = F.nullif(refund_item_raw, F.lit(""))
    items_path = "$." + m["fields"]["items"]
    items = F.transform(_get(p, m["fields"]["items"]), lambda i: _item_struct(m, i, order_no))
    return F.struct(
        F.get_json_object(F.col("payload"), "$." + m["fields"]["code"]).alias("code"),
        F.nullif(f["event_id"], F.lit("")).alias("event_id"), order_no.alias("order_id"),
        F.nullif(f["user_id"], F.lit("")).alias("user_id"), f["time"].alias("time"), f["shipping"].alias("shipping"),
        F.get_json_object(F.col("payload"), items_path).alias("items_json"), items.alias("items"),
        f["cancel_reason"].alias("cancel_reason"), F.nullif(f["payment_id"], F.lit("")).alias("payment_id"),
        f["method"].alias("method"), f["amount"].alias("amount"), f["fail"].alias("fail"),
        F.nullif(f["refund_id"], F.lit("")).alias("refund_id"), F.nullif(f["refund_payment"], F.lit("")).alias("refund_payment"),
        refund_item_id.alias("refund_item"), refund_item_raw.alias("refund_item_raw"), f["refund_qty"].alias("refund_qty"),
        f["refund_amount"].alias("refund_amount"), f["refund_type"].alias("refund_type"), f["refund_reason"].alias("refund_reason"))


def _by_channel(builder):
    """채널마다 builder(channel)로 만든 표현식을 channel 열로 고른다."""
    channel = F.col("channel")
    return F.when(channel == "A", builder("A")).when(channel == "B", builder("B")).when(channel == "C", builder("C"))


def _event_type(channel: str, x):
    return _lookup(CHANNEL_MAPS[channel]["events"], x["code"])


def _event_time(channel: str, x):
    kind = CHANNEL_MAPS[channel]["time"]
    t = x["time"]
    if kind == "iso_utc":
        return F.expr("try_to_timestamp(x.time)")
    if kind == "epoch_ms":
        return F.when(t.rlike("^[0-9]{1,15}$"), F.timestamp_millis(t.cast("bigint")))
    return F.expr("to_utc_timestamp(try_to_timestamp(x.time, 'yyyy-MM-dd HH:mm:ss'), 'Asia/Seoul')")


def _time_reason(channel: str, x, ts):
    kind = CHANNEL_MAPS[channel]["time"]
    t = x["time"]
    if kind == "epoch_ms":
        return _first((t.isNull(), MISSING), (~t.rlike(INT_TEXT), INVALID_TYPE), (ts.isNull(), INVALID_TYPE))
    return _first((t.isNull(), MISSING), (ts.isNull(), INVALID_TYPE))


# ── 격리 사유 ────────────────────────────────────────────────────────────────────────
def _item_reason(channel: str, i):
    """상품 행 하나의 사유 (파이썬 변환의 필드 읽기 순서와 같다). 정상이면 NULL."""
    seq_id = isinstance(CHANNEL_MAPS[channel]["item"]["id"], dict)
    return _first((i.isNull(), INVALID_TYPE),
                  *([(i["id_raw"].isNull(), MISSING), (~i["id_raw"].rlike(INT_TEXT), INVALID_TYPE)] if seq_id
                    else [(i["id"].isNull(), MISSING)]),
                  (i["sku"].isNull(), MISSING),
                  (i["qty"].isNull(), MISSING), (~i["qty"].rlike(INT_TEXT), INVALID_TYPE),
                  (i["price"].isNull(), MISSING), (~i["price"].rlike(INT_TEXT), INVALID_TYPE),
                  (i["discount"].isNull(), MISSING), (~i["discount"].rlike(INT_TEXT), INVALID_TYPE))


def _order_reason(channel: str, x):
    """ORDER_CREATED의 유형별 필드 사유: 배송비, 상품 행 목록, 상품 행 필드 순서."""
    per_item = F.filter(F.transform(x["items"], lambda i: _item_reason(channel, i)), lambda r: r.isNotNull())
    return F.coalesce(
        _first((x["shipping"].isNull(), MISSING), (~x["shipping"].rlike(INT_TEXT), INVALID_TYPE),
               (x["items_json"].isNull(), MISSING), (~x["items_json"].startswith("["), INVALID_TYPE),
               (x["items"].isNull(), INVALID_TYPE), (F.size(x["items"]) == 0, MISSING)),
        F.get(per_item, F.lit(0)))


def _payment_reason(x):
    return F.coalesce(_str_reason(x["payment_id"]), _first((x["method"].isNull(), MISSING)), _int_reason(x["amount"]))


def _refund_reason(channel: str, x):
    table = CHANNEL_MAPS[channel]["refund_types"]
    item_raw = x["refund_item_raw"]
    item_reason = (_int_reason(item_raw) if isinstance(CHANNEL_MAPS[channel]["refund_item"], dict) else _str_reason(x["refund_item"]))
    return F.coalesce(_first((x["refund_type"].isNull(), MISSING)),
                      _first((_lookup(table, x["refund_type"]).isNull(), INVALID_VALUE)),
                      _str_reason(x["refund_id"]), _str_reason(x["refund_payment"]), item_reason,
                      _int_reason(x["refund_qty"]), _int_reason(x["refund_amount"]))


def _value_reason(x, etype, known_skus):
    """값 범위 검사(명세 4절 INVALID_VALUE): 음수 금액, 수량 0 이하, 할인 > 단가 x 수량, 상품 ID 중복, 기준 데이터에 없는 SKU."""
    known = F.array(*[F.lit(s) for s in sorted(known_skus)])
    items = x["items"]
    no = F.lit(False)
    bad_item = F.exists(items, lambda i: F.coalesce(
        (_int(i["qty"]) <= 0) | (_int(i["price"]) < 0) | (_int(i["discount"]) < 0)
        | (_int(i["discount"]) > _int(i["price"]) * _int(i["qty"])) | ~F.array_contains(known, i["sku"]), no))
    duplicate_ids = F.size(F.array_distinct(F.transform(items, lambda i: i["id"]))) < F.size(items)
    order = (etype == "ORDER_CREATED") & F.coalesce((_int(x["shipping"]) < 0) | bad_item | duplicate_ids, no)
    payment = etype.isin("PAYMENT_SUCCEEDED", "PAYMENT_FAILED") & F.coalesce(_int(x["amount"]) < 0, no)
    refund = (etype == "REFUND_SUCCEEDED") & F.coalesce((_int(x["refund_qty"]) <= 0) | (_int(x["refund_amount"]) < 0), no)
    return _first((order | payment | refund, INVALID_VALUE))


def _reason(known_skus):
    """한 행의 격리 사유 (정상이면 NULL)."""
    channel, payload, x = F.col("channel"), F.col("payload"), F.col("x")
    etype = F.col("etype")
    ts = F.col("ts")
    type_reason = F.when(etype == "ORDER_CREATED", _by_channel(lambda ch: _order_reason(ch, x))) \
        .when(etype.isin("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"), _payment_reason(x)) \
        .when(etype == "REFUND_SUCCEEDED", _by_channel(lambda ch: _refund_reason(ch, x)))
    is_object = F.get_json_object(payload, "$").isNotNull() & payload.rlike(r"^\s*\{")
    return F.coalesce(
        _first((~channel.isin("A", "B", "C"), "UNKNOWN_CHANNEL"), (~is_object, "MALFORMED_JSON"),
               (x["code"].isNull(), MISSING), (etype.isNull(), "UNKNOWN_EVENT_TYPE")),
        _str_reason(x["event_id"]), _str_reason(x["order_id"]), _by_channel(lambda ch: _time_reason(ch, x, ts)),
        type_reason, _value_reason(x, etype, known_skus))


# ── 공통 이벤트 구조체 e ─────────────────────────────────────────────────────────────────
def _event_struct(x, etype, ts):
    """정상 행의 공통 이벤트. silver_tables.EVENT_SCHEMA와 같은 이름이다 (event_time은 TIMESTAMP)."""
    items = F.transform(x["items"], lambda i: F.struct(
        i["id"].alias("order_item_id"), i["sku"].alias("product_id"), i["name"].alias("product_name"), i["category"].alias("category"),
        _int(i["qty"]).cast("int").alias("quantity"), _int(i["price"]).alias("unit_price"), _int(i["discount"]).alias("item_discount")))
    def per_channel(table_name):
        return _by_channel(lambda ch: _code(CHANNEL_MAPS[ch][table_name], F.col("x")[{"methods": "method", "reasons": "cancel_reason", "fails": "fail"}[table_name]]))
    return F.struct(
        x["event_id"].alias("event_id"), etype.alias("event_type"), F.col("channel").alias("channel"), x["order_id"].alias("order_id"),
        x["user_id"].alias("user_id"), ts.alias("event_time"), _int(x["shipping"]).alias("shipping_fee"),
        F.when(etype == "ORDER_CREATED", items).alias("items"),
        F.when(etype == "ORDER_CANCELLED", per_channel("reasons")).alias("cancel_reason_code"),
        F.when(etype.isin("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"), x["payment_id"]).when(etype == "REFUND_SUCCEEDED", x["refund_payment"]).alias("payment_id"),
        F.when(etype.isin("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"), per_channel("methods")).alias("payment_method"),
        F.when(etype.isin("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"), _int(x["amount"])).alias("payment_amount"),
        F.when(etype == "PAYMENT_FAILED", per_channel("fails")).alias("fail_reason_code"),
        F.when(etype == "REFUND_SUCCEEDED", x["refund_id"]).alias("refund_id"),
        F.when(etype == "REFUND_SUCCEEDED", x["refund_item"]).alias("order_item_id"),
        F.when(etype == "REFUND_SUCCEEDED", _int(x["refund_qty"]).cast("int")).alias("refund_quantity"),
        F.when(etype == "REFUND_SUCCEEDED", _int(x["refund_amount"])).alias("refund_amount"),
        F.when(etype == "REFUND_SUCCEEDED", _by_channel(lambda ch: _lookup(CHANNEL_MAPS[ch]["refund_types"], x["refund_type"]))).alias("refund_type"),
        F.when(etype == "REFUND_SUCCEEDED", _by_channel(lambda ch: _code(CHANNEL_MAPS[ch]["reasons"], x["refund_reason"]))).alias("refund_reason_code"))


def convert(bronze_df, known_skus):
    """Bronze 데이터프레임에 result, e 열을 붙인다. 위 머리말 참고. UDF를 쓰지 않는다."""
    df = bronze_df.withColumn("x", _by_channel(_normalize))
    df = df.withColumn("etype", _by_channel(lambda ch: _event_type(ch, F.col("x"))))
    df = df.withColumn("ts", _by_channel(lambda ch: _event_time(ch, F.col("x"))))
    df = df.withColumn("reason", _reason(known_skus))
    ok = F.col("reason").isNull()
    event = _event_struct(F.col("x"), F.col("etype"), F.col("ts"))
    return (df.withColumn("result", F.struct(F.when(ok, F.col("x")["event_id"]).alias("event_id"),
                                              F.when(ok, F.col("etype")).alias("event_type"), F.col("reason").alias("reason_code")))
              .withColumn("e", F.when(ok, event))
              .drop("x", "etype", "ts", "reason"))

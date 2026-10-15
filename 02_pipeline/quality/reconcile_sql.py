"""Bronze 원본(payload)에서 Silver·Gold를 독립 경로로 다시 계산하는 SQL을 만든다. Spark를 모르는 순수 함수다 (SQL 문자열만 만든다).

[목적] 정답 비교(전수 대조). Silver·Gold 코드(`silver/channels/*`, `validate_*`, `gold_sql.py`)를 쓰지 않고, 이벤트 명세 2절 매핑표와 1.3·4·6.1절 규칙만 보고
  Bronze의 payload JSON을 SQL로 직접 읽어 같은 테이블을 다시 만든다. 이 결과(recon_* 임시 뷰)를 실제 Silver·Gold와 행 단위로 비교하는 것이 reconcile_rules.json이다.
  같은 해석 오류를 두 경로가 함께 하지 않도록 변환 코드를 import하거나 복사하지 않는다 (코드 값 표는 명세 2.1~2.3절을 이 파일에 따로 적었다).
[만드는 뷰] (모두 임시 뷰, 이름은 VIEW_ORDER의 순서대로 만든다)
  recon_parsed_{A,B,C}  채널별로 payload에서 필드를 꺼낸 행 (문자열·정수만. 정수는 숫자 문자열일 때만 정수로 본다)
  recon_parsed          세 채널을 합친 것
  recon_items_raw       ORDER_CREATED의 상품 행 펼침(행마다 정상 여부 item_ok)
  recon_item_check      이벤트별 상품 행 검사 (행 수, 나쁜 행 수, 서로 다른 order_item_id 수)
  recon_event_rows      모든 Bronze 행 + 정상 여부 accepted (Silver가 격리하지 않을 행)
  recon_events          정상 행에서 (channel, event_id)마다 최초 1건 (received_at, eh_partition, eh_offset 순)
  recon_orders, recon_order_items, recon_payments, recon_cancellations, recon_refunds   Silver 5개 테이블에 해당
  recon_rejected        격리되어야 할 Bronze 위치 (eh_partition, eh_offset)
  recon_order_stats     주문별 생성·결제 성공·취소 건수
  recon_valid_refunds   유효 환불 (event_spec.md 6.1절)
  recon_fact_sales_item, recon_fact_refund   Gold Fact에 해당
[한계] JSON 타입은 문자열로 읽으므로 "숫자가 문자열로 들어옴"(예: "5")은 정수로 받아들이고, Silver는 INVALID_TYPE으로 격리한다. 생성기·덤프의 오류 주입은 숫자가 아닌
  문자열("N/A")만 쓰므로 이 차이는 나타나지 않는다. 시각 형식은 Spark의 파서가 받아들이는 범위(정확한 밀리초 형식 요구 없음)로 읽는다.
Databricks(ANSI SQL 모드)에서도 오류 없이 돌도록 try_to_timestamp와 정규식 가드를 쓴다.
"""
from rules import sql_literal_list

INT_TEXT = "^-?[0-9]{1,15}$"
# recon_parsed의 열 (recon_events가 그대로 가져간다)
PARSED_COLUMNS = ("channel, eh_partition, eh_offset, received_at, event_type, event_id, order_id, user_id, event_time, shipping_fee, items_json, "
                  "cancel_reason, payment_id, payment_method, payment_amount, fail_reason, refund_id, refund_payment_id, refund_item_id, "
                  "refund_quantity, refund_amount, refund_type, refund_reason")

# 이벤트 명세 2.1~2.3절. 코드 값 표 (OTHER가 없는 환불 유형은 모르는 값이면 정상이 아니다).
# 채널마다: 이벤트 필드 경로, 시각 변환, 코드 표, 상품 행 필드. "{ord}"는 주문 번호 필드 경로 자리다.
CHANNELS = {
    "A": {
        "event_code": "$.event_type", "event_id": "$.event_id", "order_id": "$.order_id", "user_id": "$.user_id",
        "time_path": "$.occurred_at", "time": lambda t: f"try_to_timestamp({t})",
        "events": {"order.created": "ORDER_CREATED", "order.cancelled": "ORDER_CANCELLED", "payment.succeeded": "PAYMENT_SUCCEEDED",
                   "payment.failed": "PAYMENT_FAILED", "refund.succeeded": "REFUND_SUCCEEDED"},
        "methods": {"card": "CARD", "easy_pay": "EASY_PAY", "bank": "BANK_TRANSFER", "vbank": "VIRTUAL_ACCOUNT"},
        "reasons": {"change_of_mind": "CHANGE_OF_MIND", "defect": "DEFECT", "wrong_item": "WRONG_ITEM", "delay": "DELAY"},
        "fails": {"limit_exceeded": "LIMIT_EXCEEDED", "declined": "CARD_DECLINED", "timeout": "TIMEOUT"},
        "refund_types": {"cancel": "CANCEL", "return": "RETURN"},
        "shipping": "$.shipping_fee", "items": "$.items",
        "item_schema": "order_item_id string, sku string, quantity string, unit_price string, discount_amount string",
        "item": {"id": "item.order_item_id", "sku": "item.sku", "qty": "item.quantity", "price": "item.unit_price", "discount": "item.discount_amount"},
        "cancel_reason": "$.reason", "payment_id": "$.payment_id", "method": "$.method", "amount": "$.amount", "fail": "$.failure_reason",
        "refund_id": "$.refund_id", "refund_payment": "$.payment_id", "refund_item": ("plain", "$.order_item_id"),
        "refund_qty": "$.refund_quantity", "refund_amount": "$.amount", "refund_type": "$.type", "refund_reason": "$.reason",
    },
    "B": {
        "event_code": "$.eventName", "event_id": "$.eventId", "order_id": "$.orderNo", "user_id": "$.buyer.memberId",
        "time_path": "$.occurredAtMs", "time": lambda t: f"CASE WHEN {t} RLIKE '^[0-9]{{1,15}}$' THEN timestamp_millis(CAST({t} AS BIGINT)) END",
        "events": {"ORDER_PLACED": "ORDER_CREATED", "ORDER_CANCELED": "ORDER_CANCELLED", "PAY_APPROVED": "PAYMENT_SUCCEEDED",
                   "PAY_DECLINED": "PAYMENT_FAILED", "REFUND_DONE": "REFUND_SUCCEEDED"},
        "methods": {"CARD": "CARD", "SIMPLE": "EASY_PAY", "TRANSFER": "BANK_TRANSFER", "VBANK": "VIRTUAL_ACCOUNT"},
        "reasons": {"BUYER_REMORSE": "CHANGE_OF_MIND", "DEFECTIVE": "DEFECT", "WRONG_DELIVERY": "WRONG_ITEM", "DELAYED": "DELAY"},
        "fails": {"LIMIT": "LIMIT_EXCEEDED", "DECLINED": "CARD_DECLINED", "TIMEOUT": "TIMEOUT"},
        "refund_types": {"CANCEL": "CANCEL", "RETURN": "RETURN"},
        "shipping": "$.delivery.fee", "items": "$.orderItems",
        "item_schema": "lineId string, sellerSku string, qty string, unitPrice string, discount struct<total: string>",
        "item": {"id": "item.lineId", "sku": "item.sellerSku", "qty": "item.qty", "price": "item.unitPrice", "discount": "item.discount.total"},
        "cancel_reason": "$.cancelReason", "payment_id": "$.paymentId", "method": "$.payMethod", "amount": "$.paidAmount", "fail": "$.declineCode",
        "refund_id": "$.refundId", "refund_payment": "$.paymentId", "refund_item": ("plain", "$.lineId"),
        "refund_qty": "$.refundQty", "refund_amount": "$.refundAmount", "refund_type": "$.claimType", "refund_reason": "$.claimReason",
    },
    "C": {
        "event_code": "$.evt_cd", "event_id": "$.evt_id", "order_id": "$.ord_no", "user_id": "$.mem_no",
        "time_path": "$.evt_dt", "time": lambda t: f"to_utc_timestamp(try_to_timestamp({t}, 'yyyy-MM-dd HH:mm:ss'), 'Asia/Seoul')",
        "events": {"O01": "ORDER_CREATED", "O09": "ORDER_CANCELLED", "P01": "PAYMENT_SUCCEEDED", "P02": "PAYMENT_FAILED", "R01": "REFUND_SUCCEEDED"},
        "methods": {"10": "CARD", "20": "EASY_PAY", "30": "BANK_TRANSFER", "40": "VIRTUAL_ACCOUNT"},
        "reasons": {"01": "CHANGE_OF_MIND", "02": "DEFECT", "03": "WRONG_ITEM", "04": "DELAY"},
        "fails": {"F1": "LIMIT_EXCEEDED", "F2": "CARD_DECLINED", "F3": "TIMEOUT"},
        "refund_types": {"1": "CANCEL", "2": "RETURN"},
        "shipping": "$.dlv_amt", "items": "$.lines",
        "item_schema": "seq string, goods_cd string, qty string, price string, dc_amt string",
        "item": {"id": "CASE WHEN item.seq RLIKE '{int}' THEN concat({ord}, '-', CAST(item.seq AS BIGINT)) END", "sku": "item.goods_cd",
                 "qty": "item.qty", "price": "item.price", "discount": "item.dc_amt"},
        "cancel_reason": "$.rsn_cd", "payment_id": "$.pay_no", "method": "$.pay_type", "amount": "$.pay_amt", "fail": "$.fail_cd",
        "refund_id": "$.rfd_no", "refund_payment": "$.pay_no", "refund_item": ("ord_seq", "$.ord_no", "$.seq"),
        "refund_qty": "$.rfd_qty", "refund_amount": "$.rfd_amt", "refund_type": "$.clm_tp", "refund_reason": "$.rsn_cd",
    },
}


def _json(path: str) -> str:
    return f"get_json_object(payload, '{path}')"


def _text(path: str) -> str:
    """문자열 필드: 없거나 빈 문자열이면 NULL."""
    return f"NULLIF({_json(path)}, '')"


def _int_of(expr: str) -> str:
    """숫자 문자열(부호와 1~15자리 숫자)일 때만 정수, 아니면 NULL (ANSI 모드에서도 오류가 나지 않는다)."""
    return f"CASE WHEN ({expr}) RLIKE '{INT_TEXT}' THEN CAST(({expr}) AS BIGINT) END"


def _int(path: str) -> str:
    return _int_of(_json(path))


def _case(path: str, table: dict, default: str = None, null_is_null: bool = False) -> str:
    """코드 표로 값을 바꾼다. 표에 없는 값은 default(없으면 NULL). 필드가 없을 때(NULL)는 기본적으로 default를 따르고, null_is_null이면 NULL로 둔다."""
    value = _json(path)
    whens = " ".join(f"WHEN '{code}' THEN '{common}'" for code, common in table.items())
    tail = f" ELSE '{default}'" if default else ""
    sql = f"CASE {value} {whens}{tail} END"
    return f"CASE WHEN {value} IS NULL THEN NULL ELSE {sql} END" if null_is_null else sql


def parsed_sql(channel: str, bronze_table: str) -> str:
    """채널 하나의 Bronze 행에서 필드를 꺼내는 SELECT. 이벤트 유형·ID·시각·유형별 필드를 열로 만든다."""
    f = CHANNELS[channel]
    kind, *args = f["refund_item"]
    if kind == "plain":
        refund_item = _text(args[0])
    else:   # 주문 번호 + "-" + seq (seq는 정수여야 한다)
        refund_item = (f"CASE WHEN {_text(args[0])} IS NOT NULL AND ({_json(args[1])}) RLIKE '{INT_TEXT}' "
                       f"THEN concat({_text(args[0])}, '-', CAST({_json(args[1])} AS BIGINT)) END")
    columns = [
        f"channel, eh_partition, eh_offset, received_at",
        f"{_case(f['event_code'], f['events'])} AS event_type",
        f"{_text(f['event_id'])} AS event_id",
        f"{_text(f['order_id'])} AS order_id",
        f"{_text(f['user_id'])} AS user_id",
        f"{f['time'](_json(f['time_path']))} AS event_time",
        f"{_int(f['shipping'])} AS shipping_fee",
        f"{_json(f['items'])} AS items_json",
        f"{_case(f['cancel_reason'], f['reasons'], 'OTHER')} AS cancel_reason",
        f"{_text(f['payment_id'])} AS payment_id",
        f"{_case(f['method'], f['methods'], 'OTHER', null_is_null=True)} AS payment_method",
        f"{_int(f['amount'])} AS payment_amount",
        f"{_case(f['fail'], f['fails'], 'OTHER')} AS fail_reason",
        f"{_text(f['refund_id'])} AS refund_id",
        f"{_text(f['refund_payment'])} AS refund_payment_id",
        f"{refund_item} AS refund_item_id",
        f"{_int(f['refund_qty'])} AS refund_quantity",
        f"{_int(f['refund_amount'])} AS refund_amount",
        f"{_case(f['refund_type'], f['refund_types'])} AS refund_type",
        f"{_case(f['refund_reason'], f['reasons'], 'OTHER')} AS refund_reason",
    ]
    return f"SELECT {', '.join(columns)} FROM {bronze_table} WHERE channel = '{channel}'"


def items_sql(channel: str, known_skus) -> str:
    """ORDER_CREATED의 상품 행을 펼친다. 행마다 item_ok(ID·SKU·수량·단가·할인이 명세 4절 INVALID_VALUE 조건에 맞는가)를 계산한다."""
    f = CHANNELS[channel]
    ord_expr = "order_id"
    item = {key: expr.replace("{int}", INT_TEXT).replace("{ord}", ord_expr) for key, expr in f["item"].items()}
    qty, price, discount = (_int_of(item[k]) for k in ("qty", "price", "discount"))
    skus = sql_literal_list(known_skus)
    return (f"SELECT eh_partition, eh_offset, channel, order_id, "
            f"NULLIF({item['id']}, '') AS item_id, NULLIF({item['sku']}, '') AS sku, "
            f"{qty} AS quantity, {price} AS unit_price, {discount} AS item_discount, "
            f"(NULLIF({item['id']}, '') IS NOT NULL AND NULLIF({item['sku']}, '') IN ({skus}) "
            f"AND COALESCE({qty} > 0 AND {price} >= 0 AND {discount} >= 0 AND {discount} <= {price} * {qty}, FALSE)) AS item_ok "
            f"FROM (SELECT eh_partition, eh_offset, channel, order_id, "
            f"explode(from_json(items_json, 'array<struct<{f['item_schema']}>>')) AS item "
            f"FROM recon_parsed_{channel} WHERE event_type = 'ORDER_CREATED' AND items_json IS NOT NULL) x")


def view_sqls(bronze_table: str, known_skus) -> dict:
    """{뷰 이름: SELECT 문장}. VIEW_ORDER 순서로 만들어야 앞 뷰를 참조할 수 있다."""
    views = {f"recon_parsed_{ch}": parsed_sql(ch, bronze_table) for ch in CHANNELS}
    views["recon_parsed"] = " UNION ALL ".join(f"SELECT * FROM recon_parsed_{ch}" for ch in CHANNELS)
    views["recon_items_raw"] = " UNION ALL ".join(items_sql(ch, known_skus) for ch in CHANNELS)
    views["recon_item_check"] = (
        "SELECT eh_partition, eh_offset, COUNT(*) AS n_items, SUM(CASE WHEN item_ok THEN 0 ELSE 1 END) AS n_bad, "
        "COUNT(DISTINCT item_id) AS n_distinct FROM recon_items_raw GROUP BY eh_partition, eh_offset")
    views["recon_event_rows"] = (
        "SELECT p.*, CASE "
        "WHEN p.event_type IS NULL OR p.event_id IS NULL OR p.order_id IS NULL OR p.event_time IS NULL THEN FALSE "
        "WHEN p.event_type = 'ORDER_CREATED' THEN COALESCE(p.shipping_fee >= 0 AND c.n_items > 0 AND c.n_bad = 0 AND c.n_distinct = c.n_items, FALSE) "
        "WHEN p.event_type = 'ORDER_CANCELLED' THEN TRUE "
        "WHEN p.event_type IN ('PAYMENT_SUCCEEDED', 'PAYMENT_FAILED') THEN "
        "p.payment_id IS NOT NULL AND p.payment_method IS NOT NULL AND COALESCE(p.payment_amount >= 0, FALSE) "
        "WHEN p.event_type = 'REFUND_SUCCEEDED' THEN p.refund_id IS NOT NULL AND p.refund_payment_id IS NOT NULL AND p.refund_item_id IS NOT NULL "
        "AND p.refund_type IS NOT NULL AND COALESCE(p.refund_quantity > 0 AND p.refund_amount >= 0, FALSE) "
        "ELSE FALSE END AS accepted "
        "FROM recon_parsed p LEFT JOIN recon_item_check c ON c.eh_partition = p.eh_partition AND c.eh_offset = p.eh_offset")
    views["recon_events"] = (
        f"SELECT {PARSED_COLUMNS} FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, event_id "
        "ORDER BY received_at, eh_partition, eh_offset) AS rn FROM recon_event_rows WHERE accepted) WHERE rn = 1")
    views["recon_orders"] = ("SELECT channel, event_id, order_id, user_id, shipping_fee, event_time AS order_created_at "
                             "FROM recon_events WHERE event_type = 'ORDER_CREATED'")
    views["recon_order_items"] = (
        "SELECT DISTINCT i.channel, i.item_id AS order_item_id, i.order_id, i.sku AS product_id, i.quantity, i.unit_price, i.item_discount "
        "FROM recon_items_raw i JOIN recon_events e ON e.eh_partition = i.eh_partition AND e.eh_offset = i.eh_offset")
    views["recon_payments"] = (
        "SELECT channel, event_id, order_id, user_id, payment_id, "
        "CASE event_type WHEN 'PAYMENT_SUCCEEDED' THEN 'SUCCEEDED' ELSE 'FAILED' END AS status, payment_method, payment_amount, "
        "CASE event_type WHEN 'PAYMENT_FAILED' THEN fail_reason END AS payment_fail_reason_code, event_time AS payment_created_at "
        "FROM recon_events WHERE event_type IN ('PAYMENT_SUCCEEDED', 'PAYMENT_FAILED')")
    views["recon_cancellations"] = ("SELECT channel, event_id, order_id, user_id, cancel_reason AS cancel_reason_code, event_time AS cancel_created_at "
                                    "FROM recon_events WHERE event_type = 'ORDER_CANCELLED'")
    views["recon_refunds"] = (
        "SELECT channel, event_id, order_id, user_id, refund_id, refund_payment_id AS payment_id, refund_item_id AS order_item_id, "
        "refund_quantity, refund_amount, refund_type, refund_reason AS refund_reason_code, event_time AS refund_created_at "
        "FROM recon_events WHERE event_type = 'REFUND_SUCCEEDED'")
    views["recon_rejected"] = (
        f"SELECT b.eh_partition, b.eh_offset FROM {bronze_table} b WHERE b.channel IS NOT NULL AND NOT EXISTS ("
        f"SELECT 1 FROM recon_event_rows r WHERE r.accepted AND r.eh_partition = b.eh_partition AND r.eh_offset = b.eh_offset)")
    views["recon_order_stats"] = (
        "SELECT channel, order_id, SUM(created) AS n_created, SUM(paid) AS n_paid, SUM(cancelled) AS n_cancelled FROM ("
        "SELECT channel, order_id, 1 AS created, 0 AS paid, 0 AS cancelled FROM recon_orders UNION ALL "
        "SELECT channel, order_id, 0, 1, 0 FROM recon_payments WHERE status = 'SUCCEEDED' UNION ALL "
        "SELECT channel, order_id, 0, 0, 1 FROM recon_cancellations) u GROUP BY channel, order_id")
    kst = "CAST(from_utc_timestamp({}, 'Asia/Seoul') AS DATE)"
    views["recon_valid_refunds"] = (
        f"SELECT r.channel, r.refund_id, i.product_id, {kst.format('p.payment_created_at')} AS payment_date, "
        f"{kst.format('r.refund_created_at')} AS refund_date, concat(r.refund_type, '-', r.refund_reason_code) AS refund_profile_key, "
        f"r.order_item_id, r.order_id, r.payment_id, r.refund_quantity, r.refund_amount "
        f"FROM recon_refunds r JOIN recon_order_items i ON i.channel = r.channel AND i.order_item_id = r.order_item_id "
        f"JOIN recon_payments p ON p.channel = r.channel AND p.payment_id = r.payment_id AND p.status = 'SUCCEEDED' "
        f"JOIN recon_order_stats s ON s.channel = r.channel AND s.order_id = r.order_id "
        f"WHERE s.n_created = 1 AND s.n_paid = 1 AND s.n_cancelled = 0 AND r.order_id = i.order_id AND r.order_id = p.order_id")
    views["recon_fact_refund"] = "SELECT * FROM recon_valid_refunds"
    views["recon_fact_sales_item"] = (
        f"SELECT i.channel, i.order_item_id, i.product_id, {kst.format('p.payment_created_at')} AS payment_date, p.payment_method, "
        f"i.order_id, o.user_id, p.payment_id, p.payment_created_at AS payment_at, i.quantity, "
        f"i.unit_price * i.quantity AS item_gross_amount, i.item_discount, i.unit_price * i.quantity - i.item_discount AS item_paid_amount, "
        f"COALESCE(v.refund_quantity, 0) AS refund_quantity, COALESCE(v.refund_amount, 0) AS refund_amount, v.last_refund_date "
        f"FROM recon_order_items i JOIN recon_order_stats s ON s.channel = i.channel AND s.order_id = i.order_id "
        f"JOIN recon_orders o ON o.channel = i.channel AND o.order_id = i.order_id "
        f"JOIN recon_payments p ON p.channel = i.channel AND p.order_id = i.order_id AND p.status = 'SUCCEEDED' "
        f"LEFT JOIN (SELECT channel, order_item_id, SUM(refund_quantity) AS refund_quantity, SUM(refund_amount) AS refund_amount, "
        f"MAX(refund_date) AS last_refund_date FROM recon_valid_refunds GROUP BY channel, order_item_id) v "
        f"ON v.channel = i.channel AND v.order_item_id = i.order_item_id "
        f"WHERE s.n_created = 1 AND s.n_paid = 1 AND s.n_cancelled = 0")
    return views


VIEW_ORDER = (["recon_parsed_A", "recon_parsed_B", "recon_parsed_C", "recon_parsed", "recon_items_raw", "recon_item_check",
               "recon_event_rows", "recon_events", "recon_orders", "recon_order_items", "recon_payments", "recon_cancellations",
               "recon_refunds", "recon_rejected", "recon_order_stats", "recon_valid_refunds", "recon_fact_refund", "recon_fact_sales_item"])

# 실제 테이블과 비교하는 열 (Silver는 {silver}.테이블, Gold는 {gold}.테이블). 이름은 두 쪽에서 같다.
COMPARE = {
    "silver": {
        "orders": ("recon_orders", ["channel", "event_id", "order_id", "user_id", "shipping_fee", "order_created_at"]),
        "order_items": ("recon_order_items", ["channel", "order_item_id", "order_id", "product_id", "quantity", "unit_price", "item_discount"]),
        "payments": ("recon_payments", ["channel", "event_id", "order_id", "user_id", "payment_id", "status", "payment_method",
                                        "payment_amount", "payment_fail_reason_code", "payment_created_at"]),
        "order_cancellations": ("recon_cancellations", ["channel", "event_id", "order_id", "user_id", "cancel_reason_code", "cancel_created_at"]),
        "refunds": ("recon_refunds", ["channel", "event_id", "order_id", "user_id", "refund_id", "payment_id", "order_item_id",
                                      "refund_quantity", "refund_amount", "refund_type", "refund_reason_code", "refund_created_at"]),
    },
    "gold": {
        "fact_sales_item": ("recon_fact_sales_item", ["channel", "order_item_id", "product_id", "payment_date", "payment_method", "order_id",
                                                      "user_id", "payment_id", "payment_at", "quantity", "item_gross_amount", "item_discount",
                                                      "item_paid_amount", "refund_quantity", "refund_amount", "last_refund_date"]),
        "fact_refund": ("recon_fact_refund", ["channel", "refund_id", "product_id", "payment_date", "refund_date", "refund_profile_key",
                                              "order_item_id", "order_id", "payment_id", "refund_quantity", "refund_amount"]),
    },
}


def diff_count_sql(recon_view: str, actual: str, columns: list) -> str:
    """두 쪽의 차이 건수: (재계산에만 있는 행) + (실제 테이블에만 있는 행). 0이어야 한다. 행은 columns의 값 전체로 비교한다."""
    cols = ", ".join(columns)
    return (f"SELECT (SELECT COUNT(*) FROM (SELECT {cols} FROM {recon_view} EXCEPT SELECT {cols} FROM {actual}) a) "
            f"+ (SELECT COUNT(*) FROM (SELECT {cols} FROM {actual} EXCEPT SELECT {cols} FROM {recon_view}) b) AS violations")


def quarantine_diff_sql(silver_placeholder: str = "{silver}") -> str:
    """격리 위치 차이: 재계산이 격리해야 한다고 본 Bronze 위치와 Silver quarantine의 위치가 다른 건수."""
    return (f"SELECT (SELECT COUNT(*) FROM (SELECT eh_partition, eh_offset FROM recon_rejected EXCEPT "
            f"SELECT eh_partition, eh_offset FROM {silver_placeholder}.quarantine) a) "
            f"+ (SELECT COUNT(*) FROM (SELECT eh_partition, eh_offset FROM {silver_placeholder}.quarantine EXCEPT "
            f"SELECT eh_partition, eh_offset FROM recon_rejected) b) AS violations")


def materialize(spark, name: str) -> None:
    """뷰 name의 결과를 계산해 두고 이후 뷰가 그 결과를 읽게 한다 (뷰를 겹쳐 쓰면 같은 계산이 계획에 수십 번 펼쳐져 느려지기 때문이다).

    localCheckpoint로 계획(lineage)을 끊는다. 이 기능이 없는 컴퓨트(서버리스)에서는 건너뛰고 뷰 그대로 둔다 (느려질 수 있다).
    """
    try:
        spark.table(name).localCheckpoint(eager=True).createOrReplaceTempView(name)
    except Exception:   # noqa: BLE001  지원하지 않는 컴퓨트면 뷰 그대로 쓴다
        pass


def create_views(spark, bronze_table: str, known_skus, materialize_views: bool = True) -> None:
    """recon_* 임시 뷰를 순서대로 만든다 (Spark가 있는 곳에서만 실행). 앞 뷰의 결과를 계산해 두고 다음 뷰가 읽는다."""
    sqls = view_sqls(bronze_table, known_skus)
    for name in VIEW_ORDER:
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {name} AS {sqls[name]}")
        if materialize_views:
            materialize(spark, name)


def rule_list() -> list[dict]:
    """reconcile_rules.json에 들어갈 규칙 목록 (집합 규칙, 전수). 규칙 파일은 이 함수로 만들고 테스트가 같은지 확인한다."""
    rules = []
    for layer, tables in COMPARE.items():
        for table, (view, columns) in tables.items():
            rules.append({"id": f"reconcile_{layer}_{table}", "kind": "set", "layer": layer, "severity": "error", "scope": "full",
                          "description": f"{layer}.{table}: Bronze에서 다시 계산한 결과와 다른 행 수 (재계산에만 있는 행 + 실제에만 있는 행)",
                          "sql": diff_count_sql(view, "{" + layer + "}." + table, columns)})
    rules.append({"id": "reconcile_silver_quarantine", "kind": "set", "layer": "silver", "severity": "error", "scope": "full",
                  "description": "silver.quarantine: Bronze에서 다시 계산한 격리 위치와 다른 행 수",
                  "sql": quarantine_diff_sql()})
    return rules

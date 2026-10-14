"""Fact를 Silver에서 다시 계산하는 SQL 문장을 만든다. 기준은 event_spec.md 6.1절이다.

[입력] Silver 스키마 이름(예: "commerce_poc.silver")
[출력] SELECT 문장 문자열. 컬럼 이름·순서는 gold_tables.TABLES와 같다 (MERGE의 `UPDATE SET *`·`INSERT *`가 이름으로 맞춘다).
[규칙]
  fact_sales_item  order_items ⨝ orders ⨝ payments(SUCCEEDED, order_id). 취소가 있는 주문은 제외 (취소+결제 성공).
                   환불은 (channel, order_item_id)별 합계를 붙인다 (없으면 0).
  fact_refund      refunds ⨝ order_items ⨝ orders ⨝ payments(SUCCEEDED, payment_id = 원결제). 취소가 있는 주문의 환불은 제외.
조인이 안 되는 행(결제·환불이 먼저 도착)은 이번 결과에서 빠지고 짝이 도착한 다음 실행에서 들어온다.
조인·집계·COALESCE·EXISTS만 써서 로컬에서 sqlite로도 같은 문장을 실행해 볼 수 있다. 달라지는 것은 KST 일자 변환뿐이라
date_expr 인자로 바꿀 수 있다 (기본은 Spark용 kst_date).
이 파일은 SQL 문자열만 만든다. pyspark는 쓰지 않는다.
"""


def kst_date(column: str) -> str:
    """UTC TIMESTAMP 컬럼을 KST 날짜(DATE)로 바꾸는 Spark SQL 식. 일자는 UTC가 아니라 KST 기준이다 (event_spec.md 6.3절)."""
    return f"CAST(from_utc_timestamp({column}, 'Asia/Seoul') AS DATE)"


def _has_cancellation(silver: str, alias: str) -> str:
    """alias 주문에 취소 이벤트가 있다는 조건."""
    return (f"EXISTS (SELECT 1 FROM {silver}.order_cancellations c "
            f"WHERE c.channel = {alias}.channel AND c.order_id = {alias}.order_id)")


def sales_item_sql(silver: str, date_expr=kst_date) -> str:
    """fact_sales_item: 결제 성공한 주문상품 1행."""
    return (
        f"SELECT i.channel AS channel, i.order_item_id AS order_item_id, i.product_id AS product_id, "
        f"{date_expr('p.payment_created_at')} AS payment_date, p.payment_method AS payment_method, "
        f"i.order_id AS order_id, o.user_id AS user_id, p.payment_id AS payment_id, "
        f"p.payment_created_at AS payment_at, i.quantity AS quantity, "
        f"i.unit_price * i.quantity AS item_gross_amount, i.item_discount AS item_discount, "
        f"i.unit_price * i.quantity - i.item_discount AS item_paid_amount, "
        f"COALESCE(r.refund_quantity, 0) AS refund_quantity, COALESCE(r.refund_amount, 0) AS refund_amount, "
        f"r.last_refund_date AS last_refund_date "
        f"FROM {silver}.order_items i "
        f"JOIN {silver}.orders o ON o.channel = i.channel AND o.order_id = i.order_id "
        f"JOIN {silver}.payments p ON p.channel = i.channel AND p.order_id = i.order_id AND p.status = 'SUCCEEDED' "
        f"LEFT JOIN (SELECT channel, order_item_id, CAST(SUM(refund_quantity) AS INT) AS refund_quantity, "
        f"SUM(refund_amount) AS refund_amount, MAX({date_expr('refund_created_at')}) AS last_refund_date "
        f"FROM {silver}.refunds GROUP BY channel, order_item_id) r "
        f"ON r.channel = i.channel AND r.order_item_id = i.order_item_id "
        f"WHERE NOT {_has_cancellation(silver, 'i')}"
    )


def refund_sql(silver: str, date_expr=kst_date) -> str:
    """fact_refund: 환불 이벤트 1행. payment_date는 원결제의 결제일이다."""
    return (
        f"SELECT r.channel AS channel, r.refund_id AS refund_id, i.product_id AS product_id, "
        f"{date_expr('p.payment_created_at')} AS payment_date, {date_expr('r.refund_created_at')} AS refund_date, "
        f"r.refund_type || '-' || COALESCE(r.refund_reason_code, 'OTHER') AS refund_profile_key, "
        f"r.order_item_id AS order_item_id, r.order_id AS order_id, r.payment_id AS payment_id, "
        f"r.refund_quantity AS refund_quantity, r.refund_amount AS refund_amount "
        f"FROM {silver}.refunds r "
        f"JOIN {silver}.order_items i ON i.channel = r.channel AND i.order_item_id = r.order_item_id "
        f"JOIN {silver}.orders o ON o.channel = r.channel AND o.order_id = r.order_id "
        f"JOIN {silver}.payments p ON p.channel = r.channel AND p.payment_id = r.payment_id AND p.status = 'SUCCEEDED' "
        f"WHERE NOT {_has_cancellation(silver, 'r')}"
    )

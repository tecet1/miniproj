"""Fact를 Silver에서 다시 계산하는 SQL 문장을 만든다. 기준은 event_spec.md 6.1절이다.

[입력] Silver 스키마 이름(예: "commerce_poc.silver")
[출력] SELECT 문장 문자열. 컬럼 이름·순서는 gold_tables.TABLES와 같다 (MERGE의 `UPDATE SET *`·`INSERT *`가 이름으로 맞춘다).
[규칙]
  fact_sales_item  order_items ⨝ orders ⨝ payments(SUCCEEDED, order_id). 취소가 있는 주문과 업무 키 위반 주문은 제외.
                   환불은 유효 환불의 (channel, order_item_id)별 합계를 붙인다 (없으면 0).
  fact_refund      유효 환불 1건 = 1행. 취소가 있는 주문과 업무 키 위반 주문의 환불은 제외.
[업무 키 위반 주문 (2026-10-15)] 주문 생성이 2건 이상이거나 결제 성공이 2건 이상인 주문. 조인으로 상품 행이 늘어 금액이 중복 집계되는 것을
  막으려고 두 Fact에서 제외하며, 건수는 violation_order_count_sql로 보고한다 (PoC 단순화 선택이지 업무 규칙이 아니다).
[유효 환불 (2026-10-15)] 환불의 payment_id가 SUCCEEDED 결제를 가리키고, 환불·상품 행·결제의 order_id가 모두 같다.
  두 Fact가 같은 유효 환불 집합(valid_refund_sql)을 쓰므로 환불 합계가 서로 어긋나지 않는다.
  결제·상품 행이 아직 도착하지 않은 환불은 무효가 아니라 보류다 (다음 실행에서 짝이 도착하면 들어온다). invalid_refund_count_sql은 보류를 세지 않는다.
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


def violation_orders_sql(silver: str) -> str:
    """업무 키 위반 주문의 (channel, order_id) 목록: 주문 생성 2건 이상 또는 결제 성공 2건 이상."""
    return (f"SELECT channel, order_id FROM {silver}.orders GROUP BY channel, order_id HAVING COUNT(*) > 1 "
            f"UNION SELECT channel, order_id FROM {silver}.payments WHERE status = 'SUCCEEDED' "
            f"GROUP BY channel, order_id HAVING COUNT(*) > 1")


def _not_violating(silver: str, alias: str):
    """(JOIN 조각, WHERE 조건): alias 주문이 업무 키 위반 목록에 없다는 조건. 상관 서브쿼리 대신 LEFT JOIN으로 쓴다."""
    join = (f"LEFT JOIN ({violation_orders_sql(silver)}) vo "
            f"ON vo.channel = {alias}.channel AND vo.order_id = {alias}.order_id")
    return join, "vo.order_id IS NULL"


def _restrict(alias: str, view, key: str) -> str:
    """증분 재계산용 범위 제한: 작은 뷰(channel, key)와 조인해 그 키의 행만 남긴다. view가 없으면 제한하지 않는다."""
    return f"JOIN {view} scope_{key} ON scope_{key}.channel = {alias}.channel AND scope_{key}.{key} = {alias}.{key} " if view else ""


def valid_refund_sql(silver: str, date_expr=kst_date, orders=None, refunds=None) -> str:
    """유효 환불 1건 = 1행 (취소·업무 키 위반 주문의 환불 제외). 열은 두 Fact가 필요한 것만이다.

    orders·refunds는 증분 재계산 범위다: (channel, order_id) 뷰이면 그 주문의 환불만, (channel, refund_id) 뷰이면 그 환불만 계산한다 (None이면 전체).
    """
    violation_join, not_violating = _not_violating(silver, "r")
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
        f"{_restrict('r', orders, 'order_id')}{_restrict('r', refunds, 'refund_id')}"
        f"{violation_join} "
        f"WHERE r.order_id = i.order_id AND r.order_id = p.order_id AND {not_violating} "
        f"AND NOT {_has_cancellation(silver, 'r')}"
    )


def sales_item_sql(silver: str, date_expr=kst_date, orders=None) -> str:
    """fact_sales_item: 결제 성공한 주문상품 1행. orders가 (channel, order_id) 뷰이면 그 주문의 행만 계산한다 (증분 재계산)."""
    violation_join, not_violating = _not_violating(silver, "i")
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
        f"{_restrict('i', orders, 'order_id')}"
        f"LEFT JOIN (SELECT channel, order_item_id, CAST(SUM(refund_quantity) AS INT) AS refund_quantity, "
        f"SUM(refund_amount) AS refund_amount, MAX(refund_date) AS last_refund_date "
        f"FROM ({valid_refund_sql(silver, date_expr, orders=orders)}) v GROUP BY channel, order_item_id) r "
        f"ON r.channel = i.channel AND r.order_item_id = i.order_item_id "
        f"{violation_join} "
        f"WHERE NOT {_has_cancellation(silver, 'i')} AND {not_violating}"
    )


def refund_sql(silver: str, date_expr=kst_date, refunds=None) -> str:
    """fact_refund: 유효 환불 이벤트 1행. payment_date는 원결제의 결제일이다. refunds가 (channel, refund_id) 뷰이면 그 환불만 계산한다 (증분 재계산)."""
    return valid_refund_sql(silver, date_expr, refunds=refunds)


def violation_order_count_sql(silver: str) -> str:
    """업무 키 위반으로 Gold에서 제외된 주문 수 (event_spec.md 6.6절 보고용)."""
    return f"SELECT COUNT(*) AS n FROM ({violation_orders_sql(silver)}) v"


def violation_order_refund_count_sql(silver: str) -> str:
    """업무 키 위반 주문에 붙은 환불 수. 이 환불은 무효 환불이 아니라 위반 주문 쪽으로 세고 Gold에서 제외된다 (겹치는 경우를 한 곳에만 센다)."""
    return (f"SELECT COUNT(*) AS n FROM {silver}.refunds r JOIN ({violation_orders_sql(silver)}) vo "
            f"ON vo.channel = r.channel AND vo.order_id = r.order_id")


def invalid_refund_count_sql(silver: str) -> str:
    """무효 환불 수 (6.6절 보고용). 위반 주문의 환불은 세지 않는다. 결제·상품 행이 아직 없는 환불(보류)도 세지 않는다.

    무효 = 환불의 payment_id를 가진 결제가 있는데 (같은 order_id의 SUCCEEDED 결제가 없다), 또는
           환불의 order_item_id를 가진 상품 행이 있는데 (같은 order_id의 행이 없다).
    """
    violation_join, not_violating = _not_violating(silver, "r")
    payment_exists = (f"EXISTS (SELECT 1 FROM {silver}.payments q WHERE q.channel = r.channel AND q.payment_id = r.payment_id)")
    payment_ok = (f"EXISTS (SELECT 1 FROM {silver}.payments q WHERE q.channel = r.channel AND q.payment_id = r.payment_id "
                  f"AND q.status = 'SUCCEEDED' AND q.order_id = r.order_id)")
    item_exists = (f"EXISTS (SELECT 1 FROM {silver}.order_items t WHERE t.channel = r.channel AND t.order_item_id = r.order_item_id)")
    item_ok = (f"EXISTS (SELECT 1 FROM {silver}.order_items t WHERE t.channel = r.channel AND t.order_item_id = r.order_item_id "
               f"AND t.order_id = r.order_id)")
    return (f"SELECT COUNT(*) AS n FROM {silver}.refunds r {violation_join} "
            f"WHERE {not_violating} AND (({payment_exists} AND NOT {payment_ok}) OR ({item_exists} AND NOT {item_ok}))")

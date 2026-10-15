"""Silver가 일부러 그대로 두는 위반의 건수를 보고한다. 노트북에서 호출만 한다.

Silver의 자체 정합성 검사(PK 중복, 결제금액 = 상품합 + 배송비, 격리 행 원본 보존)는 규칙 목록(02_pipeline/quality/rules.json)으로 옮겼다 (계획 07 카드 7-C).
노트북은 run_checks.check_layer(spark, schemas, "silver")로 그 규칙을 돌리고, 결과는 commerce_poc.quality.results에 남는다.

[보고 항목] report_by_design은 event_spec.md 1.3절에 따라 Silver가 그대로 두고 Gold가 제외·보고하는 위반의 건수를 돌려준다. 오류가 섞인 데이터에서는 0이 아닌 것이 정상이라 실패로 보지 않는다.
  - 업무 ID 중복: orders·order_cancellations는 (channel, order_id), refunds는 (channel, refund_id)
  - 참조 누락: refunds -> order_items(channel, order_item_id), refunds -> payments(SUCCEEDED, channel+payment_id)
    (원인: 주문 생성이 격리되어 상품 행이 없음, 환불이 실패한 결제를 가리킴, 성공 결제가 격리됨, 도착 순서)
Spark 조회 함수는 Databricks에서만 돈다.
"""


def count_orphans(spark, schema: str) -> dict:
    """환불이 가리키는 상품 행·성공 결제가 없는 건수."""
    def one(sql: str) -> int:
        return spark.sql(sql).first()["n"]

    return {
        "refunds->order_items": one(
            f"SELECT COUNT(*) AS n FROM {schema}.refunds r LEFT ANTI JOIN {schema}.order_items i "
            f"ON r.channel = i.channel AND r.order_item_id = i.order_item_id"),
        "refunds->payments(SUCCEEDED)": one(
            f"SELECT COUNT(*) AS n FROM {schema}.refunds r LEFT ANTI JOIN "
            f"(SELECT * FROM {schema}.payments WHERE status = 'SUCCEEDED') p "
            f"ON r.channel = p.channel AND r.payment_id = p.payment_id"),
    }


def count_business_id_duplicates(spark, schema: str) -> dict:
    """업무 ID가 겹치는 키 수. payment_id는 실패·성공 이벤트가 공유할 수 있어 검사하지 않는다."""
    targets = {"orders": "channel, order_id", "order_cancellations": "channel, order_id", "refunds": "channel, refund_id"}
    return {table: spark.sql(f"SELECT COUNT(*) AS n FROM (SELECT {keys} FROM {schema}.{table} "
                             f"GROUP BY {keys} HAVING COUNT(*) > 1)").first()["n"]
            for table, keys in targets.items()}


def report_by_design(spark, schema: str) -> dict:
    """Silver가 일부러 그대로 두는 위반의 건수 (업무 ID 중복, 참조 누락). 실패가 아니라 보고다."""
    return {**{f"업무 ID 중복 {name}": n for name, n in count_business_id_duplicates(spark, schema).items()},
            **{f"참조 누락 {name}": n for name, n in count_orphans(spark, schema).items()}}

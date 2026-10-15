"""Silver 변경분에서 바뀐 주문만 다시 계산해 Gold Fact에 반영한다 (계획 07 카드 7-E). 기준은 event_spec.md 6.1·6.4절이다.

[왜 다시 계산하는가] Gold 한 행은 여러 Silver 테이블(주문·상품·결제·취소·환불)을 조인한 결과라서, 한 이벤트가 바뀌어도 그 주문의 행 전체가 달라질 수 있다.
  그래서 "바뀐 이벤트"가 아니라 "바뀐 주문"을 정하고 그 주문의 Fact 행을 전체 재계산 SQL(gold_sql)과 같은 식으로 다시 만든다.
[범위(영향 받는 주문)] 변경 행(gold_changes 뷰: channel, order_id, order_item_id, payment_id, refund_id)에서
  1. 변경 행의 order_id
  2. 변경된 환불이 가리키는 상품 행의 주문, 결제의 주문 (환불 · 상품 · 결제의 order_id가 다를 수 있으므로 세 곳을 모두 본다)
  을 모은 것이 gold_affected_orders(channel, order_id)다. 이 주문들의 환불과, 변경된 환불 자신이 gold_affected_refunds(channel, refund_id)다.
[반영] 범위에 있는 Fact 행을 새로 계산한 결과로 바꾼다: 새 결과에 있으면 갱신·추가, 범위 안의 기존 행이 새 결과에 없으면 삭제
  (예: 나중에 취소나 이중 결제가 도착해 제외 대상이 된 주문). MERGE 한 번에 삭제 표시(_delete)를 함께 넣어 한 번에 적용한다.
[일관성 검사] 반영 직후 범위 안의 Gold 행이 다시 계산한 결과와 같은지(차이 0) 확인해 quality.results에 남긴다 (규칙 gold_batch_consistency).
순수 SQL 문자열을 만드는 함수는 pyspark 없이 로컬(sqlite)에서 테스트한다. Spark·Delta 부분(apply_changes, run_gold_stream)은 Databricks에서만 돌고 아직 미검증이다.
"""
import gold_sql
from gold_tables import TABLES, column_names

CHANGES = "gold_changes"
AFFECTED_ORDERS = "gold_affected_orders"
AFFECTED_REFUNDS = "gold_affected_refunds"
SILVER_SOURCES = ("orders", "order_items", "order_cancellations", "payments", "refunds")
RULE_ID = "gold_batch_consistency"


def affected_orders_sql(silver: str, changes: str = CHANGES) -> str:
    """영향 받는 주문 (channel, order_id): 변경 행의 주문 + 변경된 환불이 가리키는 상품 행·결제의 주문."""
    return (f"SELECT DISTINCT channel, order_id FROM ("
            f"SELECT channel, order_id FROM {changes} WHERE order_id IS NOT NULL "
            f"UNION SELECT c.channel, i.order_id FROM {changes} c JOIN {silver}.order_items i "
            f"ON i.channel = c.channel AND i.order_item_id = c.order_item_id WHERE c.refund_id IS NOT NULL "
            f"UNION SELECT c.channel, p.order_id FROM {changes} c JOIN {silver}.payments p "
            f"ON p.channel = c.channel AND p.payment_id = c.payment_id WHERE c.refund_id IS NOT NULL) a")


def affected_refunds_sql(silver: str, orders: str = AFFECTED_ORDERS, changes: str = CHANGES) -> str:
    """영향 받는 환불 (channel, refund_id): 변경된 환불 + 영향 받는 주문에 걸린 환불(환불 자신의 주문, 가리키는 상품 행의 주문, 결제의 주문)."""
    return (f"SELECT DISTINCT channel, refund_id FROM ("
            f"SELECT channel, refund_id FROM {changes} WHERE refund_id IS NOT NULL "
            f"UNION SELECT r.channel, r.refund_id FROM {silver}.refunds r JOIN {orders} a ON a.channel = r.channel AND a.order_id = r.order_id "
            f"UNION SELECT r.channel, r.refund_id FROM {silver}.refunds r JOIN {silver}.order_items i "
            f"ON i.channel = r.channel AND i.order_item_id = r.order_item_id JOIN {orders} a ON a.channel = i.channel AND a.order_id = i.order_id "
            f"UNION SELECT r.channel, r.refund_id FROM {silver}.refunds r JOIN {silver}.payments p "
            f"ON p.channel = r.channel AND p.payment_id = r.payment_id JOIN {orders} a ON a.channel = p.channel AND a.order_id = p.order_id) x")


def new_rows_sql(table: str, silver: str, date_expr=gold_sql.kst_date) -> str:
    """범위 안의 Fact 행을 다시 계산하는 SELECT."""
    if table == "fact_sales_item":
        return gold_sql.sales_item_sql(silver, date_expr, orders=AFFECTED_ORDERS)
    return gold_sql.refund_sql(silver, date_expr, refunds=AFFECTED_REFUNDS)


def scope_keys_sql(table: str, gold: str) -> str:
    """범위 안에 이미 있는 Gold 행의 키 (삭제 대상 후보)."""
    if table == "fact_sales_item":
        return (f"SELECT t.channel, t.order_item_id FROM {gold}.fact_sales_item t "
                f"JOIN {AFFECTED_ORDERS} a ON a.channel = t.channel AND a.order_id = t.order_id")
    return (f"SELECT t.channel, t.refund_id FROM {gold}.fact_refund t "
            f"JOIN {AFFECTED_REFUNDS} a ON a.channel = t.channel AND a.refund_id = t.refund_id")


def merge_source_sql(table: str, silver: str, gold: str, date_expr=gold_sql.kst_date) -> str:
    """MERGE 소스: 새로 계산한 행(_delete = FALSE) + 범위 안의 기존 행 중 새 결과에 없는 키(_delete = TRUE, 나머지 열은 NULL)."""
    keys = TABLES[table]["pk"]
    columns = column_names(table)
    new = new_rows_sql(table, silver, date_expr)
    kept = ", ".join(f"n.{c}" for c in columns)
    marks = ", ".join(f"e.{c}" if c in keys else f"CAST(NULL AS {dict(TABLES[table]['columns'])[c]})" for c in columns)
    on = " AND ".join(f"n2.{k} = e.{k}" for k in keys)
    return (f"SELECT {kept}, FALSE AS _delete FROM ({new}) n "
            f"UNION ALL SELECT {marks}, TRUE AS _delete FROM ({scope_keys_sql(table, gold)}) e "
            f"WHERE NOT EXISTS (SELECT 1 FROM ({new}) n2 WHERE {on})")


def scoped_merge_sql(table: str, silver: str, gold: str, target: str, date_expr=gold_sql.kst_date) -> str:
    """범위 안의 Fact 행을 새 결과로 바꾸는 MERGE 문장 (삭제 표시가 있는 키는 지운다)."""
    keys = TABLES[table]["pk"]
    columns = column_names(table)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    assignments = ", ".join(f"t.{c} = s.{c}" for c in columns if c not in keys)
    values = ", ".join(f"s.{c}" for c in columns)
    return (f"MERGE INTO {target} AS t USING ({merge_source_sql(table, silver, gold, date_expr)}) AS s ON {on} "
            f"WHEN MATCHED AND s._delete THEN DELETE "
            f"WHEN MATCHED THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED AND NOT s._delete THEN INSERT ({', '.join(columns)}) VALUES ({values})")


def scoped_target_sql(table: str, gold: str) -> str:
    """범위 안에 있는 Gold 행 (모든 열)."""
    columns = ", ".join(f"g.{c}" for c in column_names(table))
    if table == "fact_sales_item":
        return (f"SELECT {columns} FROM {gold}.fact_sales_item g "
                f"JOIN {AFFECTED_ORDERS} a ON a.channel = g.channel AND a.order_id = g.order_id")
    return (f"SELECT {columns} FROM {gold}.fact_refund g "
            f"JOIN {AFFECTED_REFUNDS} a ON a.channel = g.channel AND a.refund_id = g.refund_id")


def consistency_sql(table: str, silver: str, gold: str, date_expr=gold_sql.kst_date) -> str:
    """반영 직후 범위 안의 Gold 행과 다시 계산한 행이 다른 건수 (재계산에만 있는 행 + Gold에만 있는 행). 0이어야 한다."""
    columns = ", ".join(column_names(table))
    new = new_rows_sql(table, silver, date_expr)
    scoped = scoped_target_sql(table, gold)
    return (f"SELECT (SELECT COUNT(*) FROM (SELECT {columns} FROM ({new}) a EXCEPT SELECT {columns} FROM ({scoped}) b) x) "
            f"+ (SELECT COUNT(*) FROM (SELECT {columns} FROM ({scoped}) b EXCEPT SELECT {columns} FROM ({new}) a) y) AS violations")


# ── Databricks 전용 (Delta 변경 데이터 피드, 구조적 스트리밍). 로컬에서는 돌지 않는다 (미검증) ─────────────────────────────
def enable_change_feed(spark, silver: str) -> None:
    """Silver 이벤트 테이블 5개에 변경 데이터 피드를 켠다. 이미 켜져 있어도 안전하다. 켠 뒤의 변경부터 읽을 수 있다."""
    for table in SILVER_SOURCES:
        spark.sql(f"ALTER TABLE {silver}.{table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


def latest_versions(spark, silver: str) -> dict:
    """Silver 테이블 5개의 현재 Delta 버전. 처음 전체 계산 직전에 기록해 두고 그 다음 버전부터 스트림이 읽게 한다."""
    return {table: spark.sql(f"DESCRIBE HISTORY {silver}.{table} LIMIT 1").first()["version"] for table in SILVER_SOURCES}


def bootstrap(spark, silver: str, gold: str) -> dict:
    """처음 한 번: 변경 피드를 켜고 버전을 기록한 뒤 Gold를 전체 계산한다. 돌려주는 버전부터 증분 스트림을 시작한다.

    버전을 기록한 뒤 전체 계산 사이에 들어온 변경은 스트림이 한 번 더 다시 계산한다 (같은 결과라 안전하다).
    """
    import gold_build
    enable_change_feed(spark, silver)
    versions = latest_versions(spark, silver)
    gold_build.build_gold(spark, silver, gold)
    return versions


def read_changes_stream(spark, silver: str, starting_versions: dict = None):
    """Silver 5개 테이블의 새 행을 (channel, order_id, order_item_id, payment_id, refund_id) 한 줄씩으로 읽는 스트림."""
    import pyspark.sql.functions as F
    columns = {"orders": ("order_id",), "order_items": ("order_id", "order_item_id"), "order_cancellations": ("order_id",),
               "payments": ("order_id", "payment_id"), "refunds": ("order_id", "order_item_id", "payment_id", "refund_id")}
    frames = []
    for table in SILVER_SOURCES:
        reader = spark.readStream.option("readChangeFeed", "true")
        if starting_versions:
            reader = reader.option("startingVersion", starting_versions[table] + 1)
        frame = reader.table(f"{silver}.{table}").where(F.col("_change_type") == "insert")
        names = ("order_id", "order_item_id", "payment_id", "refund_id")
        frames.append(frame.select("channel", *[F.col(n) if n in columns[table] else F.lit(None).cast("string").alias(n) for n in names]))
    stream = frames[0]
    for frame in frames[1:]:
        stream = stream.unionByName(frame)
    return stream


def _materialize(spark, name: str) -> None:
    """작은 범위 뷰를 한 번 계산해 두고 이후 쿼리가 그 결과를 읽게 한다 (localCheckpoint가 없는 컴퓨트에서는 건너뛴다)."""
    try:
        spark.table(name).localCheckpoint(eager=True).createOrReplaceTempView(name)
    except Exception:   # noqa: BLE001
        pass


def apply_changes(spark, silver: str, gold: str, changes_df, results_table=None, batch_id=None) -> dict:
    """변경 행 한 묶음을 Gold에 반영하고 {Fact 이름: 일관성 위반 건수}를 돌려준다.

    1. 영향 받는 주문·환불을 정한다. 2. 새로 계산한 행의 PK 중복을 검사한다 (중복이면 SourceKeyError, 아무것도 쓰지 않는다).
    3. Fact마다 범위 안의 행을 MERGE로 바꾼다. 4. 반영 직후 범위 안의 Gold 행이 다시 계산한 결과와 같은지 센다 (results_table이 있으면 기록).
    """
    import gold_build
    changes_df.dropDuplicates().createOrReplaceTempView(CHANGES)
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {AFFECTED_ORDERS} AS {affected_orders_sql(silver)}")
    _materialize(spark, AFFECTED_ORDERS)
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {AFFECTED_REFUNDS} AS {affected_refunds_sql(silver)}")
    _materialize(spark, AFFECTED_REFUNDS)
    gold_build.check_source_keys(spark, {table: new_rows_sql(table, silver) for table in ("fact_sales_item", "fact_refund")})
    for table in ("fact_sales_item", "fact_refund"):
        spark.sql(scoped_merge_sql(table, silver, gold, f"{gold}.{table}"))
    violations = {table: spark.sql(consistency_sql(table, silver, gold)).first()["violations"] for table in ("fact_sales_item", "fact_refund")}
    if results_table:
        from datetime import datetime, timezone
        from run_checks import make_result, save_results
        now = datetime.now(timezone.utc)
        run_id = f"gold-{now.strftime('%Y%m%dT%H%M%S')}"
        save_results(spark, [make_result(f"{RULE_ID}_{table}", "gold", "batch", "batch", "error", 0, count, run_id, now, None, batch_id,
                                         f"{table}: 반영한 범위의 Gold 행과 다시 계산한 행이 다른 건수") for table, count in violations.items()],
                     results_table)
    return violations


def make_batch_handler(silver: str, gold: str, results_table=None):
    """foreachBatch에 넘길 함수 (batch_df, batch_id)를 만든다."""
    def handle(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        apply_changes(batch_df.sparkSession, silver, gold, batch_df, results_table, batch_id)
    return handle


def run_gold_stream(spark, silver: str, gold: str, checkpoint: str, starting_versions: dict = None, trigger_seconds: int = 60,
                    available_now: bool = False, results_table=None) -> None:
    """Silver 변경 피드를 읽어 Gold에 증분 반영하는 스트림을 시작한다. available_now면 쌓인 분량만 처리하고 끝낸다 (테스트용)."""
    writer = (read_changes_stream(spark, silver, starting_versions).writeStream
              .foreachBatch(make_batch_handler(silver, gold, results_table))
              .option("checkpointLocation", checkpoint))
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=f"{trigger_seconds} seconds")
    writer.start().awaitTermination()

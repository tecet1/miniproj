# quality — 선언형 품질 규칙

품질 규칙을 코드가 아니라 데이터(`rules.json`)로 둔다. 규칙을 추가하려면 이 파일에 한 항목을 쓰면 된다. 검증은 `rules.py`의 `load_rules`가 먼저 하므로 잘못된 모양은 실행 전에 `RuleError`로 멈춘다.

계획 `plans/07` 카드 7-A의 산출물이다. 규칙을 실제 파이프라인에 연결하는 일은 카드 7-B(행 규칙)·7-C(집합 규칙)다. 지금은 형식, 규칙 목록, 검증, SQL 만들기까지 있고 Spark에서 돌려 본 것은 없다 (**Databricks 미검증**).

## 두 종류의 규칙

| 종류 | 무엇을 보나 | 위반하면 | 쓰이는 곳 |
|---|---|---|---|
| `row` (행 단위) | 공통 이벤트 한 건 | 그 이벤트가 Quarantine으로 간다 (사유 코드 기록) | Silver 변환 (카드 7-B) |
| `set` (집합 단위) | 테이블 전체 또는 이번에 들어온 부분 | 결과 테이블에 기록, `error`면 다음 단계로 가지 않는다 | 단계 사이의 품질 검사 (카드 7-C) |

기획안(PPT)의 네 규칙과의 관계는 이렇다. 중복 제거(`channel + event_id`, insert-only MERGE), 누락 검증, 데이터 표준화, 컬럼 매핑은 채널 원본을 공통 이벤트로 바꾸는 단계의 일이고 지금도 Silver가 하고 있다. 카드 7-B에서 이 단계를 Spark로 옮길 때 같은 선언형 방식(채널별 매핑표)으로 표현한다. 이 파일의 `row` 규칙은 변환이 끝난 공통 이벤트의 값이 정상 범위인지를 보는 규칙이다.

## 규칙 한 줄 추가하기

### 행 규칙: 이벤트 값 검사

`rules.json`의 `rules` 목록에 추가한다. 예: 주문 배송비가 100만 원을 넘으면 격리.

```json
{"id": "order_shipping_fee_not_huge", "kind": "row", "applies_to": ["ORDER_CREATED"],
 "expect": "shipping_fee <= 1000000", "reason_code": "INVALID_VALUE", "detail": "배송비가 100만 원 초과"}
```

- `expect`는 **이 조건이 참이어야 정상**이라는 식이다. 거짓이거나 NULL이면 위반이고 그 이벤트는 격리된다.
- `applies_to`: 규칙을 적용할 이벤트 유형 (`ORDER_CREATED`, `ORDER_CANCELLED`, `PAYMENT_SUCCEEDED`, `PAYMENT_FAILED`, `REFUND_SUCCEEDED`).
- 식에 쓸 수 있는 컬럼은 공통 이벤트 모델의 컬럼이다 (`silver_tables.EVENT_SCHEMA`: `shipping_fee`, `payment_amount`, `refund_quantity`, `items` 등).
- 주문 상품 행처럼 배열은 고차 함수로 쓴다: `forall(items, i -> i.quantity > 0)`.
- 기준 SKU 목록은 `{known_skus}` 자리표시자로 받는다. 서브쿼리는 쓸 수 없다.
- `reason_code`는 기존 사유 코드(`MALFORMED_JSON`, `UNKNOWN_EVENT_TYPE`, `MISSING_REQUIRED`, `INVALID_TYPE`, `INVALID_VALUE`, `UNKNOWN_CHANNEL`) 중 하나다. 새 사유 코드가 필요하면 이벤트 명세 4절 변경이 먼저다.

### 집합 규칙: 테이블 점검

위반 건수를 `violations` 컬럼 하나로 돌려주는 SELECT를 쓴다. 예: 이번에 들어온 결제 중 금액이 0인 것이 100건을 넘으면 경고.

```json
{"id": "silver_zero_payments", "kind": "set", "layer": "silver", "severity": "warn", "scope": "incremental",
 "max": 100, "description": "금액이 0인 결제",
 "sql": "SELECT COUNT(*) AS violations FROM {silver}.payments WHERE payment_amount = 0 AND received_at >= {since}"}
```

- `max`: 허용하는 위반 건수 (기본 0). 넘으면 `severity`(`error` 또는 `warn`)로 기록한다.
- `scope`: `incremental`은 이번에 들어온 부분만 본다. SQL에 `{since}`(직전 확인 시각)로 범위를 걸어야 하고, `full`은 전체를 보며 `{since}`를 쓰지 않는다. 운영 중에는 incremental을 기본으로 하고 full은 가끔 도는 점검에 둔다.
- 자리표시자 `{bronze}`, `{silver}`, `{gold}`는 스키마 이름으로 바뀐다.
- 읽기 전용이어야 한다. `;`, `INSERT`/`UPDATE`/`DELETE`/`DROP`/`ALTER`/`MERGE`/`CREATE`/`TRUNCATE`/`OPTIMIZE`/`VACUUM`이 있으면 거부한다.

### 규칙 끄기와 확인

- 지우지 않고 끄려면 항목에 `"enabled": false`를 넣는다.
- 모양 검사: `python -m unittest discover -s .\02_pipeline\tests\quality -v` (규칙 목록 자체도 이 테스트가 읽는다).

## 증분 실행과 결과 기록 (카드 7-C·7-D 일부)

- `run_checks.run_quality_checks(spark, schemas, mode)`: 집합 규칙을 실행하고 결과를 `commerce_poc.quality.results`에 쌓는다. 노트북은 `run_quality.py`.
  - `incremental`: 증분 규칙만, 직전 incremental 실행 시각 - 10분 이후의 행만 본다. 처음이면 전체를 본다.
  - `full`: 모든 집합 규칙을 시간 제한 없이 본다. 덤프처럼 `received_at`이 과거인 행은 증분에 걸리지 않으므로 여기서 본다.
  - 결과 한 줄 = 규칙 하나의 한 번 실행: 규칙 이름(`rule_id`)·단계(`stage`)·위반 건수(`violations`)·`status`(pass/warn/error/exception)·`since`·`detail`. SQL이 실패하면 `exception`으로 기록하고 다음 규칙을 계속한다.
- `batch_check.make_after_batch(schema)`: Silver 배치마다 "들어온 수 = Silver 반영 + 중복 제거 + 격리"를 확인해 `silver_batch_balance` 한 줄을 `batch_id`와 함께 기록한다. `silver_stream.run_silver(..., after_batch=...)`로 연결한다(`run_silver.py` 노트북이 연결한다).
- 로컬 Spark에서 SQL 의미와 집계를 확인했다. results 테이블 저장, Delta MERGE 뒤 조회, 노트북의 `sys.path` 가정은 **Databricks 미검증**이다.

## Bronze 재계산 대조 (전수 정답 비교, 2026-10-15)

- 정답 파일 없이, Bronze 원본(payload)에서 **독립 경로로 Silver·Gold를 다시 계산**해 실제 테이블과 행 단위로 비교한다. `run_reconcile.py` 노트북이 `run_quality_checks(mode="reconcile")`를 부른다. 긴 주기(하루 한 번 등) 배치용이다.
- `reconcile_sql.py`가 Bronze 행에서 채널 A·B·C의 필드를 SQL(`get_json_object`, `from_json`)로 직접 읽어 `recon_*` 임시 뷰를 만든다. Silver 변환 코드(`channels/*`, `validate_*`)와 `gold_sql.py`를 쓰지 않고 명세 2·4·6.1절만 보고 따로 구현했다. 같은 해석 오류를 두 경로가 함께 하지 않게 하려는 것이다.
- `reconcile_rules.json`(8개, `reconcile_sql.rule_list()`가 만든다)이 `silver.orders·order_items·payments·order_cancellations·refunds·quarantine`, `gold.fact_sales_item·fact_refund`를 재계산 결과와 비교한다. 위반 건수 = (재계산에만 있는 행) + (실제에만 있는 행). 0이어야 하고 결과는 `quality.results`(`mode = reconcile`)에 쌓인다.
- 로컬 Spark에서 고정 입력(S24~S28 포함)과 오류를 높은 비율로 주입한 합성 데이터, 손으로 만든 변형 51줄을 파이썬 파이프라인 결과와 대조해 모든 규칙이 0임을 확인했고, 실제 테이블을 일부러 틀리게 만든 변이도 잡힘을 확인했다 (`tests/quality/test_reconcile.py`).
- 한계: JSON 숫자가 문자열("5")로 오면 SQL은 정수로 받아들이고 Silver는 INVALID_TYPE으로 격리한다 (생성기는 "N/A"만 쓴다). 파이프라인이 Bronze를 따라잡기 전에 돌리면 반영 중인 행 때문에 차이가 난다. 중간 뷰를 `localCheckpoint`로 계산해 두므로 서버리스에서는 느릴 수 있다. **Databricks 미검증.**

## 계층 검사와 보고 (카드 7-C)

- 예전 `check_silver`·`check_gold`의 정합성 검사(PK 중복, 결제금액 = 상품합 + 배송비, 환불 합계, 배송비 규칙, Dimension 키 누락, 격리 행 원본 보존)는 모두 `rules.json`의 집합 규칙으로 옮겼다.
- 노트북은 `run_checks.check_layer(spark, SCHEMAS, "silver" | "gold")`를 부른다. 그 계층의 집합 규칙을 전수로 돌려 `quality.results`에 기록하고, 실패(error·exception) 규칙의 문장 목록을 돌려준다. 빈 목록이면 통과다. warn은 실패로 세지 않는다.
- 실패가 아닌 **보고**는 따로 남는다: `silver_check.report_by_design`(업무 ID 중복·참조 누락, 일부러 그대로 둔 위반), `gold_check.report_exclusions`(업무 키 위반 주문 / 무효 환불 / 위반 주문에 붙은 환불). 오류를 섞은 데이터에서는 0이 아닌 것이 정상이다.

## Gold 증분 계산 (카드 7-E)

- `gold/gold_incremental.py`: Silver 변경 피드(CDF)에서 바뀐 주문·환불만 찾아 Gold의 해당 키만 다시 계산해 MERGE한다 (`_delete` 표식 행으로 범위 안 옛 행을 지운다). 전체 재계산(`gold_build.build_gold`)과 같은 결과가 나와야 하며, 배치마다 `gold_batch_consistency*` 규칙으로 확인해 `quality.results`에 남긴다.
- 처음 한 번 `bootstrap`(변경 피드 켜기 + 전체 계산) 후 증분으로 이어 간다. 업무 키 위반 주문, 환불 겹침 규칙은 전체 계산과 같은 SQL 조각(`gold_sql.py`)을 쓴다.
- 로컬에서는 sqlite로 SQL 의미를 검증했다(무작위 도착·배치 분할 뒤 전체 재계산과 일치). CDF 읽기, 범위 MERGE, 스트림은 **Databricks 미검증**이다.

## 상시 스트리밍과 지연 관찰 (카드 7-F·7-G)

- 트리거 간격은 `02_pipeline/pipeline_config.json`(bronze 10초, silver 30초, gold 60초 `processingTime`)에만 둔다. `stream_config.trigger_for(layer)`가 읽고 형식을 검사한다. 노트북 `bronze/stream_bronze.py`, `silver/stream_silver.py`, `gold/stream_gold.py`는 이를 넘겨 호출만 한다.
- `jobs/pipeline_job.yml`(Asset Bundle 리소스 정의)이 스트림 3개를 한 Job의 태스크로 묶고(`max_retries: -1`로 실패 시 재시작, 체크포인트가 이어 읽는다), 별도 Job으로 `run_reconcile`을 매일 돌린다. `cluster_id`는 배포할 때 사용자가 채운다. **배포·실행 미검증**.
- 지연 관찰: `run_latency.py`에 방금 보낸 테스트 주문 번호를 넣으면 Bronze·Silver·Gold에 처음 보이는 시각을 잰다(`latency_probe.py`). 관찰값이고 목표는 없다.

## 파일

| 파일 | 역할 |
|---|---|
| `rules.json` | 규칙 목록. 사용자가 추가·수정하는 곳 |
| `run_checks.py` | 집합 규칙 실행(증분·전수)과 results 저장 |
| `batch_check.py` | Silver 배치 균형 검사와 기록 |
| `run_quality.py` | 노트북(호출만) |
| `reconcile_sql.py` | Bronze 재계산 SQL(recon_* 뷰)과 reconcile 규칙 만들기 |
| `reconcile_rules.json` | Bronze 재계산 대조 규칙 8개 (`reconcile_sql.rule_list()`로 생성) |
| `run_reconcile.py` | 재계산 대조 노트북(호출만) |
| `latency_probe.py`, `run_latency.py` | 지연 관찰(테스트 주문이 각 계층에 보이는 시각)과 노트북 |
| `rules.py` | `load_rules`(읽기·검사), `validate_rules`, `row_violation_condition`(행 규칙 → Spark SQL 위반 조건), `render_set_sql`(집합 규칙 SQL 채우기), `judge`(허용값 판정) |

## 지금 들어 있는 규칙

- 행 규칙 10개: 기존 `silver/validate_values.py`의 값 검사 8개 + 한 주문 `items` 안 `order_item_id` 중복(2026-10-15) + SKU 기준 데이터 검사(`validate_skus.py`).
- 집합 규칙 21개: Silver 8개(PK 중복 — 주문·결제·환불은 증분, 상품 행은 전수 —, 환불→상품 행 참조, 격리 행의 원본 보존, 결제금액 = 상품합 + 배송비 등), Gold 13개(PK 중복, Dimension 키 누락, 환불 합계 일치, 결제금액 대조, 배송비 규칙 등). 실제 목록은 `rules.json`이 기준이다.

## 한계

- 행 규칙은 로컬 Spark 4.2에서 기존 파이썬 검사(`validate_values`, `validate_skus`)와 같은 이벤트를 같게 가르는지 차분 비교했다(경계값 조합 약 150건, `tests/quality/test_rules_spark.py`). 집합 규칙 SQL은 같은 로컬 Spark에서 위반 있음/없음, 증분 범위를 확인했다. **Databricks(Delta, Unity Catalog)에서는 아직 미검증이다.** 로컬 테스트는 테이블 대신 임시 뷰를 쓴다 (로컬 Windows Spark는 파일 테이블 생성에 Hadoop 도구가 필요함).
- 기존 정답 파일 비교는 없앴다. 대신 Bronze 재계산 대조(위 절)가 전수 정답 역할을 한다. Silver 네이티브 변환(UDF 없음)은 로컬에서 파이썬 기준 변환과 행 단위로 같음을 확인했고(`tests/silver/test_native_convert.py`), Databricks 실행은 미검증이다.

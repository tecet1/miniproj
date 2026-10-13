# 계획 04. Silver → Gold (Fact·Dimension)

## 목표

Silver에서 결제 성공 주문상품 Fact와 환불 Fact, 이를 공유하는 Dimension 5개를 다시 계산해 MERGE로 적재하고, 지표 8개와 환불 지표를 낼 수 있는 모양으로 만든다.

## 입력 / 산출물

- 입력
  - `commerce_poc.silver.{orders, order_items, payments, order_cancellations, refunds}`
  - `01_contracts/contracts/event_spec.md` 6절(Gold·지표), `01_contracts/data_fixtures/A사_ERD.pdf` ②③ (구조 레퍼런스)
  - `01_contracts/contracts/skus.json` (`dim_product` 원천)
  - 기대값: `01_contracts/data_fixtures/expected/expected.json`
- 산출물
  - 테이블 `commerce_poc.gold.{fact_sales_item, fact_refund, dim_date, dim_channel, dim_product, dim_payment_method, dim_refund_profile}`
  - `02_pipeline/gold/` (한 파일은 한 가지 책임):
    - `gold_tables.py`: 테이블 정의(컬럼·타입·PK)와 생성
    - `gold_sql.py`: Fact를 Silver에서 계산하는 SQL 문장 (Spark SQL의 공통 부분집합, KST 일자 변환만 한 함수에 분리)
    - `dimensions.py`: 상수·`skus.json` Dimension 행과 `dim_date` 행 (순수 Python)
    - `gold_merge.py`: MERGE 문장 생성과 실행 (Fact는 `NOT MATCHED BY SOURCE`로 삭제까지, Dimension은 갱신·추가)
    - `gold_build.py`: 위를 순서대로 호출하는 `build_gold`
    - `gold_check.py`: `expected.json` 대조 (순수 비교 함수 + Spark 집계)
    - `run_gold.py`: 실행 노트북 (호출만)
  - 테스트 `02_pipeline/tests/gold/`

## 이미 정해진 것 (event_spec.md 6절, ERD ②③)

- Fact 2개. `fact_sales_item`은 결제 성공한 주문상품 1행(PK `channel, order_item_id`), `fact_refund`는 환불 이벤트 1행(PK `channel, refund_id`).
- 한 주문의 결제 성공은 최대 1건이다(분할 결제 제외, 2026-10-13). 결제 시도 전체는 `silver.payments`에 남고 Fact는 성공 결제를 가리킨다.
- 취소와 결제 성공이 함께 있는 주문은 `fact_sales_item`에서 제외하며 그 주문의 환불도 `fact_refund`에서 뺀다. 이상 주문 목록(뷰)·미결 목록은 PoC 이후다.
- 일자는 KST이며, 판매·환불 지표는 결제일(원결제일)로 귀속한다.
- 재계산 후 MERGE. 조인이 안 되는 행은 다음 실행에서 반영된다.
- 환불 사유가 없으면 `OTHER`. `holiday`는 코드에 둔 공휴일 목록이다(공식 달력과 미대조).

## 설계 결정 (2026-10-13)

1. **Fact 계산:** Silver 테이블을 조인하는 SQL 한 문장씩이다. `silver_stream.py`처럼 DataFrame API를 쓰면 로컬에서 실행해 볼 수 없다. SQL은 조인·집계·`COALESCE`·`NOT EXISTS`만 쓰는 공통 부분집합이라 로컬에서 `sqlite3`로 같은 문장을 고정 입력에 실행해 독립 기대값과 비교할 수 있다. 달라지는 것은 KST 일자 변환(`kst_date`) 하나이며 테스트에서만 sqlite용으로 바꾼다.
2. **MERGE:** Fact는 `WHEN MATCHED UPDATE SET *`, `WHEN NOT MATCHED INSERT *`, `WHEN NOT MATCHED BY SOURCE DELETE`다. 마지막 절이 있어야 나중에 취소가 도착해 제외 대상이 된 주문의 행이 사라져 재계산 결과와 같아진다 (Databricks Runtime 12.2 이상, Microsoft Learn MERGE INTO 문서). 소스에 같은 키가 둘이면 MERGE가 오류를 내므로 중복은 조용히 넘어가지 않는다.
3. **Dimension:** 키로 갱신·추가만 한다 (삭제하지 않아 Fact의 참조가 끊기지 않는다). `dim_date`는 Fact를 만든 뒤 두 Fact의 일자 최소~최대로 만든다.
4. **검증 층:** 로컬은 SQL 결과·Dimension 행·MERGE 문장 모양·호출 순서까지다. Spark에서 실제로 MERGE가 도는지는 Databricks에서 `gold_check`로 확인한다.

## Spark 처리 흐름

`build_gold(spark, silver_schema, gold_schema, skus_path)`가 순서대로 수행한다.

1. `create_tables`: Gold 테이블 7개를 만든다 (이미 있으면 그대로).
2. 상수 Dimension(`dim_channel`, `dim_payment_method`, `dim_refund_profile`)과 `dim_product`(`skus.json`)를 `createDataFrame` → 임시 뷰 → MERGE.
3. `sales_item_sql`·`refund_sql`의 결과를 임시 뷰로 만들고 키 기준 MERGE (갱신·추가·삭제).
4. 두 Fact의 일자 최소~최대로 `dim_date` 행을 만들어 MERGE.

## 단계

1. 계약 6절, 독립 기대값(Gold 분포) 추가, 문서·기록 갱신 (완료)
2. `02_pipeline/gold/` 구현 (Claude)
3. 로컬 테스트: SQL을 sqlite로 실행해 `expected.json`과 대조, Dimension·MERGE·호출 순서
4. Databricks 실행, `gold_check` 대조, 재실행

## 완료 조건

- 건수가 `expected.json` 합계와 일치한다: `fact_sales_item` 30, `fact_refund` 3, `dim_channel` 3, `dim_product` 15, `dim_payment_method` 5, `dim_refund_profile` 10, `dim_date` 1.
- 금액: 총판매금액 1,266,000, 환불금액 105,000, 환불수량 3, 순판매금액 1,161,000. 상품결제건수 30, 결제고객수 18, 환불금액비율·상품결제단가(42,200)·객단가(70,333.33)가 일치한다. 채널별 값도 일치한다(총판매금액 422,000, 환불 35,000 등).
- 분포: 결제일은 모두 2026-10-12(S23이 UTC로는 10-11이므로 KST 변환 확인), 결제수단은 CARD 18·EASY_PAY 6·BANK_TRANSFER 3·VIRTUAL_ACCOUNT 3(S06은 실패한 CARD가 아니라 성공한 EASY_PAY), 환불 프로필 `RETURN-DEFECT` 3, 환불일 10-12.
- 대조 규칙: `Σ fact_refund` = `Σ fact_sales_item`의 환불 합계(금액·수량), 성공 결제금액 = 상품 결제금액 합 + 배송비.
- PK 중복 0건, FK 누락 0건(상품·결제수단·환불 프로필·채널·일자).
- 같은 Silver로 재실행해도 건수와 금액이 그대로다.
- 이상 주문(취소+결제 성공) 제외 규칙은 현재 픽스처에 해당 시나리오가 없어 합성 입력의 단위 테스트로만 확인한다(`scenarios.md` 보류 항목).

## 검증 방법

- 로컬: `python -m unittest discover -s .\02_pipeline\tests\gold -v`. 픽스처 72행을 Silver 변환·중복 제거해 Silver 행으로 만들고(`§1.3` 매핑), 같은 SQL을 sqlite로 실행해 `expected.json`과 비교한다.
- Databricks: `run_gold.py` 실행 후 `gold_check.check_gold`가 빈 목록이면 일치. 실행 전까지 '미검증'.
- 기대값은 구현 통과를 위해 수정하지 않는다.

## 범위 밖

- Metric View·대시보드(다음 계획), 이상 주문 목록·미결 목록, 분할 결제, 상품 SCD Type 2, 참조에 없는 SKU 자리표시 행, 채널 간 고객 통합, 실행 모니터링

## 상태

구현·로컬 테스트 완료. Databricks는 테이블 건수만 확인했고 `check_gold` 결과는 **완료** (2026-10-13).

- 로컬 (Claude 환경): `02_pipeline/tests` 전체 337건 통과, 그중 Gold 73건. 확인한 범위: Fact SQL을 sqlite로 픽스처 72행(Silver 행으로 변환)에 실행해 `expected.json`과 대조(건수·금액·지표 9개·결제일/결제수단/환불 프로필/환불일 분포·참조·대조 규칙), 합성 입력으로 이상 주문 제외·지연 도착·부분 환불 합산·사유 없음·익명 고객·채널 분리, Dimension 행, MERGE 문장, `build_gold` 호출 순서(가짜 Spark 객체), 비교 함수.
- 테스트가 규칙 위반을 실제로 잡는지 변형 7개로 확인했다(UTC 일자, 취소 제외 삭제, 결제 상태 조건 삭제, 사유 기본값 삭제, 환불 합계를 최대값으로 변경, 환불 조인 키를 주문 ID로 변경, 환불의 주문 조인 삭제). 처음에 2개(환불 조인 키, 환불의 주문 조인)는 테스트를 통과해 테스트를 추가했고 모두 잡힌다.
- Databricks (사용자 보고): 건수 조회 결과가 `expected.json`과 일치한다: `fact_sales_item` 30, `fact_refund` 3, `dim_date` 1, `dim_channel` 3, `dim_product` 15, `dim_payment_method` 5, `dim_refund_profile` 10. 이로써 SQL·MERGE·KST 변환이 오류 없이 돌았음은 확인되었고 금액·지표·분포·참조는 `check_gold`로 확인했다.
- 가정·한계: (1) 취소+결제 성공 주문의 환불도 `fact_refund`에서 빼는 것은 ERD에 없는 추가 해석이다(6.1절에 기재, 확인 필요). (2) `holiday`는 공식 달력과 대조하지 않았다. (3) 결제 성공이 2건 이상인 주문은 계약 위반으로 별도 규칙이 없으며, 들어오면 Fact 키가 중복되어 MERGE가 오류를 낸다. (4) 취소+결제 성공 제외와 부분 환불 합산은 현재 픽스처에 시나리오가 없어 합성 입력으로만 확인했다.
- 저장소에 키·연결 문자열이 없다: 새로 만든 `.py`·`.md`·`.json` 텍스트를 키워드로 검색해 확인했다. Git 이력과 바이너리는 검사하지 않았다.

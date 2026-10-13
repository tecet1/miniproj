# 계획 05. Metric View와 대시보드

## 목표

검증된 Gold 위에 지표 정의(Metric View) 2개를 만들고, 그 지표로 판매·환불 대시보드(2페이지)를 구성한다.

## 입력 / 산출물

- 입력
  - `commerce_poc.gold.{fact_sales_item, fact_refund, dim_date, dim_channel, dim_product, dim_payment_method, dim_refund_profile}` (계획 04)
  - `01_contracts/contracts/event_spec.md` 6.5절(지표 정의), `01_contracts/data_fixtures/expected/expected.json`(독립 기대값)
- 산출물
  - Metric View `commerce_poc.serving.sales_metrics`(원천 `fact_sales_item`), `commerce_poc.serving.refund_metrics`(원천 `fact_refund`)
  - `03_serving/metric_views/` (한 파일은 한 가지 책임):
    - `metric_defs.py`: 두 정의(조인·분석 기준·지표 식)와 YAML·`CREATE OR REPLACE VIEW ... WITH METRICS` 문장 생성
    - `create_metric_views.py`: 스키마와 뷰 생성
    - `metric_check.py`: `MEASURE()` 조회 결과를 `expected.json`과 대조 (`gold_check.compare_metrics` 재사용)
    - `run_metric_views.py`: 실행 노트북 (호출만)
  - `03_serving/dashboards/`: `dashboard_spec.py`(데이터셋·위젯 정의), `build_dashboard.py`(Lakeview JSON 생성), `commerce_poc.lvdash.json`(생성 결과)
  - 테스트 `02_pipeline/tests/serving/`

## 설계

- **지표:** 정의는 event_spec 6.5절 그대로다. 판매 지표(총판매금액, 환불금액, 순판매금액, 환불금액비율, 상품결제건수, 상품결제단가, 결제고객수, 객단가)는 `sales_metrics`, 환불수량은 `refund_metrics`다. 환불 분석의 환불금액은 `fact_refund` 합이다.
- **분석 기준:** `sales_metrics`는 결제일·주·월·요일·공휴일·채널·상품·카테고리·결제수단, `refund_metrics`는 결제일·환불일·월·채널·상품·카테고리·환불 유형·사유다. 모두 Dimension 조인으로 얻는다.
- **비가산 지표:** 비율·단가·결제고객수·객단가는 식 안에 분자와 분모를 함께 넣어 조회 범위마다 다시 계산한다. 결제고객수는 `channel || '|' || user_id`의 distinct이며 `user_id`가 null이면 세지 않는다.
- **사유별 비중:** 구성비는 합이 1이 되는 값이라 지표가 아니라 대시보드 데이터셋에서 `환불금액 / SUM(환불금액) OVER ()`로 계산한다.
- **대시보드:** 판매 페이지(KPI 8개, 결제일별 판매·환불, 채널별 표, 결제수단별·카테고리별 총판매금액)와 환불 페이지(환불금액·환불수량, 사유별 표·막대, 결제일별·채널별 환불금액). 막대에는 비가산 지표를 쓰지 않는다. 필터는 두지 않았다(하루치 데이터).
- **로컬 검증 방식:** 같은 정의의 조인·식을 일반 SQL로 풀어 sqlite에서 실행한다. `MEASURE()`와 YAML 해석은 Databricks만 한다.

## 완료 조건

- 두 Metric View가 만들어지고 `check_metric_views`가 빈 목록이다: 총판매금액 1,266,000, 환불금액 105,000, 환불수량 3, 순판매금액 1,161,000, 상품결제건수 30, 결제고객수 18, 환불금액비율·상품결제단가(42,200)·객단가(70,333.33), 채널별 값이 `expected.json`과 같다.
- `sales_metrics`의 환불금액과 `refund_metrics`의 환불금액이 같다.
- 대시보드 데이터셋이 실행되고 위 KPI 숫자가 화면에 같은 값으로 나온다. 사유별 비중의 합이 100%다.

## 검증 방법

- 로컬: `python -m unittest discover -s .\02_pipeline\tests\serving -v`. Gold를 sqlite에 만들고(계획 04 SQL 재사용) 정의의 식으로 조회해 `expected.json`과 비교한다.
- Databricks: `run_metric_views.py` 실행 후 `check_metric_views` 결과, 그리고 `commerce_poc.lvdash.json`을 Git folder로 동기화해 대시보드를 연다. 확인 전까지 '미검증'.
- 기대값은 구현 통과를 위해 수정하지 않는다.

## 범위 밖

- Genie, 필터·스케줄·알림, 이상 주문 목록, 방문수·전환율·결제 성공률·취소율, 벤치마크 화면의 복제, 실행 모니터링

## 상태

구현·로컬 테스트 완료. Databricks는 **미검증** (2026-10-13).

- 로컬 (Claude 환경): `02_pipeline/tests` 전체 373건 통과, 그중 Serving 36건. 확인한 범위: 두 정의를 일반 SQL로 실행해 `expected.json`과 대조(합계·채널별·파생 지표·결제수단 분포), 어떤 기준으로 나눠도 가산 지표 합이 같음, 비가산 지표(결제고객수)가 합이 되지 않고 다시 계산됨, 익명 고객 제외, 채널이 다르면 같은 `user_id`도 다른 고객, 0건일 때 비율이 NULL, 나누어떨어지지 않는 단가, YAML 왕복(PyYAML이 있을 때), 데이터셋 열·위젯 필드·격자 겹침, 저장소의 JSON 파일이 정의와 같음, `metric_check`(가짜 spark로 정상·값 불일치·채널 누락·환불수량·두 뷰 불일치).
- 변이 12개(결제고객수 키에서 채널 제외, 순판매금액 부호, 환불금액비율 분모, 건수 정의, 조인 키, 단가 정수 나눗셈, 환불수량 정의, 막대에 비가산 지표, 구성비 분모, 위젯 겹침, 환불수량 읽기, 두 뷰 대조 삭제)을 넣어 모두 테스트가 잡음을 확인했다. 단가의 정수 나눗셈 변이는 처음에 통과해(고정 입력이 나누어떨어짐) 테스트를 추가했다.
- 못 한 범위(미검증): Databricks가 Metric View YAML을 받아들이는지(`version: 0.1`, `joins`, `source.` 접두어, 측정값 이름이 컬럼 이름과 같은 경우), `MEASURE()` 조회와 `GROUP BY ALL`, `check_metric_views`의 `OK`, 대시보드 JSON을 Databricks가 열고 위젯(counter·table·bar의 spec 버전과 encodings)을 그리는지, 화면 숫자. 오류가 나면 해당 정의·JSON만 고치고 테스트는 약화하지 않는다.
- 가정·한계: (1) Metric View 문법은 Databricks 문서 기억에 따른 것이며 실제 실행 전이다. 런타임이 `version: 1.1`만 받으면 `metric_defs.py`의 한 줄을 바꾼다. (2) 대시보드 JSON도 직접 작성한 것이라 위젯 일부가 열리지 않을 수 있다. 그 경우 데이터셋 쿼리(`dashboard_spec.dataset_query`)는 그대로 쓸 수 있으니 화면에서 위젯만 다시 만든다. (3) 하루치 데이터라 날짜 추세는 한 점이다. (4) 환불 유형·사유 비중은 현재 픽스처에 `RETURN-DEFECT`만 있어 100%다.

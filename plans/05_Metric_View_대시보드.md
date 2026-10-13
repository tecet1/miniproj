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
- **대시보드:** 판매 페이지(KPI 8개, 결제일별 판매·환불, 채널별 표, 결제수단별·카테고리별 총판매금액)와 환불 페이지(환불금액·환불수량, 사유별 표·막대, 결제일별·채널별 환불금액). 막대에는 비가산 지표를 쓰지 않는다.
- **필터(2026-10-13 추가):** 기간(결제일, 날짜 범위)과 채널(다중 선택)을 전역 필터 페이지에 두어 모든 페이지 맨 위에 보이게 한다. 필터는 데이터셋 쿼리의 매개변수(`:date_range.min/.max`, `:channels`)로 Metric View 조회의 `WHERE`에 들어간다. 집계된 결과 행을 화면에서 다시 거르면 결제고객수·객단가·비율·구성비가 틀리기 때문이다. 환불 지표의 기간도 원결제일 기준이다(event_spec 6.3). 기본값은 기간 1년 전~오늘, 채널 전체이며 채널 선택지는 `dim_channel`을 읽는 데이터셋(`channel_options`)이다.
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

- Genie, 스케줄·알림, 결제수단·상품 등 다른 필터, 이상 주문 목록, 방문수·전환율·결제 성공률·취소율, 벤치마크 화면의 복제, 실행 모니터링

## 상태

**완료** (사용자 보고 기준, 2026-10-13). 로컬 테스트 통과. Databricks에서 `check_metric_views` OK, 대시보드 정상 출력(사용자 보고). 출력 원문과 화면은 보지 못했다.

- 로컬 (Claude 환경): `02_pipeline/tests` 전체 385건 통과, 그중 Serving 48건 (필터 추가 후). 확인한 범위: 두 정의를 일반 SQL로 실행해 `expected.json`과 대조(합계·채널별·파생 지표·결제수단 분포), 어떤 기준으로 나눠도 가산 지표 합이 같음, 비가산 지표(결제고객수)가 합이 되지 않고 다시 계산됨, 익명 고객 제외, 채널이 다르면 같은 `user_id`도 다른 고객, 0건일 때 비율이 NULL, 나누어떨어지지 않는 단가, YAML 왕복(PyYAML이 있을 때), 데이터셋 열·위젯 필드·격자 겹침, 저장소의 JSON 파일이 정의와 같음, `metric_check`(가짜 spark로 정상·값 불일치·채널 누락·환불수량·두 뷰 불일치).
- 변이 12개(결제고객수 키에서 채널 제외, 순판매금액 부호, 환불금액비율 분모, 건수 정의, 조인 키, 단가 정수 나눗셈, 환불수량 정의, 막대에 비가산 지표, 구성비 분모, 위젯 겹침, 환불수량 읽기, 두 뷰 대조 삭제)을 넣어 모두 테스트가 잡음을 확인했다. 단가의 정수 나눗셈 변이는 처음에 통과해(고정 입력이 나누어떨어짐) 테스트를 추가했다.
- Databricks (사용자 보고): 뷰 생성, `check_metric_views` OK, 채널별 `MEASURE()` 조회, 대시보드 출력이 정상이다. Databricks에서 내려받은 JSON을 저장소 파일과 비교하니 `datasets`·`pages`가 같고 Databricks가 `uiSettings`(테마)만 더했다. 그래서 문법 가정(`version: 0.1`, `joins`, `source.` 접두어, `GROUP BY ALL`)과 위젯 spec 버전은 이 환경에서 통한다.
- 필터 추가분은 **Databricks 미검증**이다: 대시보드 JSON의 전역 필터 페이지(`filter-date-range-picker`, `filter-multi-select`, 매개변수 묶음 형식)와 데이터셋 `parameters` 정의는 기억에 의존한 형식이라 열리지 않거나 선택이 쿼리에 반영되지 않을 수 있다. 로컬에서는 같은 WHERE를 sqlite로 실행해 채널 하나·둘 선택과 날짜 범위 경계의 값, 선택 범위에서의 재계산(객단가·환불금액비율·사유별 비중)을 기대값과 대조했고, 변이 9개(채널 조건 삭제, 날짜 상한 오류, WHERE 삭제, 매개변수 이름 불일치, 데이터셋 하나 누락 등)를 모두 잡았다.
- 못 한 범위: 화면의 숫자를 직접 보지 못했다. 하루치 데이터라 날짜 추세·구성비 변화는 확인할 수 없다.
- 가정·한계: (1) 저장소의 JSON에는 `uiSettings`가 없고, 이를 맞추려 파일을 바꾸지 않았다(테스트가 생성 결과와 같은지 본다). (2) 하루치 데이터라 날짜 추세는 한 점이다. (3) 환불 유형·사유 비중은 현재 픽스처에 `RETURN-DEFECT`만 있어 100%다.

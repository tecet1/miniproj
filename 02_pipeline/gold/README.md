# Gold 상품 이력 로직

- product_scd.py는 전체 상품 변경 이력에서 SCD Type 2를 만들고 시점별 상품 버전을 조회한다.
- 실제 Gold fact·지표·원격 Delta 저장은 아직 검증하지 않았다.

## 주요 함수와 파라미터

- 상품명을 포함한 세 속성의 변화만 버전을 추가한다.
- 거래 시점 조회와 현재 상품 조회를 구분한다.

| 함수 | 입력 / 결과 |
|---|---|
| build_product_history(changes) | 모든 상품 변경 목록 → 유효기간과 대체키를 갖는 차원 행 |
| lookup_product(history, sku_id, event_time) | 거래 시각에 유효한 상품 버전 한 행 |
| write_delta_dimension(spark, history, table_name) | Databricks에서 전체 이력 결과를 단일 Delta 테이블에 저장 |

build_product_history는 시각을 UTC로 표준화하고 SKU·시각으로 정렬한다. 같은 시각의 값 충돌을 먼저 거른 뒤, 속성이 바뀐 경우 이전 행을 닫고 새 행을 추가한다. 작은 제품 마스터는 전체 이력 재계산으로 시작하면 증분 MERGE의 상태·재시도 처리를 줄일 수 있다. 매번 전체 이력을 제공해야 하는 제약이 있다.

`(sku_id, *ATTRIBUTES, "effective_from")`의 *는 튜플 원소를 펼치는 문법이다. 속성 목록을 필수 필드 검사에 재사용한다. `{**row, ...}`는 사전을 복사하면서 관리 컬럼을 추가한다. 원본 입력을 직접 변경하지 않는다.

상품 버전 key는 SKU·시작 시각의 JSON 배열을 SHA-256으로 만든다. 구분자로 문자열을 이어 붙일 때의 모호함을 줄이고, 재실행이나 새 버전 추가로 기존 키가 달라지지 않게 한다. 이것을 보안 인증이나 업무 데이터 비교용 검증값으로 사용하지 않는다.

lookup_product는 [시작, 종료) 구간에서 정확히 한 행만 허용한다. 잘못된 데이터에서 임의의 첫 행을 골라 분석 결과를 만들지 않는다. 이 함수는 소규모 로컬 검증용 조회이며, Gold fact 전체는 Spark의 유효기간 조인으로 연결해야 한다.

write_delta_dimension은 표준 PySpark DataFrame 작성 코드다. 비어 있는 이력은 overwrite를 차단하고, Spark 타입을 명시해 null 종료 시각의 추론 오류를 피한다. PySpark import는 이 함수 안에 두어 로컬 이력 테스트에 Spark 설치가 필요하지 않다. 원격에서 이 함수 실행 및 테이블 권한을 확인해야 한다.

참고: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.saveAsTable.html

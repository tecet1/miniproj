# 상품 차원 SCD Type 2 계약

- product_name·category·brand의 변경 이력을 보존한다.
- 첫 구현은 작은 전체 변경 이력의 결정적인 재계산이다. 변경분만 받아 증분 MERGE하는 방식이 아니다.

## 입력과 출력

- 입력 product_changes.json은 SKU별 모든 과거 변경을 포함한다.
- 새 변경만 넣고 기존 파일을 대체하면 과거 이력을 잃으므로 금지한다.

입력 필드: sku_id, product_name, category, brand, effective_from(시간대 포함).

출력 필드: product_key, sku_id, product_name, category, brand, valid_from, valid_to, is_current.

product_key는 SKU와 유효 시작 시각의 JSON 배열을 SHA-256으로 계산한 결정적인 대체키다. 변하지 않은 버전의 키는 재실행·입력 순서 변경에도 유지된다. 자연키 SKU와 상품 버전 키를 구분한다. hash는 금액·행 검증을 대체하지 않는다.

## 변경과 조회 규칙

- 유효기간은 [valid_from, valid_to), 현재 버전의 종료 시각은 null이다.
- 같은 SKU·effective_from에 다른 속성값이 들어오면 충돌로 실패한다.

속성값이 같으면 새 버전을 만들지 않는다. exact 재수신도 버전 수를 늘리지 않는다. 거래 발생 시각으로 정확히 한 버전이 매칭되어야 하고, 0개 또는 여러 개면 조회에 실패한다.

현재 SKU-001은 10월 1일 '의류', 10월 11일 '아웃도어'로 변경한다. SKU-002는 '의류'를 유지한다. 출력 3행이며 현재 행은 SKU별 1개다. 10월 10일 거래는 의류, 10월 11일 변경 시각부터는 아웃도어다.

## 저장 방식과 한계

- write_delta_dimension은 Databricks에서 한 dim_product Delta 테이블을 전체 이력으로 overwrite한다.
- 로컬에서 변환·이력·시점 조회를 테스트한다. 원격 Databricks 실행은 별도 확인해야 한다.

단일 writer를 가정한다. 이 함수가 여러 fact·dimension을 원자적으로 전환하거나 동시 writer를 조정하는 것은 아니다. 현재 변화는 순서와 무관하게 재계산 가능하지만, 과거 속성의 정정으로 기존 버전이 재구성될 때에는 관련 fact의 상품 키도 재계산해야 한다.

동일 source SKU가 다른 공통 SKU로 바뀌는 매핑 이력은 이 상품 속성 SCD 계약에 포함하지 않는다. 그 요구가 생기면 매핑의 유효기간과 기존 fact 처리 정책을 별도로 정의한다.

참고: https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/

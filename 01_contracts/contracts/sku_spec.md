# SKU 명세

상품 참조 데이터의 기준 문서다. `gold.dim_product`와 이벤트 생성기가 이 명세를 따른다.

## 원칙

- 상품 식별자는 판매자 SKU(`product_id`)이며 모든 채널(A·B·C)이 같은 값을 쓴다.
- 채널 원본의 SKU 필드명은 다르다. A는 `sku`, B는 `sellerSku`, C는 `goods_cd`이며 Silver에서 `product_id`로 변환한다.
- 카테고리는 5종이고 카테고리마다 3개 상품을 둔다. 총 15개이며 목록은 `skus.json`에 있다.
- 단가(`unit_price`)는 원화 정수이며 `skus.json`의 값을 사용한다. 이벤트의 단가는 생성기가 이 표에서 가져온다.
- `dim_product`는 이 참조 데이터를 그대로 적재한다(현재 값만 유지). 주문 이벤트의 상품명·카테고리와 값이 다르면 이 참조 데이터가 우선한다.
- 상품 변경 이력(SCD)은 PoC 범위 밖이다.

## 필드

| 필드 | 타입 | 설명 |
|---|---|---|
| product_id | STRING | 판매자 SKU. `SKU-` + 3자리 숫자 |
| product_name | STRING | 상품명 |
| category | STRING | 카테고리 (아래 5종) |
| unit_price | INT | 단가 (원) |

## 상품 목록

`skus.json`이 기준이다(15개, 카테고리 5종 × 3개). 이 문서에는 목록을 복제하지 않는다.

## 사용 범위

- 소량 시나리오 세트는 SKU-001, 004, 007, 010, 013, 014만 사용한다(`scenarios.md` 참고).
- 나머지 SKU는 대량 세트와 카테고리·상품별 대시보드 분석에서 사용한다.

## 금액 계산

- `item_gross_amount = unit_price × quantity` (할인 전)
- `item_paid_amount = item_gross_amount − item_discount` (할인 후)
- 배송비는 한 주문의 `item_paid_amount` 합계가 20,000원 이상이면 0원, 미만이면 4,000원이다.

## 참조 데이터에 없는 SKU

참조에 없는 SKU가 들어오면 "알 수 없음" 속성의 자리표시 행을 `dim_product`에 만든다. 이 처리는 후속 범위이며 현재 시나리오 세트에서는 검증하지 않는다.

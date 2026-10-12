# 이벤트 명세 (01_contracts/contracts)

> 상태: 초안. 2026-10-12 환불 수량 필드(`refund_quantity`)와 구현된 Bronze 컬럼·채널 전달 방식(요청 경로)을 반영했다. 5절은 미확정 항목이다.

## 0. 적용 원칙

| 항목 | 결정 |
|---|---|
| 전송 경로 | 모든 채널 동일: HTTP POST → Azure Functions → Event Hubs (`commerce-events`) |
| Bronze | 원본 JSON(`payload`)을 수정 없이 보존 + 수신 메타데이터. 채널은 요청 URL 경로(`/api/events/{channel}`)로 전달받아 Function이 봉투에 담고 `channel` 컬럼으로 둔다 |
| Silver | 채널별 원본을 **공통 이벤트 모델**로 변환. 검증 실패는 Quarantine |
| 이벤트 종류 | 주문 생성 · 주문 취소 · 결제 성공 · 결제 실패 · 환불 성공 (5종) |
| 금액 | 원화 정수(KRW). 상품금액·할인·배송비·결제금액·환불금액을 별도 필드로 분리 |
| 시간 | 공통 모델은 UTC `TIMESTAMP`. 채널 원본 형식은 채널별로 다름(아래 참고) |
| 고객 | `channel + user_id`로 식별(채널 간 통합 없음). 비로그인은 `user_id = null` |
| 스키마 버전 | PoC 범위 제외 |

개념 관계: 주문 1 → 주문상품 N → 결제 N(주문 ID 참조) → 환불(상품 단위).
주문과 결제는 별도 이벤트이므로 **결제가 주문보다 먼저 도착할 수 있습니다.** 이 경우 격리하지 않고 Gold에서 조인합니다.

---

## 1. 공통 이벤트 모델 (Silver)

### 1.1 공통 헤더 (모든 이벤트)

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| event_id | STRING | Y | 이벤트 고유 ID. 중복 제거 키 (`channel + event_id` 유일) |
| event_type | STRING | Y | `ORDER_CREATED` `ORDER_CANCELLED` `PAYMENT_SUCCEEDED` `PAYMENT_FAILED` `REFUND_SUCCEEDED` |
| event_time | TIMESTAMP(UTC) | Y | 업무 발생 시각. 결제일 귀속 기준 |
| channel | STRING | Y | `A` `B` `C` |
| order_id | STRING | Y | 주문 ID (채널 내 유일) |
| user_id | STRING | N | 채널 내 회원 ID. 비로그인이면 null |

Bronze 컬럼: `channel`(요청 경로의 채널), `received_at`(Function이 부여한 수신 시각, UTC), `payload`(원본 JSON 문자열), `eh_partition`, `eh_offset`(Event Hubs 위치, 키는 `(eh_partition, eh_offset)`), `eh_enqueued_at`(Event Hubs 수신 시각), `ingested_at`(Bronze 적재 시각).
Function은 payload를 해석하지 않고 `{"channel","received_at","payload"}` 봉투로 감싸 Event Hubs에 보낸다. `channel`은 A·B·C 외에는 400으로 거절한다.

### 1.2 이벤트별 본문

**ORDER_CREATED** — 주문 생성

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| shipping_fee | INT | Y | 주문 단위 배송비 (0 이상) |
| items[] | ARRAY | Y | 1건 이상 |
| items[].order_item_id | STRING | Y | 주문 내 유일한 상품 행 ID |
| items[].product_id | STRING | Y | 판매자 SKU (공통 상품코드) |
| items[].product_name | STRING | N | 표시용 |
| items[].category | STRING | N | 상품 카테고리 |
| items[].quantity | INT | Y | 1 이상 |
| items[].unit_price | INT | Y | 단가 |
| items[].item_discount | INT | Y | 상품 단위 할인 합계 (0 이상, 단가×수량 이하) |

상품금액 = `unit_price × quantity − item_discount` (Silver에서 계산).

**ORDER_CANCELLED** — 주문 취소 (결제 전)

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| cancel_reason_code | STRING | N | 아래 공통 사유 코드 |

**PAYMENT_SUCCEEDED / PAYMENT_FAILED** — 결제

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| payment_id | STRING | Y | 결제 시도 ID (채널 내 유일) |
| payment_method | STRING | Y | `CARD` `EASY_PAY` `BANK_TRANSFER` `VIRTUAL_ACCOUNT` |
| payment_amount | INT | Y | 결제 요청/승인 금액 (배송비 포함, 할인 반영 후) |
| fail_reason_code | STRING | 실패 시 Y | `LIMIT_EXCEEDED` `CARD_DECLINED` `TIMEOUT` `OTHER` |

**REFUND_SUCCEEDED** — 환불 (결제 후, 상품 단위)

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| refund_id | STRING | Y | 환불 ID |
| payment_id | STRING | Y | 원결제 참조 (귀속일 판별) |
| order_item_id | STRING | Y | 환불 대상 상품 행 |
| refund_quantity | INT | Y | 환불 수량 (1 이상, 해당 상품 행 수량 이하). 수량 일부 환불 가능 |
| refund_amount | INT | Y | 환불 금액 (환불 수량분) |
| refund_type | STRING | Y | `CANCEL`(발송 전) `RETURN`(반품) |
| refund_reason_code | STRING | N | 공통 사유 코드 |

공통 사유 코드: `CHANGE_OF_MIND` `DEFECT` `WRONG_ITEM` `DELAY` `OTHER`

---

## 2. 채널별 원본 형식

채널마다 필드명·시간 형식·이벤트 코드를 다르게 둡니다. 변환 규칙이 Silver의 핵심 산출물이 됩니다.

| 구분 | A 자사몰 | B 종합 오픈마켓 | C 스포츠 전문몰 |
|---|---|---|---|
| 필드 표기 | snake_case, 평면+items 배열 | camelCase, 중첩 객체 | 약어 snake_case |
| 시간 형식 | ISO 8601 UTC (`2026-10-12T01:23:45.123Z`) | epoch 밀리초 (UTC) | `yyyy-MM-dd HH:mm:ss` **KST, 시간대 표기 없음** |
| 이벤트 구분 필드 | `event_type` | `eventName` | `evt_cd` |
| 비로그인 | `user_id: null` | `buyer.memberId` 없음 | `mem_no: ""` (빈 문자열) |
| 금액 | 정수 | 정수 | 정수 |

### 2.1 이벤트 종류 매핑

| 공통 | A | B | C |
|---|---|---|---|
| ORDER_CREATED | `order.created` | `ORDER_PLACED` | `O01` |
| ORDER_CANCELLED | `order.cancelled` | `ORDER_CANCELED` | `O09` |
| PAYMENT_SUCCEEDED | `payment.succeeded` | `PAY_APPROVED` | `P01` |
| PAYMENT_FAILED | `payment.failed` | `PAY_DECLINED` | `P02` |
| REFUND_SUCCEEDED | `refund.succeeded` | `REFUND_DONE` | `R01` |

### 2.2 필드 매핑

**공통 헤더**

| 공통 | A | B | C |
|---|---|---|---|
| event_id | `event_id` | `eventId` | `evt_id` |
| event_time | `occurred_at` | `occurredAtMs` | `evt_dt` (KST→UTC 변환) |
| order_id | `order_id` | `orderNo` | `ord_no` |
| user_id | `user_id` | `buyer.memberId` | `mem_no` (빈 값→null) |

**ORDER_CREATED**

| 공통 | A | B | C |
|---|---|---|---|
| shipping_fee | `shipping_fee` | `delivery.fee` | `dlv_amt` |
| items[] | `items[]` | `orderItems[]` | `lines[]` |
| order_item_id | `order_item_id` | `lineId` | `ord_no` + `-` + `seq` |
| product_id | `sku` | `sellerSku` | `goods_cd` |
| product_name | `product_name` | `itemName` | `goods_nm` |
| category | `category` | `categoryName` | `cate_nm` |
| quantity | `quantity` | `qty` | `qty` |
| unit_price | `unit_price` | `unitPrice` | `price` |
| item_discount | `discount_amount` | `discount.total` | `dc_amt` |

**ORDER_CANCELLED**

| 공통 | A | B | C |
|---|---|---|---|
| cancel_reason_code | `reason` | `cancelReason` | `rsn_cd` |

**PAYMENT_***

| 공통 | A | B | C |
|---|---|---|---|
| payment_id | `payment_id` | `paymentId` | `pay_no` |
| payment_method | `method` | `payMethod` | `pay_type` |
| payment_amount | `amount` | `paidAmount` | `pay_amt` |
| fail_reason_code | `failure_reason` | `declineCode` | `fail_cd` |

**REFUND_SUCCEEDED**

| 공통 | A | B | C |
|---|---|---|---|
| refund_id | `refund_id` | `refundId` | `rfd_no` |
| payment_id | `payment_id` | `paymentId` | `pay_no` |
| order_item_id | `order_item_id` | `lineId` | `ord_no` + `-` + `seq` |
| refund_quantity | `refund_quantity` | `refundQty` | `rfd_qty` |
| refund_amount | `amount` | `refundAmount` | `rfd_amt` |
| refund_type | `type` (`cancel`/`return`) | `claimType` (`CANCEL`/`RETURN`) | `clm_tp` (`1`/`2`) |
| refund_reason_code | `reason` | `claimReason` | `rsn_cd` |

### 2.3 코드 값 매핑

| 항목 | 공통 | A | B | C |
|---|---|---|---|---|
| 결제수단 | CARD | `card` | `CARD` | `10` |
| | EASY_PAY | `easy_pay` | `SIMPLE` | `20` |
| | BANK_TRANSFER | `bank` | `TRANSFER` | `30` |
| | VIRTUAL_ACCOUNT | `vbank` | `VBANK` | `40` |
| 사유 | CHANGE_OF_MIND | `change_of_mind` | `BUYER_REMORSE` | `01` |
| | DEFECT | `defect` | `DEFECTIVE` | `02` |
| | WRONG_ITEM | `wrong_item` | `WRONG_DELIVERY` | `03` |
| | DELAY | `delay` | `DELAYED` | `04` |
| | OTHER | 그 외 | 그 외 | 그 외 |
| 결제 실패 | LIMIT_EXCEEDED / CARD_DECLINED / TIMEOUT / OTHER | `limit_exceeded` `declined` `timeout` | `LIMIT` `DECLINED` `TIMEOUT` | `F1` `F2` `F3` |

(사유·실패 코드는 매핑에 없는 값이면 `OTHER`로 처리하고 오류로 보지 않음)

---

## 3. 원본 예시 (주문 생성)

**A**
```json
{"event_id":"A-evt-000001","event_type":"order.created","occurred_at":"2026-10-12T01:23:45.123Z",
 "order_id":"A-20261012-0001","user_id":"u1001","shipping_fee":3000,
 "items":[{"order_item_id":"A-20261012-0001-1","sku":"SKU-RUN-001","product_name":"러닝화 X","category":"신발",
           "quantity":2,"unit_price":50000,"discount_amount":10000}]}
```

**B**
```json
{"eventId":"B-evt-000001","eventName":"ORDER_PLACED","occurredAtMs":1791768225123,
 "orderNo":"B-20261012-0001","buyer":{"memberId":"m77"},"delivery":{"fee":0},
 "orderItems":[{"lineId":"L1","sellerSku":"SKU-RUN-001","itemName":"러닝화 X","categoryName":"신발",
                "qty":2,"unitPrice":50000,"discount":{"total":0}}]}
```

**C**
```json
{"evt_id":"C-evt-000001","evt_cd":"O01","evt_dt":"2026-10-12 10:23:45",
 "ord_no":"C-20261012-0001","mem_no":"","dlv_amt":2500,
 "lines":[{"seq":1,"goods_cd":"SKU-RUN-001","goods_nm":"러닝화 X","cate_nm":"신발",
           "qty":2,"price":50000,"dc_amt":5000}]}
```

위 세 건은 모두 `ORDER_CREATED`로 변환되며, C는 `mem_no`가 빈 값이라 `user_id = null`, `order_item_id = C-20261012-0001-1`이 됩니다.

---

## 4. 처리 규칙

**중복·순서**
- 중복 제거 키: `channel + event_id`. 동일 키가 다시 오면 무시(멱등).
- 같은 `event_id`에 내용이 다른 경우: 최초 건 유지, 충돌 건은 Quarantine(`DUPLICATE_CONFLICT`).
- 결제가 주문보다 먼저 와도 격리하지 않음. Gold에서 `order_id`로 조인.
- 환불이 결제보다 먼저 오는 경우도 격리하지 않고 Gold 조인 시점에 매칭.

**Quarantine 사유 코드**

| 코드 | 조건 |
|---|---|
| `MALFORMED_JSON` | 파싱 불가 |
| `UNKNOWN_EVENT_TYPE` | 매핑표에 없는 이벤트 코드 |
| `MISSING_REQUIRED` | 필수 필드 누락 |
| `INVALID_TYPE` | 정수/시간 형식 오류 |
| `INVALID_VALUE` | 음수 금액, 수량 0 이하, 할인 > 단가×수량 |
| `UNKNOWN_CHANNEL` | channel이 A/B/C 아님 |
| `DUPLICATE_CONFLICT` | 같은 event_id, 다른 내용 |

**지표와의 연결**
- 결제일 귀속: `PAYMENT_SUCCEEDED.event_time` 기준. 환불은 `payment_id`로 원결제일에 귀속.
- 총판매금액: 결제 성공 건의 상품금액 합. 순판매금액 = 총판매금액 − 환불금액.
- 결제고객수: 결제 성공 이벤트의 `channel + user_id` distinct (`user_id` null 제외, 이후 취소·반품 포함).
- 상품결제건수: 결제 성공 주문의 상품 행 수(item 단위).

---

## 5. 확정이 필요한 항목

1. **결제 1건이 여러 상품을 포함하는 경우**: `payment_amount`를 주문 전체 금액으로 두고, 상품별 총판매금액은 Gold에서 `상품금액`으로 계산하는 안을 전제함. 맞는지 확인 필요.
2. **부분 결제/분할 결제**: 한 주문에 결제 성공 2건 이상이 가능하다고 보되, 생성기는 PoC 1차에서 주문당 성공 1건만 생성할지 결정 필요.
3. **취소 단위**: 주문 취소는 주문 전체만 허용(상품별 취소는 환불로 처리)하는 안.
4. **배송비 환불**: 환불은 상품 단위만 두고 배송비 환불은 제외하는 안. 순매출 정의와 연결됨.
5. **채널 A/B/C 형식 차이의 정도**: 위 차이(필드명·시간·코드)가 과한지, 더 단순화할지.

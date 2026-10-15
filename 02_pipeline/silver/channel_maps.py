"""채널별 원본 필드 매핑표 (선언형). 이벤트 명세 2.1~2.3절을 그대로 옮긴 데이터이고 로직은 없다.

native_convert.py가 이 표로 Spark 표현식을 만든다. 채널을 하나 더하거나 필드 이름이 바뀌면 이 표만 고친다.
[주의] Python 변환기(channels/*.py, 테스트의 기준 구현)와 Bronze 재계산 대조(quality/reconcile_sql.py)는 이 표를 쓰지 않는다.
  같은 해석 오류를 여러 경로가 함께 하지 않도록 세 곳이 각자 명세를 옮겨 적는다.

표 항목
  schema   payload를 from_json으로 읽는 스키마. 모든 값을 문자열로 읽는다 (정수 여부는 표현식이 판정한다)
  fields   공통 이름 -> 원본 필드 경로 ("a.b"는 중첩 객체). time은 원본 시각 문자열 필드다
  time     시각 형식: "iso_utc" (2026-10-12T01:23:45.123Z) | "epoch_ms" | "kst_text" (yyyy-MM-dd HH:mm:ss, KST)
  item     상품 행 필드 경로. id가 {"order_no_seq": 필드}면 주문 번호 + "-" + 순번으로 만든다 (C)
  refund_item  환불이 가리키는 상품 행: 필드 경로 또는 {"order_no_seq": 필드} (C)
  events / methods / reasons / fails / refund_types   원본 코드 -> 공통 값
"""

CHANNEL_MAPS = {
    "A": {
        "schema": ("event_type string, event_id string, occurred_at string, order_id string, user_id string, shipping_fee string, "
                   "items array<struct<order_item_id:string, sku:string, product_name:string, category:string, quantity:string, "
                   "unit_price:string, discount_amount:string>>, reason string, payment_id string, method string, amount string, "
                   "failure_reason string, refund_id string, order_item_id string, refund_quantity string, type string"),
        "fields": {"code": "event_type", "event_id": "event_id", "order_id": "order_id", "user_id": "user_id", "time": "occurred_at",
                   "shipping": "shipping_fee", "items": "items", "cancel_reason": "reason", "payment_id": "payment_id",
                   "method": "method", "amount": "amount", "fail": "failure_reason", "refund_id": "refund_id",
                   "refund_payment": "payment_id", "refund_qty": "refund_quantity", "refund_amount": "amount",
                   "refund_type": "type", "refund_reason": "reason"},
        "time": "iso_utc",
        "item": {"id": "order_item_id", "sku": "sku", "name": "product_name", "category": "category", "qty": "quantity",
                 "price": "unit_price", "discount": "discount_amount"},
        "refund_item": "order_item_id",
        "events": {"order.created": "ORDER_CREATED", "order.cancelled": "ORDER_CANCELLED", "payment.succeeded": "PAYMENT_SUCCEEDED",
                   "payment.failed": "PAYMENT_FAILED", "refund.succeeded": "REFUND_SUCCEEDED"},
        "methods": {"card": "CARD", "easy_pay": "EASY_PAY", "bank": "BANK_TRANSFER", "vbank": "VIRTUAL_ACCOUNT"},
        "reasons": {"change_of_mind": "CHANGE_OF_MIND", "defect": "DEFECT", "wrong_item": "WRONG_ITEM", "delay": "DELAY"},
        "fails": {"limit_exceeded": "LIMIT_EXCEEDED", "declined": "CARD_DECLINED", "timeout": "TIMEOUT"},
        "refund_types": {"cancel": "CANCEL", "return": "RETURN"},
    },
    "B": {
        "schema": ("eventName string, eventId string, occurredAtMs string, orderNo string, buyer struct<memberId:string>, "
                   "delivery struct<fee:string>, orderItems array<struct<lineId:string, sellerSku:string, itemName:string, "
                   "categoryName:string, qty:string, unitPrice:string, discount:struct<total:string>>>, cancelReason string, "
                   "paymentId string, payMethod string, paidAmount string, declineCode string, refundId string, lineId string, "
                   "refundQty string, refundAmount string, claimType string, claimReason string"),
        "fields": {"code": "eventName", "event_id": "eventId", "order_id": "orderNo", "user_id": "buyer.memberId", "time": "occurredAtMs",
                   "shipping": "delivery.fee", "items": "orderItems", "cancel_reason": "cancelReason", "payment_id": "paymentId",
                   "method": "payMethod", "amount": "paidAmount", "fail": "declineCode", "refund_id": "refundId",
                   "refund_payment": "paymentId", "refund_qty": "refundQty", "refund_amount": "refundAmount",
                   "refund_type": "claimType", "refund_reason": "claimReason"},
        "time": "epoch_ms",
        "item": {"id": "lineId", "sku": "sellerSku", "name": "itemName", "category": "categoryName", "qty": "qty",
                 "price": "unitPrice", "discount": "discount.total"},
        "refund_item": "lineId",
        "events": {"ORDER_PLACED": "ORDER_CREATED", "ORDER_CANCELED": "ORDER_CANCELLED", "PAY_APPROVED": "PAYMENT_SUCCEEDED",
                   "PAY_DECLINED": "PAYMENT_FAILED", "REFUND_DONE": "REFUND_SUCCEEDED"},
        "methods": {"CARD": "CARD", "SIMPLE": "EASY_PAY", "TRANSFER": "BANK_TRANSFER", "VBANK": "VIRTUAL_ACCOUNT"},
        "reasons": {"BUYER_REMORSE": "CHANGE_OF_MIND", "DEFECTIVE": "DEFECT", "WRONG_DELIVERY": "WRONG_ITEM", "DELAYED": "DELAY"},
        "fails": {"LIMIT": "LIMIT_EXCEEDED", "DECLINED": "CARD_DECLINED", "TIMEOUT": "TIMEOUT"},
        "refund_types": {"CANCEL": "CANCEL", "RETURN": "RETURN"},
    },
    "C": {
        "schema": ("evt_cd string, evt_id string, evt_dt string, ord_no string, mem_no string, dlv_amt string, "
                   "lines array<struct<seq:string, goods_cd:string, goods_nm:string, cate_nm:string, qty:string, price:string, dc_amt:string>>, "
                   "rsn_cd string, pay_no string, pay_type string, pay_amt string, fail_cd string, rfd_no string, seq string, "
                   "rfd_qty string, rfd_amt string, clm_tp string"),
        "fields": {"code": "evt_cd", "event_id": "evt_id", "order_id": "ord_no", "user_id": "mem_no", "time": "evt_dt",
                   "shipping": "dlv_amt", "items": "lines", "cancel_reason": "rsn_cd", "payment_id": "pay_no",
                   "method": "pay_type", "amount": "pay_amt", "fail": "fail_cd", "refund_id": "rfd_no",
                   "refund_payment": "pay_no", "refund_qty": "rfd_qty", "refund_amount": "rfd_amt",
                   "refund_type": "clm_tp", "refund_reason": "rsn_cd"},
        "time": "kst_text",
        "item": {"id": {"order_no_seq": "seq"}, "sku": "goods_cd", "name": "goods_nm", "category": "cate_nm", "qty": "qty",
                 "price": "price", "discount": "dc_amt"},
        "refund_item": {"order_no_seq": "seq"},
        "events": {"O01": "ORDER_CREATED", "O09": "ORDER_CANCELLED", "P01": "PAYMENT_SUCCEEDED", "P02": "PAYMENT_FAILED",
                   "R01": "REFUND_SUCCEEDED"},
        "methods": {"10": "CARD", "20": "EASY_PAY", "30": "BANK_TRANSFER", "40": "VIRTUAL_ACCOUNT"},
        "reasons": {"01": "CHANGE_OF_MIND", "02": "DEFECT", "03": "WRONG_ITEM", "04": "DELAY"},
        "fails": {"F1": "LIMIT_EXCEEDED", "F2": "CARD_DECLINED", "F3": "TIMEOUT"},
        "refund_types": {"1": "CANCEL", "2": "RETURN"},
    },
}

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import _events as ev
import channel_c


class TestChannelC(unittest.TestCase):
    def test_order_created(self):
        self.assertEqual(channel_c.to_raw(ev.created("C")), {
            "evt_id": "C-S01-1-E1", "evt_cd": "O01", "evt_dt": "2026-10-12 10:00:00",
            "ord_no": "C-S01-1", "mem_no": "U01", "dlv_amt": 0,
            "lines": [{"seq": 1, "goods_cd": "SKU-004", "goods_nm": "러닝화", "cate_nm": "신발",
                       "qty": 1, "price": 89000, "dc_amt": 0}]})

    def test_logged_out_user_is_empty_string(self):
        self.assertEqual(channel_c.to_raw(ev.created("C", user_id=None))["mem_no"], "")

    def test_payment_succeeded(self):
        self.assertEqual(channel_c.to_raw(ev.payment_ok("C")), {
            "evt_id": "C-S01-1-E2", "evt_cd": "P01", "evt_dt": "2026-10-12 10:00:30",
            "ord_no": "C-S01-1", "mem_no": "U01",
            "pay_no": "C-S01-1-P1", "pay_type": "10", "pay_amt": 89000})

    def test_payment_failed(self):
        raw = channel_c.to_raw(ev.payment_failed("C"))
        self.assertEqual(raw["evt_cd"], "P02")
        self.assertEqual(raw["fail_cd"], "F1")

    def test_order_cancelled(self):
        raw = channel_c.to_raw(ev.cancelled("C"))
        self.assertEqual(raw["evt_cd"], "O09")
        self.assertEqual(raw["rsn_cd"], "01")

    def test_refund_succeeded(self):
        raw = channel_c.to_raw(ev.refund("C"))
        self.assertEqual(raw["evt_cd"], "R01")
        self.assertEqual(raw["rfd_no"], "C-S09-1-R1")
        self.assertEqual(raw["pay_no"], "C-S09-1-P1")
        # order_item_id "C-S09-1-1" 은 ord_no + "-" + seq 로 복원되어야 한다
        self.assertEqual((raw["ord_no"], raw["seq"]), ("C-S09-1", 1))
        self.assertEqual((raw["rfd_amt"], raw["clm_tp"], raw["rsn_cd"]), (35000, "2", "02"))

    @unittest.skip("명세 승인 대기: 환불 수량 필드명 (제안: rfd_qty)")
    def test_refund_quantity(self):
        self.assertEqual(channel_c.to_raw(ev.refund("C"))["rfd_qty"], 1)


if __name__ == "__main__":
    unittest.main()

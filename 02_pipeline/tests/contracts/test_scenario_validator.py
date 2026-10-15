import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
CONTRACTS = ROOT / "01_contracts" / "contracts"

import copy

import scenario_loader
import scenario_validator


class TestScenarioValidator(unittest.TestCase):
    def setUp(self):
        self.skus = scenario_loader.load_skus(CONTRACTS / "skus.json")
        self.scenarios = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")

    def test_real_data_is_valid(self):
        scenario_validator.validate_scenarios(self.scenarios, self.skus)  # 예외가 없어야 한다

    def test_unknown_sku_is_rejected(self):
        bad = copy.deepcopy(self.scenarios)
        bad[0]["orders"][0]["items"][0]["sku"] = "SKU-999"
        with self.assertRaises(ValueError):
            scenario_validator.validate_scenarios(bad, self.skus)

    def test_unknown_event_type_is_rejected(self):
        bad = copy.deepcopy(self.scenarios)
        bad[0]["orders"][0]["events"][0]["type"] = "SHIPPING_STARTED"
        with self.assertRaises(ValueError):
            scenario_validator.validate_scenarios(bad, self.skus)


    def test_reuse_item_id_must_point_at_an_earlier_row(self):
        for bad_value in (0, 1, 3, "x"):    # 1번 행 자신, 없는 행, 숫자가 아님
            bad = copy.deepcopy(self.scenarios)
            s28 = next(s for s in bad if s["id"] == "S28")
            s28["orders"][0]["items"][0]["reuse_item_id_of"] = bad_value
            with self.assertRaises(ValueError, msg=bad_value):
                scenario_validator.validate_scenarios(bad, self.skus)

    def test_refund_payment_failed_needs_an_earlier_failed_payment(self):
        bad = copy.deepcopy(self.scenarios)
        s09 = next(s for s in bad if s["id"] == "S09")
        s09["orders"][0]["events"][-1]["payment"] = "failed"        # S09에는 결제 실패가 없다
        with self.assertRaises(ValueError):
            scenario_validator.validate_scenarios(bad, self.skus)

    def test_refund_payment_value_other_than_failed_is_rejected(self):
        bad = copy.deepcopy(self.scenarios)
        s26 = next(s for s in bad if s["id"] == "S26")
        s26["orders"][0]["events"][-1]["payment"] = "succeeded"
        with self.assertRaises(ValueError):
            scenario_validator.validate_scenarios(bad, self.skus)

    def test_refund_order_no_must_exist_in_the_scenario(self):
        bad = copy.deepcopy(self.scenarios)
        s27 = next(s for s in bad if s["id"] == "S27")
        s27["orders"][0]["events"][-1]["order_no"] = 9
        with self.assertRaises(ValueError):
            scenario_validator.validate_scenarios(bad, self.skus)


if __name__ == "__main__":
    unittest.main()

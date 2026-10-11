import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
CONTRACTS = ROOT / "01_contracts" / "contracts"

import copy

import scenario_loader


class TestScenarioLoader(unittest.TestCase):
    def setUp(self):
        self.skus = scenario_loader.load_skus(CONTRACTS / "skus.json")
        self.scenarios = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")

    def test_load_skus_is_keyed_by_product_id(self):
        self.assertEqual(len(self.skus), 15)
        self.assertEqual(self.skus["SKU-004"]["unit_price"], 89000)
        self.assertEqual(self.skus["SKU-004"]["product_name"], "러닝화")

    def test_load_scenarios_keeps_order(self):
        ids = [s["id"] for s in self.scenarios]
        self.assertEqual(ids, ["S01", "S02", "S04", "S06", "S08", "S09", "S19", "S21", "S23"])

    def test_real_data_is_valid(self):
        scenario_loader.validate_scenarios(self.scenarios, self.skus)  # 예외가 없어야 한다

    def test_unknown_sku_is_rejected(self):
        bad = copy.deepcopy(self.scenarios)
        bad[0]["orders"][0]["items"][0]["sku"] = "SKU-999"
        with self.assertRaises(ValueError):
            scenario_loader.validate_scenarios(bad, self.skus)

    def test_unknown_event_type_is_rejected(self):
        bad = copy.deepcopy(self.scenarios)
        bad[0]["orders"][0]["events"][0]["type"] = "SHIPPING_STARTED"
        with self.assertRaises(ValueError):
            scenario_loader.validate_scenarios(bad, self.skus)


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
CONTRACTS = ROOT / "01_contracts" / "contracts"

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
        self.assertEqual(ids, ["S01", "S02", "S04", "S06", "S08", "S09", "S19", "S21", "S23", "S24", "S25", "S26", "S27", "S28"])


if __name__ == "__main__":
    unittest.main()

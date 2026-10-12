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


if __name__ == "__main__":
    unittest.main()

import inspect
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "bronze"))

import bronze_check


class TestBronzeCheckImport(unittest.TestCase):
    def test_imports_without_pyspark_and_finds_fixtures(self):
        self.assertTrue((bronze_check.DEFAULT_FIXTURES / "A" / "events.jsonl").is_file())

    def test_checks_have_no_time_cutoff(self):
        for function in (bronze_check.check_bronze, bronze_check.load_bronze_pairs):
            self.assertNotIn("since", inspect.signature(function).parameters, function.__name__)

    def test_reuses_verify_arrival_compare(self):
        self.assertEqual(bronze_check.compare(bronze_check.load_expected(bronze_check.DEFAULT_FIXTURES),
                                              bronze_check.load_expected(bronze_check.DEFAULT_FIXTURES)), [])


if __name__ == "__main__":
    unittest.main()

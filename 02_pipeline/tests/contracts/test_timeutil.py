import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
CONTRACTS = ROOT / "01_contracts" / "contracts"

from datetime import datetime, timedelta, timezone

import timeutil

UTC = timezone.utc
S01_ORDER = datetime(2026, 10, 12, 1, 0, 0, tzinfo=UTC)      # KST 10:00:00
S23_ORDER = datetime(2026, 10, 11, 15, 29, 30, tzinfo=UTC)   # KST 2026-10-12 00:29:30


class TestTimeutil(unittest.TestCase):
    def test_to_iso_utc(self):
        self.assertEqual(timeutil.to_iso_utc(S01_ORDER), "2026-10-12T01:00:00.000Z")

    def test_to_iso_utc_from_kst_aware(self):
        kst = datetime(2026, 10, 12, 10, 0, 0, tzinfo=timezone(timedelta(hours=9)))
        self.assertEqual(timeutil.to_iso_utc(kst), "2026-10-12T01:00:00.000Z")

    def test_to_epoch_ms(self):
        self.assertEqual(timeutil.to_epoch_ms(S01_ORDER), 1791766800000)
        self.assertEqual(timeutil.to_epoch_ms(S23_ORDER), 1791732570000)

    def test_to_kst_string(self):
        self.assertEqual(timeutil.to_kst_string(S01_ORDER), "2026-10-12 10:00:00")

    def test_to_kst_string_crosses_midnight(self):
        # UTC 날짜는 10-11이지만 KST로는 다음 날이다 (S23)
        self.assertEqual(timeutil.to_kst_string(S23_ORDER), "2026-10-12 00:29:30")


if __name__ == "__main__":
    unittest.main()

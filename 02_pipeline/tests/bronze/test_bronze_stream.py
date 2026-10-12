import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "bronze"))

import bronze_stream


class TestKafkaOptions(unittest.TestCase):
    def setUp(self):
        self.options = bronze_stream.kafka_options("ns.servicebus.windows.net", "commerce-events", "cred")

    def test_bootstrap_uses_eventhubs_kafka_port(self):
        self.assertEqual(self.options["kafka.bootstrap.servers"], "ns.servicebus.windows.net:9093")

    def test_subscribes_to_hub_from_earliest(self):
        self.assertEqual(self.options["subscribe"], "commerce-events")
        self.assertEqual(self.options["startingOffsets"], "earliest")

    def test_uses_service_credential(self):
        self.assertEqual(self.options["databricks.serviceCredential"], "cred")

    def test_has_no_sasl_or_security_options(self):
        self.assertFalse([k for k in self.options if k.startswith("kafka.sasl") or k == "kafka.security.protocol"])

    def test_group_id_only_when_given(self):
        self.assertNotIn("kafka.group.id", self.options)
        with_group = bronze_stream.kafka_options("ns.servicebus.windows.net", "h", "c", "databricks")
        self.assertEqual(with_group["kafka.group.id"], "databricks")


if __name__ == "__main__":
    unittest.main()

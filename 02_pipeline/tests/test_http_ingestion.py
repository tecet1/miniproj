"""Offline routing and HTTP serialization checks; no Azure requests are performed."""

import importlib.util
import json
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[2]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HTTPIngestionTests(unittest.TestCase):
    def setUp(self):
        self.sender = load_module("sender", ROOT / "02_pipeline/ingestion/send_http_events.py")
        self.events = self.sender.load_events(ROOT / "01_contracts/data/fixtures/channel_a/events.jsonl")
        self.producer = MagicMock()
        self.factory = MagicMock()
        self.factory.return_value.__enter__.return_value = self.producer
        azure = types.ModuleType("azure")
        azure.__path__ = []
        functions = types.ModuleType("azure.functions")
        functions.AuthLevel = types.SimpleNamespace(FUNCTION="function")
        functions.FunctionApp = lambda **kwargs: types.SimpleNamespace(route=lambda **kwargs: lambda f: f)
        functions.HttpRequest = object
        functions.HttpResponse = lambda body, status_code: types.SimpleNamespace(body=body, status_code=status_code)
        eventhub = types.ModuleType("azure.eventhub")
        eventhub.EventData = lambda data: data
        eventhub.EventHubProducerClient = self.factory
        identity = types.ModuleType("azure.identity")
        identity.ManagedIdentityCredential = MagicMock()
        modules = {"azure": azure, "azure.functions": functions,
                   "azure.eventhub": eventhub, "azure.identity": identity}
        with patch.dict(sys.modules, modules), patch.dict(os.environ, {
            "EVENT_HUB_NAMESPACE": "example.servicebus.windows.net", "EVENT_HUB_NAME": "commerce-events",
        }):
            self.receiver = load_module("receiver", ROOT / "02_pipeline/ingestion/function_app.py")

    def test_all_six_raw_events_use_order_no_and_keep_the_body(self):
        for event in self.events:
            with self.subTest(event_id=event["event_id"]):
                request = types.SimpleNamespace(get_json=lambda: event)
                response = self.receiver.receive_events(request)
                self.assertEqual(response.status_code, 200)
                self.producer.create_batch.assert_called_with(partition_key=f"A:{event['order_no']}")
                body = self.producer.create_batch.return_value.add.call_args.args[0]
                self.assertEqual(json.loads(body), event)

    def test_missing_order_no_and_non_object_are_rejected_before_sending(self):
        for event in ({"channel_id": "A", "order_id": "A-001"}, []):
            response = self.receiver.receive_events(types.SimpleNamespace(get_json=lambda: event))
            self.assertEqual(response.status_code, 400)
        self.factory.assert_not_called()

    def test_http_sender_adds_test_metadata_and_does_not_mutate_business_fields(self):
        event = self.events[0]
        before = json.loads(json.dumps(event))
        with patch.object(self.sender, "urlopen") as opened:
            opened.return_value.__enter__.return_value.status = 200
            status = self.sender.send_event("https://example.invalid/api/receive_events", "demo-key", event, run_id="demo-run")
        self.assertEqual(status, 200)
        request = opened.call_args.args[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("X-functions-key"), "demo-key")
        body = json.loads(request.data)
        self.assertEqual(body.pop("test_run_id"), "demo-run")
        self.assertIn("+00:00", body.pop("published_at"))
        self.assertEqual(body, event)
        self.assertEqual(event, before)


if __name__ == "__main__":
    unittest.main()

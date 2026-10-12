import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "ingestion"))

import json

import ingest

RECEIVED_AT = "2026-10-12T01:00:00.123Z"


class FakeSend:
    """send 대신 넘기는 가짜. 호출된 인자를 기록하고, fail=True면 예외를 낸다."""
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def __call__(self, envelope_json):
        self.calls.append(envelope_json)
        if self.fail:
            raise RuntimeError("event hubs down")


class TestHandleEvent(unittest.TestCase):
    def test_valid_channel_is_accepted_and_sent_once(self):
        send = FakeSend()
        status, _ = ingest.handle_event("A", b'{"event_id":"x"}', send, RECEIVED_AT)
        self.assertEqual(status, 202)
        self.assertEqual(len(send.calls), 1)

    def test_envelope_has_channel_received_at_and_payload(self):
        send = FakeSend()
        ingest.handle_event("B", b'{"eventId":"x"}', send, RECEIVED_AT)
        self.assertEqual(json.loads(send.calls[0]),
                         {"channel": "B", "received_at": RECEIVED_AT, "payload": '{"eventId":"x"}'})

    def test_payload_is_preserved_exactly(self):
        body = '{"a":  1,\n "b": "한글 \\" \\\\ ",  "c":[1 , 2]}'      # 공백, 줄바꿈, 이스케이프, 한글
        send = FakeSend()
        ingest.handle_event("C", body.encode("utf-8"), send, RECEIVED_AT)
        self.assertEqual(json.loads(send.calls[0])["payload"], body)
        self.assertIn("한글", send.calls[0])                              # 한글이 이스케이프되지 않음

    def test_malformed_json_is_still_accepted(self):
        # 해석하지 않는다. 격리는 Silver의 일이다.
        send = FakeSend()
        status, _ = ingest.handle_event("A", b"{not json", send, RECEIVED_AT)
        self.assertEqual(status, 202)
        self.assertEqual(json.loads(send.calls[0])["payload"], "{not json")

    def test_unknown_channel_is_rejected_without_sending(self):
        for channel in ("D", "a", "", "AB"):
            with self.subTest(channel=channel):
                send = FakeSend()
                status, _ = ingest.handle_event(channel, b"{}", send, RECEIVED_AT)
                self.assertEqual(status, 400)
                self.assertEqual(send.calls, [])

    def test_non_utf8_body_is_rejected_without_sending(self):
        send = FakeSend()
        status, _ = ingest.handle_event("A", b"\xff\xfe\xfa", send, RECEIVED_AT)
        self.assertEqual(status, 400)
        self.assertEqual(send.calls, [])

    def test_send_failure_returns_503(self):
        status, _ = ingest.handle_event("A", b"{}", FakeSend(fail=True), RECEIVED_AT)
        self.assertEqual(status, 503)


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "ingestion"))

import json
import tempfile

import send_events

URL = "https://example.test"


class FakePost:
    """상태 코드(또는 예외)를 차례로 돌려주는 가짜 post. 호출 인자를 기록한다."""
    def __init__(self, *outcomes):
        self.outcomes, self.calls = list(outcomes), []

    def __call__(self, url, key, body):
        self.calls.append((url, key, body))
        outcome = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class TestSendEvent(unittest.TestCase):
    def setUp(self):
        self.sleeps = []

    def send(self, post, retries=3):
        send_events.send_event(URL, "k", "A", '{"x":1}', post=post, retries=retries, sleep=self.sleeps.append)

    def test_success_posts_payload_to_channel_url(self):
        post = FakePost(202)
        self.send(post)
        self.assertEqual(post.calls, [(f"{URL}/api/events/A", "k", '{"x":1}')])
        self.assertEqual(self.sleeps, [])

    def test_trailing_slash_in_base_url_is_ok(self):
        post = FakePost(202)
        send_events.send_event(URL + "/", "k", "B", "{}", post=post, sleep=self.sleeps.append)
        self.assertEqual(post.calls[0][0], f"{URL}/api/events/B")

    def test_retries_on_503_then_succeeds(self):
        post = FakePost(503, 503, 202)
        self.send(post)
        self.assertEqual(len(post.calls), 3)
        self.assertEqual(self.sleeps, [1, 2])                # 1초, 2초 간격

    def test_retries_on_connection_error(self):
        post = FakePost(ConnectionError("down"), 202)
        self.send(post)
        self.assertEqual(len(post.calls), 2)

    def test_gives_up_after_retries(self):
        post = FakePost(503)
        with self.assertRaises(send_events.SendError):
            self.send(post, retries=3)
        self.assertEqual(len(post.calls), 3)

    def test_4xx_is_not_retried(self):
        post = FakePost(400)
        with self.assertRaises(send_events.SendError):
            self.send(post)
        self.assertEqual(len(post.calls), 1)


class TestReadAndSendAll(unittest.TestCase):
    def test_read_events_keeps_order_and_duplicates(self):
        lines = [{"channel": "A", "payload": "p1"}, {"channel": "B", "payload": "p2"},
                 {"channel": "A", "payload": "p1"}]
        with tempfile.TemporaryDirectory() as tmp:
            path = f"{tmp}/events.jsonl"
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write("\n".join(json.dumps(line) for line in lines) + "\n")
            self.assertEqual(send_events.read_events(path), [("A", "p1"), ("B", "p2"), ("A", "p1")])

    def test_send_all_counts_by_channel(self):
        post = FakePost(202)
        sent = send_events.send_all([("A", "1"), ("B", "2"), ("A", "3")], URL, "k", post=post)
        self.assertEqual(sent, {"A": 2, "B": 1, "C": 0})
        self.assertEqual([c[2] for c in post.calls], ["1", "2", "3"])


if __name__ == "__main__":
    unittest.main()

"""Send the fixed fixture through an existing Azure Function, one JSON per POST."""

import argparse
import getpass
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4


DEFAULT_FILE = Path(__file__).resolve().parents[2] / "01_contracts/data/fixtures/channel_a/events.jsonl"


def load_events(path: Path) -> list[dict]:
    events = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            event = json.loads(line)
            if not isinstance(event, dict) or not isinstance(event.get("event_id"), str):
                raise ValueError(f"Invalid event at line {line_number}")
            events.append(event)
    if not events:
        raise ValueError("Fixture contains no events")
    return events


def send_event(url: str, function_key: str, event: dict, *, run_id: str) -> int:
    message = {**event, "test_run_id": run_id,
               "published_at": datetime.now(timezone.utc).isoformat()}
    request = Request(
        url,
        data=json.dumps(message, ensure_ascii=False, allow_nan=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "x-functions-key": function_key},
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        return response.status


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.environ.get("FUNCTION_APP_URL"))
    parser.add_argument("--file", type=Path, default=DEFAULT_FILE)
    parser.add_argument("--interval", type=float, default=10, help="Seconds between requests")
    parser.add_argument("--limit", type=int, help="Send only the first N events")
    args = parser.parse_args()
    address = urlsplit(args.url or "")
    if (address.scheme != "https" or not address.netloc or address.query
            or address.username or address.password or address.fragment):
        parser.error("Set --url or FUNCTION_APP_URL to the HTTPS function URL without query parameters")
    if args.interval < 0 or (args.limit is not None and args.limit <= 0):
        parser.error("interval must be non-negative and limit must be positive")
    events = load_events(args.file)
    if args.limit is not None:
        events = events[:args.limit]
    function_key = os.environ.get("FUNCTION_APP_KEY") or getpass.getpass("Function key: ")
    if not function_key:
        parser.error("Function key is required")
    run_id = uuid4().hex
    print(f"test_run_id={run_id}; expected_messages={len(events)}", flush=True)
    for index, event in enumerate(events):
        try:
            status = send_event(args.url, function_key, event, run_id=run_id)
        except HTTPError as error:
            raise SystemExit(f"Stopped at {event['event_id']}: HTTP {error.code}") from None
        except URLError:
            raise SystemExit(f"Stopped at {event['event_id']}: connection failed") from None
        print(f"{event['event_id']}: HTTP {status}", flush=True)
        if index + 1 < len(events):
            time.sleep(args.interval)


if __name__ == "__main__":
    main()

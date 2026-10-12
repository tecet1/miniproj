"""Event Hubs 전송. Managed Identity(DefaultAzureCredential)로 인증하고 연결 문자열은 쓰지 않는다.

필요한 앱 설정(환경 변수): EVENTHUB_FQDN (예: <네임스페이스>.servicebus.windows.net), EVENTHUB_NAME (commerce-events)
Function의 관리 ID에는 'Azure Event Hubs Data Sender' 역할이 필요하다.
"""
import os

from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import DefaultAzureCredential

_producer = None


def _get_producer() -> EventHubProducerClient:
    """생산자 클라이언트는 프로세스에서 한 번만 만들어 재사용한다 (요청마다 연결을 새로 만들지 않기 위함)."""
    global _producer
    if _producer is None:
        _producer = EventHubProducerClient(
            fully_qualified_namespace=os.environ["EVENTHUB_FQDN"],
            eventhub_name=os.environ["EVENTHUB_NAME"],
            credential=DefaultAzureCredential(),
        )
    return _producer


def send(envelope_json: str) -> None:
    """봉투 JSON 문자열 한 건을 Event Hubs로 보낸다. 파티션 키를 지정하지 않으므로 라운드로빈으로 분산된다."""
    producer = _get_producer()
    batch = producer.create_batch()
    batch.add(EventData(envelope_json))
    producer.send_batch(batch)

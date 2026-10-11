"""Channel-A HTTP receiver; apply the routing fix to the existing Azure app."""

import json
import os

import azure.functions as func
from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import ManagedIdentityCredential


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
event_hub_namespace = os.environ["EVENT_HUB_NAMESPACE"]
event_hub_name = os.environ["EVENT_HUB_NAME"]


@app.route(route="receive_events", methods=["POST"])
def receive_events(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event = req.get_json()
    except ValueError:
        return func.HttpResponse("Request body must be valid JSON", status_code=400)
    if not isinstance(event, dict):
        return func.HttpResponse("Request body must be one JSON object", status_code=400)

    # 추가: 파티션 키에 필요한 원본 필드가 없으면 500 대신 400으로 응답한다.
    if any(not isinstance(event.get(field), str) or not event[field].strip()
           for field in ("channel_id", "order_no")):
        return func.HttpResponse("channel_id and order_no must be non-empty strings", status_code=400)

    with ManagedIdentityCredential() as credential, EventHubProducerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        credential=credential,
    ) as producer:
        # 변경: order_id는 Silver 필드다. 현재 A 원본의 order_no로 라우팅한다.
        batch = producer.create_batch(partition_key=f"{event['channel_id']}:{event['order_no']}")
        batch.add(EventData(json.dumps(event, ensure_ascii=False, allow_nan=False)))
        producer.send_batch(batch)
    return func.HttpResponse(f"Received event: {event.get('event_id')}", status_code=200)

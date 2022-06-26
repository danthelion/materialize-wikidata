import json

from kafka import KafkaProducer
from sseclient import SSEClient as EventSource


def produce_events_from_url(url: str, topic: str) -> None:
    for event in EventSource(url):
        if event.event == "message":
            try:
                parsed_event = json.loads(event.data)
            except ValueError:
                pass
            else:
                key = parsed_event["server_name"]
                # Partiton by server_name
                producer.send(topic, value=json.dumps(parsed_event).encode("utf-8"), key=key.encode("utf-8"))


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers="localhost:63248", client_id="wikidata-producer"
    )
    produce_events_from_url(
        url="https://stream.wikimedia.org/v2/stream/recentchange", topic="recentchange"
    )

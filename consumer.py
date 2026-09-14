"""
Avro consumer reading from the "users" topic.

Usage:
    python consumer.py

Note there's no schema passed in here at all -- the AvroDeserializer
pulls whatever schema ID is embedded in each message's header bytes
from the Schema Registry automatically (cached after first fetch) and
uses it to decode. This is the payoff: consumers don't need to be
redeployed when producers add backward-compatible fields.
"""
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

BOOTSTRAP_SERVERS = "localhost:29092,localhost:29093,localhost:29094"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "users"
GROUP_ID = "users-lab-consumer"


def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client=sr_client)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    print(f"Listening on '{TOPIC}'... Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            record = avro_deserializer(
                msg.value(), SerializationContext(TOPIC, MessageField.VALUE)
            )
            print(f"[offset {msg.offset()}] {record}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

"""
Avro producer against a local Schema Registry.

Usage:
    python producer.py schemas/user_v1.avsc
    python producer.py schemas/user_v2_compatible.avsc
    python producer.py schemas/user_v3_incompatible.avsc   # <- watch this one fail

Each run registers/looks up the given schema against subject
"users-value" and sends one record. The AvroSerializer does the
registry round-trip for you: on first use of a schema it registers it
(subject to the compatibility check), then embeds the returned schema
ID in the message's first 5 bytes (magic byte + 4-byte ID) before the
Avro-encoded payload.
"""
import sys
import uuid

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

BOOTSTRAP_SERVERS = "localhost:29092,localhost:29093,localhost:29094"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "users"


def make_record(schema_path: str) -> dict:
    """Build a record matching whichever schema version was passed in."""
    if "v2" in schema_path:
        return {
            "id": str(uuid.uuid4()),
            "name": "Priya Rao",
            "email": "priya@example.com",
            "loyaltyTier": "GOLD",
        }
    if "v3" in schema_path:
        # v3 dropped "email" -- this is the intentionally-breaking schema
        return {"id": str(uuid.uuid4()), "name": "Priya Rao"}
    return {
        "id": str(uuid.uuid4()),
        "name": "Priya Rao",
        "email": "priya@example.com",
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python producer.py <path-to-avsc>")
        sys.exit(1)

    schema_path = sys.argv[1]
    with open(schema_path, "r") as f:
        schema_str = f.read()

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=schema_str,
    )

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    record = make_record(schema_path)
    print(f"Registering/using schema from {schema_path} and producing: {record}")

    try:
        producer.produce(
            topic=TOPIC,
            value=avro_serializer(record, SerializationContext(TOPIC, MessageField.VALUE)),
            on_delivery=delivery_report,
        )
        producer.flush()
    except Exception as e:
        # This is where a rejected schema registration surfaces --
        # e.g. trying to register user_v3_incompatible.avsc against
        # a BACKWARD-compatible subject that already has v1/v2 registered.
        print(f"FAILED: {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

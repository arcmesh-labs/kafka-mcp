"""
Populates the local Kafka broker with test data so all six tools can be exercised.

Topics created:
  orders          3 partitions — high-volume, consumer group partially behind (lag > 0)
  payments        2 partitions — fully caught-up consumer group (lag == 0)
  dead-letters    1 partition  — no consumer group (tests list/metadata/sample only)

Consumer groups created:
  orders-processor   subscribed to orders   — commits after 50 of 100 messages (lag=50)
  payments-processor subscribed to payments — commits all messages (lag=0)
"""

import json
import os
import time

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = [
    NewTopic("orders",       num_partitions=3, replication_factor=1),
    NewTopic("payments",     num_partitions=2, replication_factor=1),
    NewTopic("dead-letters", num_partitions=1, replication_factor=1),
]

MESSAGES = {
    "orders": [
        {"order_id": i, "customer": f"customer-{i % 20}", "amount": round(9.99 + i * 1.5, 2)}
        for i in range(100)
    ],
    "payments": [
        {"payment_id": i, "order_id": i, "status": "settled" if i % 10 != 0 else "pending"}
        for i in range(60)
    ],
    "dead-letters": [
        {"original_topic": "orders", "reason": "deserialization_error", "offset": i}
        for i in range(5)
    ],
}


def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def _producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP})


def _consumer(group: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })


def create_topics() -> None:
    admin = _admin()
    existing = set(admin.list_topics(timeout=10).topics)
    to_create = [t for t in TOPICS if t.topic not in existing]
    if not to_create:
        print("Topics already exist, skipping creation.")
        return
    fs = admin.create_topics(to_create)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"  Created topic: {topic}")
        except Exception as e:
            print(f"  Topic {topic} already exists or error: {e}")


def produce_messages() -> None:
    producer = _producer()
    for topic, messages in MESSAGES.items():
        for i, msg in enumerate(messages):
            producer.produce(
                topic,
                key=str(i).encode(),
                value=json.dumps(msg).encode(),
            )
        producer.flush()
        print(f"  Produced {len(messages)} messages to {topic}")


def consume_and_commit(group: str, topic: str, commit_after: int) -> None:
    """Consume messages from a topic, commit only the first commit_after."""
    consumer = _consumer(group)
    try:
        consumer.subscribe([topic])
        consumed = 0
        committed = 0
        while consumed < len(MESSAGES[topic]):
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                print(f"  Consumer error: {msg.error()}")
                break
            consumed += 1
            if committed < commit_after:
                consumer.commit(message=msg, asynchronous=False)
                committed += 1
        print(f"  Group '{group}': consumed {consumed}, committed {committed} (lag={consumed - committed})")
    finally:
        consumer.close()


def main() -> None:
    print("Connecting to", BOOTSTRAP)
    time.sleep(2)  # brief pause after healthcheck passes

    print("\n[1/3] Creating topics...")
    create_topics()

    print("\n[2/3] Producing messages...")
    produce_messages()

    print("\n[3/3] Simulating consumer groups...")
    # orders-processor commits 50 of 100 → lag = 50 across 3 partitions
    consume_and_commit("orders-processor", "orders", commit_after=50)
    # payments-processor commits all → lag = 0
    consume_and_commit("payments-processor", "payments", commit_after=len(MESSAGES["payments"]))

    print("\nSeed complete. Expected state:")
    print("  orders        — 100 messages, group 'orders-processor' has lag ~50")
    print("  payments      — 60 messages,  group 'payments-processor' has lag 0")
    print("  dead-letters  — 5 messages,   no consumer group")


if __name__ == "__main__":
    main()

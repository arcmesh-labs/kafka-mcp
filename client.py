import os
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource


def _build_base_config() -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    }
    username = os.environ.get("KAFKA_SASL_USERNAME")
    password = os.environ.get("KAFKA_SASL_PASSWORD")
    if username and password:
        config["sasl.username"] = username
        config["sasl.password"] = password
        config["sasl.mechanism"] = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN")
    return config


def _admin() -> AdminClient:
    return AdminClient(_build_base_config())


def _consumer(group_id: str = "kafka-mcp-internal") -> Consumer:
    return Consumer(
        {
            **_build_base_config(),
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )


# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------


def list_topics() -> list[dict[str, Any]]:
    admin = _admin()
    metadata = admin.list_topics(timeout=10)
    return [
        {"name": name, "partitions": len(t.partitions)}
        for name, t in metadata.topics.items()
        if name != "__consumer_offsets"
    ]


def get_topic_metadata(topic: str) -> dict[str, Any]:
    admin = _admin()
    metadata = admin.list_topics(topic=topic, timeout=10)

    if topic not in metadata.topics:
        return {"success": False, "error": f"Topic '{topic}' not found"}

    topic_meta = metadata.topics[topic]
    if topic_meta.error is not None:
        return {"success": False, "error": str(topic_meta.error)}

    partitions = []
    for pid, p in topic_meta.partitions.items():
        partitions.append(
            {
                "id": pid,
                "leader": p.leader,
                "replicas": list(p.replicas),
                "isrs": list(p.isrs),
            }
        )

    replication_factor = len(partitions[0]["replicas"]) if partitions else 0

    resource = ConfigResource("topic", topic)
    fs = admin.describe_configs([resource])
    configs: dict[str, Any] | None = None
    try:
        result = fs[resource].result()
        configs = {k: v.value for k, v in result.items()}
    except Exception:
        pass

    return {
        "success": True,
        "topic": topic,
        "partition_count": len(partitions),
        "replication_factor": replication_factor,
        "partitions": partitions,
        "configs": configs,
    }


# ---------------------------------------------------------------------------
# Consumer lag
# ---------------------------------------------------------------------------


def get_consumer_lag(topic: str, group: str) -> dict[str, Any]:
    consumer = _consumer(group_id=group)
    try:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        if topic not in metadata.topics:
            return {"success": False, "error": f"Topic '{topic}' not found"}

        topic_meta = metadata.topics[topic]
        tps = [TopicPartition(topic, pid) for pid in topic_meta.partitions]

        # Committed offsets for the group
        committed = consumer.committed(tps, timeout=10)

        # High-watermark (end) offsets
        lag_per_partition = []
        total_lag = 0
        for tp in committed:
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            committed_offset = tp.offset if tp.offset >= 0 else low
            lag = max(0, high - committed_offset)
            total_lag += lag
            lag_per_partition.append(
                {
                    "partition": tp.partition,
                    "committed_offset": committed_offset,
                    "log_end_offset": high,
                    "lag": lag,
                }
            )

        return {
            "success": True,
            "topic": topic,
            "group": group,
            "total_lag": total_lag,
            "partitions": lag_per_partition,
        }
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# Broker health
# ---------------------------------------------------------------------------


def get_broker_health() -> dict[str, Any]:
    admin = _admin()
    metadata = admin.list_topics(timeout=10)

    brokers = [
        {"id": b.id, "host": b.host, "port": b.port}
        for b in metadata.brokers.values()
    ]

    topic_count = sum(
        1 for name in metadata.topics if name != "__consumer_offsets"
    )

    return {
        "success": True,
        "broker_count": len(brokers),
        "brokers": brokers,
        "topic_count": topic_count,
        "controller_id": metadata.controller_id,
    }


# ---------------------------------------------------------------------------
# Sample messages
# ---------------------------------------------------------------------------


def sample_messages(topic: str, n: int = 10) -> dict[str, Any]:
    consumer = _consumer()
    try:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        if topic not in metadata.topics:
            return {"success": False, "error": f"Topic '{topic}' not found"}

        topic_meta = metadata.topics[topic]
        tps = [TopicPartition(topic, pid) for pid in topic_meta.partitions]

        # Seek each partition to max(0, high - n) so we get recent messages
        per_partition = max(1, n // len(tps))
        assign_tps = []
        for tp in tps:
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            start = max(low, high - per_partition)
            assign_tps.append(TopicPartition(topic, tp.partition, start))

        consumer.assign(assign_tps)

        messages = []
        remaining = n
        while remaining > 0:
            msg = consumer.poll(timeout=3.0)
            if msg is None:
                break
            if msg.error():
                break
            value = msg.value()
            messages.append(
                {
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp()[1],
                    "key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
                    "value": value.decode("utf-8", errors="replace") if value else None,
                }
            )
            remaining -= 1

        return {"success": True, "topic": topic, "count": len(messages), "messages": messages}
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# Consumer group status
# ---------------------------------------------------------------------------


def get_consumer_group_status(group: str) -> dict[str, Any]:
    admin = _admin()
    try:
        # list_groups is not available in all versions; use list_consumer_groups if present
        future = admin.list_consumer_groups()
        result = future.result()
        known_groups = {g.group_id for g in result.valid}
    except (AttributeError, KafkaException):
        known_groups = None  # older librdkafka — skip existence check

    if known_groups is not None and group not in known_groups:
        return {"success": False, "error": f"Consumer group '{group}' not found"}

    try:
        futures = admin.describe_consumer_groups([group])
        gd = futures[group].result()
    except KafkaException as exc:
        return {"success": False, "error": str(exc)}

    members = []
    for m in gd.members:
        assignment = []
        if m.assignment:
            assignment = [
                {"topic": tp.topic, "partition": tp.partition}
                for tp in m.assignment.topic_partitions
            ]
        members.append(
            {
                "member_id": m.member_id,
                "client_id": m.client_id,
                "host": m.host,
                "assignment": assignment,
            }
        )

    return {
        "success": True,
        "group": group,
        "state": gd.state.name,
        "protocol_type": gd.protocol_type,
        "partition_assignor": gd.partition_assignor,
        "member_count": len(members),
        "members": members,
    }

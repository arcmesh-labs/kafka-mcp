from typing import Any

from fastmcp import FastMCP

import client

mcp = FastMCP(
    name="kafka-mcp",
    instructions=(
        "Read-only access to a Kafka cluster. "
        "Use these tools to inspect topics, partitions, consumer groups, broker health, "
        "and to sample recent messages. No write operations are available."
    ),
)


@mcp.tool()
def list_topics() -> list[dict[str, Any]]:
    """
    Return a list of all topics in the Kafka cluster.

    Each entry contains:
    - name: topic name
    - partitions: number of partitions

    Use this first to discover available topics before calling topic-specific tools.
    """
    return client.list_topics()


@mcp.tool()
def get_topic_metadata(topic: str) -> dict[str, Any]:
    """
    Return detailed metadata for a single Kafka topic.

    Includes partition layout (leader, replicas, in-sync replicas), replication factor,
    and topic-level configuration values.

    - configs will be null if the broker did not return config data.
    - Returns {success: false, error: ...} if the topic does not exist.

    Args:
        topic: Exact topic name.
    """
    return client.get_topic_metadata(topic)


@mcp.tool()
def get_consumer_lag(topic: str, group: str) -> dict[str, Any]:
    """
    Return the consumer lag for a given topic and consumer group.

    Lag is computed per partition as (log_end_offset - committed_offset).
    A high total_lag means the consumer group is falling behind.

    - committed_offset is the last committed offset for the group on that partition.
    - log_end_offset is the current end of the partition log.
    - Returns {success: false, error: ...} if the topic does not exist.

    Args:
        topic: Exact topic name.
        group: Consumer group ID.
    """
    return client.get_consumer_lag(topic, group)


@mcp.tool()
def get_broker_health() -> dict[str, Any]:
    """
    Return health and basic cluster information for all brokers.

    Includes:
    - broker_count: number of brokers in the cluster
    - brokers: list of {id, host, port} for each broker
    - topic_count: number of user-visible topics (excludes __consumer_offsets)
    - controller_id: ID of the current controller broker

    Use this to verify cluster connectivity and basic health.
    """
    return client.get_broker_health()


@mcp.tool()
def sample_messages(topic: str, n: int = 10) -> dict[str, Any]:
    """
    Sample up to n recent messages from a Kafka topic.

    Messages are drawn from the tail of each partition (most recent), spread evenly
    across partitions. Message values and keys are decoded as UTF-8; non-UTF-8 bytes
    are replaced with the replacement character.

    Each message contains:
    - partition, offset, timestamp (ms epoch)
    - key: string or null
    - value: string or null

    Use this to inspect message shape and recent content without consuming the full topic.

    - Returns {success: false, error: ...} if the topic does not exist.

    Args:
        topic: Exact topic name.
        n: Number of messages to return (default 10, keep low for large topics).
    """
    return client.sample_messages(topic, n)


@mcp.tool()
def get_consumer_group_status(group: str) -> dict[str, Any]:
    """
    Return the current status and member assignments for a consumer group.

    Includes:
    - state: one of Stable, Rebalancing, Empty, Dead, PreparingRebalance
    - protocol_type: typically "consumer"
    - partition_assignor: e.g. "range" or "roundrobin"
    - member_count: number of active members
    - members: list of {member_id, client_id, host, assignment}
      where assignment is a list of {topic, partition}

    Use this to check whether a consumer group is healthy (Stable) or rebalancing,
    and to see which members hold which partitions.

    - Returns {success: false, error: ...} if the group does not exist.

    Args:
        group: Consumer group ID.
    """
    return client.get_consumer_group_status(group)


if __name__ == "__main__":
    mcp.run()

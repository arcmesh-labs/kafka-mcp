# kafka-mcp

Read-only MCP server for Apache Kafka. Exposes cluster inspection, consumer group monitoring, and message sampling as MCP tools.

## Install

### Via apm (recommended)
Install [apm](https://github.com/arcmesh-labs/arcmesh-pm-go) first, then:
```bash
apm install kafka-mcp
```

### Manual
```bash
git clone https://github.com/arcmesh-labs/kafka-mcp.git
pip install -r requirements.txt
```

## Configuration

Set the following environment variables before starting the server:

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | — | Comma-separated bootstrap servers, e.g. `broker1:9092,broker2:9092` |
| `KAFKA_SECURITY_PROTOCOL` | No | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | No | `PLAIN` | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` |
| `KAFKA_SASL_USERNAME` | No | — | Required when using SASL |
| `KAFKA_SASL_PASSWORD` | No | — | Required when using SASL |

### Plaintext (local / dev)

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
python server.py
```

### SASL/SSL (production)

```bash
export KAFKA_BOOTSTRAP_SERVERS=broker1:9093,broker2:9093
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=SCRAM-SHA-256
export KAFKA_SASL_USERNAME=myuser
export KAFKA_SASL_PASSWORD=secret
python server.py
```

## Tools

### `list_topics`
Returns all topics in the cluster with their partition count. Use this first to discover available topics.

### `get_topic_metadata(topic)`
Returns partition layout (leader, replicas, ISRs), replication factor, and topic configs for a single topic.

### `get_consumer_lag(topic, group)`
Returns lag per partition for a consumer group on a topic. `total_lag` is the sum across all partitions — a high value means the group is falling behind.

### `get_broker_health`
Returns broker list, broker count, topic count, and controller ID. Use to verify cluster connectivity.

### `sample_messages(topic, n=10)`
Returns up to `n` recent messages from the tail of the topic. Messages are spread evenly across partitions and decoded as UTF-8. Keep `n` small for high-throughput topics.

### `get_consumer_group_status(group)`
Returns group state (`Stable`, `Rebalancing`, `Empty`, `Dead`), member list, and partition assignments.

## Error handling

All tools return `{"success": false, "error": "..."}` on failure — no exceptions are raised. A successful response always includes `"success": true` (where applicable) or the expected list/object directly.

## Development

```bash
pip install -r requirements.txt
python server.py
```

## Limitations (v1)

- Read-only — no produce, no offset commits, no topic/group management.
- `sample_messages` decodes values as UTF-8; binary (Avro, Protobuf) payloads will be partially garbled.
- `get_consumer_lag` if a consumer group has never committed, committed_offset falls back to the partition start offset — lag will reflect the full unread log, which may be misleading.

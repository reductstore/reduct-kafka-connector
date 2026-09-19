# reduct-kafka-connector
ReductStore Kafka Connectors

Kafka Connect connectors for [ReductStore](https://www.reduct.store):

- **Sink** (`store.reduct.connector.ReductSinkConnector`) — consumes records from Kafka topics and writes them to ReductStore entries.
- **Source** (`store.reduct.connector.ReductSourceConnector`) — reads records from ReductStore entries and produces them to a Kafka topic.

Both connectors use the [reduct-java](../reduct-java) SDK.

## Requirements

- Java 17 (build).
- Maven 3.8+.
- Docker (for the local Kafka + Kafka Connect + ReductStore stack below).

## Project layout

```
src/main/java/store/reduct/connector/
  ReductSinkConnector.java   ReductSinkTask.java
  ReductSourceConnector.java ReductSourceTask.java
  config/                    # ConfigDef definitions
  version/                   # Version
```

Connector classes are discovered through `META-INF/services/org.apache.kafka.connect.{sink,source}.Connector`.

## Build the plugin

The connector jar is a *thin* jar, so Kafka Connect needs the connector jar **plus its runtime dependencies in the same plugin directory**.

```powershell
# 1. (optional) install the SDK into the local Maven repo if it is missing
cd ..\reduct-java
mvn install

# 2. build the connector and assemble a ready-to-deploy plugin directory
cd ..\reduct-kafka-connector
$env:JAVA_HOME = "path\to\jdk-17"
mvn package "-Dplugin.dir=C:/path/to/kafka/plugins/reduct-kafka-connector"
```

`mvn package` without `-Dplugin.dir` writes the plugin to `target/plugin/`.

Resulting plugin directory:

```
reduct-kafka-connector/
  reduct-kafka-connector-0.1.0.jar
  reduct-java-0.2.0.jar
  jackson-databind-2.18.2.jar
  jackson-core-2.18.2.jar
  jackson-annotations-2.18.2.jar
  commons-lang3-3.17.0.jar
  mapstruct-1.6.2.jar
  slf4j-api-2.0.13.jar
```

`org.apache.kafka:connect-api` and `lombok` are `provided`/compile-only and intentionally **not** bundled.

Other useful commands:

```powershell
mvn test                 # unit tests
mvn spotless:apply       # format (Eclipse formatter)
mvn spotless:check       # verify formatting
```

## Run Kafka, Kafka Connect and ReductStore

Create a working directory with a `docker-compose.yml`:

```yaml
version: '3.8'

services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_LISTENERS: 'PLAINTEXT://:9092,INTERNAL://:29092,CONTROLLER://:9093'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://localhost:9092,INTERNAL://kafka:29092'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:9093'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    networks:
      - kafka-net

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      DYNAMIC_CONFIG_ENABLED: 'true'
      KAFKA_CLUSTERS_0_NAME: 'local-cluster'
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: 'kafka:29092'
    depends_on:
      - kafka
    networks:
      - kafka-net

  connect:
    image: confluentinc/cp-kafka-connect:7.7.1
    container_name: connect
    ports:
      - "8083:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: 'kafka:29092'
      CONNECT_REST_ADVERTISED_HOST_NAME: 'connect'
      CONNECT_REST_PORT: '8083'
      CONNECT_GROUP_ID: 'reduct-connect'
      CONNECT_CONFIG_STORAGE_TOPIC: 'reduct-connect-configs'
      CONNECT_OFFSET_STORAGE_TOPIC: 'reduct-connect-offsets'
      CONNECT_STATUS_STORAGE_TOPIC: 'reduct-connect-status'
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: '1'
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: '1'
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: '1'
      CONNECT_KEY_CONVERTER: 'org.apache.kafka.connect.storage.StringConverter'
      CONNECT_VALUE_CONVERTER: 'org.apache.kafka.connect.storage.StringConverter'
      CONNECT_PLUGIN_PATH: '/usr/share/java,/connect-plugins'
    volumes:
      - ./plugins/reduct-kafka-connector:/connect-plugins/reduct-kafka-connector
    depends_on:
      - kafka
    networks:
      - kafka-net

  reductstore:
    image: reduct/store:latest
    container_name: reductstore
    ports:
      - "8383:8383"
    volumes:
      - ./reduct-data:/data
    networks:
      - kafka-net

networks:
  kafka-net:
    driver: bridge
```

Two important details:

1. **`CONNECT_PLUGIN_PATH` must point to the *parent* directory** (`/connect-plugins`), while each connector lives in its own subdirectory. If you point it directly at a directory that contains multiple jars, Kafka Connect treats every jar as a separate isolated plugin and the connector cannot see its dependencies.
2. The broker advertises **two listeners**: `localhost:9092` for clients on the host and `kafka:29092` for containers. Without the internal listener, containers would receive `localhost:9092` in the metadata and fail to connect.

Put the assembled plugin at `./plugins/reduct-kafka-connector` so it matches the volume mount, then start the stack:

```powershell
docker compose up -d
docker compose logs -f connect     # wait for "Finished starting connectors and tasks"
```

Verify that the worker sees the connectors:

```powershell
curl.exe http://localhost:8083/connector-plugins
```

You should see `store.reduct.connector.ReductSinkConnector` and `store.reduct.connector.ReductSourceConnector`.

## Register connectors

Connectors are registered through the Kafka Connect REST API (`http://localhost:8083`).

### Sink — Kafka topic to ReductStore

`sink.json`:

```json
{
  "name": "reduct-sink",
  "config": {
    "connector.class": "store.reduct.connector.ReductSinkConnector",
    "tasks.max": "1",
    "topics": "sensor-temp",
    "reduct.store.url": "http://reductstore:8383",
    "reduct.store.api-token": "",
    "reduct.store.bucket": "kafka",
    "reduct.record.content-type": "application/octet-stream",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}
```

```powershell
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@sink.json"
```

If `reduct.entry.name` is omitted, the Kafka topic name is used as the ReductStore entry name. The sink creates the bucket automatically if it does not exist.

### Source — ReductStore to Kafka topic

`source.json`:

```json
{
  "name": "reduct-source",
  "config": {
    "connector.class": "store.reduct.connector.ReductSourceConnector",
    "tasks.max": "1",
    "reduct.store.url": "http://reductstore:8383",
    "reduct.store.api-token": "",
    "reduct.store.bucket": "kafka",
    "reduct.entry.name": "sensor-temp",
    "reduct.kafka.topic": "sensor-temp-out",
    "reduct.poll.interval.ms": "1000",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
  }
}
```

```powershell
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@source.json"
```

The source requires the bucket to exist (create it with the sink or directly in ReductStore first), and `reduct.kafka.topic` is mandatory. Use `ByteArrayConverter` for the value so raw record bytes are preserved; the sink works fine with `StringConverter`.

### Managing connectors

```powershell
curl.exe http://localhost:8083/connectors/reduct-sink/status
curl.exe http://localhost:8083/connectors/reduct-source/status
curl.exe -X POST "http://localhost:8083/connectors/reduct-source/restart?includeTasks=true"
curl.exe -X DELETE http://localhost:8083/connectors/reduct-sink
```

> Plugins are scanned only at worker startup. After (re)building the plugin, restart the worker: `docker compose restart connect`.

## Configuration reference

### Sink

| Key | Required | Default | Description |
| --- | --- | --- | --- |
| `connector.class` | yes | — | `store.reduct.connector.ReductSinkConnector` |
| `topics` | yes | — | Kafka topic(s) to consume |
| `reduct.store.url` | yes | — | ReductStore URL, e.g. `http://reductstore:8383` |
| `reduct.store.api-token` | no | `""` | API token for authentication |
| `reduct.store.bucket` | yes | — | Target bucket (created if missing) |
| `reduct.entry.name` | no | topic name | Override the target entry name |
| `reduct.record.content-type` | no | `application/octet-stream` | Content type of written records |

### Source

| Key | Required | Default | Description |
| --- | --- | --- | --- |
| `connector.class` | yes | — | `store.reduct.connector.ReductSourceConnector` |
| `reduct.store.url` | yes | — | ReductStore URL |
| `reduct.store.api-token` | no | `""` | API token for authentication |
| `reduct.store.bucket` | yes | — | Bucket to read from (must exist) |
| `reduct.entry.name` | no | — | Entry to read. Ignored when `reduct.entries.all=true` |
| `reduct.entries.all` | no | `false` | Read from all entries of the bucket |
| `reduct.poll.interval.ms` | no | `1000` | Poll interval in milliseconds |
| `reduct.query.start.timestamp` | no | `0` | Start timestamp (µs) used when no offset is stored |
| `reduct.query.ttl` | no | `5` | Query TTL in seconds |
| `reduct.kafka.topic` | yes | — | Target Kafka topic for produced records |

Offsets are tracked per `{bucket, entry}` as `{"timestamp": <µs>}` in Kafka Connect offset storage.

## Examples

Create the topics first (auto-creation may be disabled):

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 `
  --create --if-not-exists --topic sensor-temp --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 `
  --create --if-not-exists --topic sensor-temp-out --partitions 1 --replication-factor 1
```

### Example 1 — write a message to ReductStore (sink)

```powershell
"hello-reduct" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh `
  --bootstrap-server localhost:9092 --topic sensor-temp
```

Read it back from ReductStore:

```powershell
curl.exe "http://localhost:8383/api/v1/b/kafka/sensor-temp/q?start=0&stop=9999999999999999&ttl=5"
# -> {"id":<queryId>}
curl.exe "http://localhost:8383/api/v1/b/kafka/sensor-temp/batch?q=<queryId>"
# -> hello-reduct
```

### Example 2 — round trip (Kafka -> ReductStore -> Kafka)

With both `reduct-sink` and `reduct-source` running (`source` reads entry `sensor-temp` into topic `sensor-temp-out`):

```powershell
"roundtrip-1" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh `
  --bootstrap-server localhost:9092 --topic sensor-temp

docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 --topic sensor-temp-out --from-beginning --timeout-ms 8000
# -> roundtrip-1
```

### Example 3 — read all entries (source)

```json
{
  "name": "reduct-source-all",
  "config": {
    "connector.class": "store.reduct.connector.ReductSourceConnector",
    "tasks.max": "1",
    "reduct.store.url": "http://reductstore:8383",
    "reduct.store.bucket": "kafka",
    "reduct.entries.all": "true",
    "reduct.kafka.topic": "reduct-all-out",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
  }
}
```

## Troubleshooting

- **`ClassNotFoundException: store.reduct...`** — `CONNECT_PLUGIN_PATH` points at the plugin folder itself instead of its parent, or the dependency jars are missing. Point it at the parent directory and keep all jars in the plugin subdirectory.
- **Task fails with `Bucket ... is not found`** — the source connector does not create buckets. Create the bucket first (the sink does it automatically, or create it in ReductStore).
- **New/updated plugin not picked up** — restart the worker (`docker compose restart connect`); plugins are loaded at startup only.
- **Changes to the SDK are ignored** — reinstall it (`mvn install` in `reduct-java`) and rebuild the plugin, then restart the worker.
- **ReductStore URL from inside the worker** — use the service name (`http://reductstore:8383`) when ReductStore runs in the same compose network, or `http://host.docker.internal:8383` if it runs on the host.

# Kafka-learning

# Generate a random UUID

```bash
kafka-storage.sh random-uuid
```

# Format the Kafka storage with the generated UUID and the server properties

```bash
kafka-storage.sh format -t <uuid> -c ~/kafka_2.13-3.9.0/config/kraft/server.properties
```
# Start the Kafka server with the specified server properties

```bash
kafka-server-start.sh ~/kafka_2.13-3.9.0/config/kraft/server.properties
```
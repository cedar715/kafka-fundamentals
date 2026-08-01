# Kafka Fundamentals (Java + Docker)

Hands-on lab for building strong Kafka fundamentals: theory in `lessons/`, runnable Java in `src/`.

## Prerequisites

- Docker
- Java 21+
- Maven 3.9+

## Start Kafka

```bash
docker compose up -d
docker compose ps   # wait until healthy
```

Stop later with `docker compose down`.

## Curriculum

| # | Topic | Status |
|---|--------|--------|
| 1 | The commit log, topics, partitions, keys | **Current** |
| 2 | Consumers, consumer groups, offsets | Next |
| 3 | Producers in depth (acks, retries, idempotence) | |
| 4 | Delivery semantics & exactly-once | |
| 5 | Replication, ISR, failure modes | |
| 6 | Compacted topics & retention | |

## Lesson 1 — quick run

```bash
mvn -q exec:java -Dexec.mainClass=com.learn.kafka.lesson01.CreateOrdersTopic
mvn -q exec:java -Dexec.mainClass=com.learn.kafka.lesson01.SimpleProducer
```

Read the theory: [lessons/01-the-log.md](lessons/01-the-log.md)

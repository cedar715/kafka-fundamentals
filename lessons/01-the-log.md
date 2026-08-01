# Lesson 1 — The commit log, topics, and partitions

## Mental model

Kafka is **not** a traditional message queue. At its core it is a **distributed, append-only commit log**.

```
Topic: orders
┌─────────────────────────────────────────────────────────────┐
│ Partition 0:  [0] [1] [2] [3] [4] ...   ← offsets           │
│ Partition 1:  [0] [1] [2] ...                               │
│ Partition 2:  [0] [1] [2] [3] ...                           │
└─────────────────────────────────────────────────────────────┘
         ↑ append only              ↑ immutable once written
```

- **Append-only**: new records go to the end. You do not update or delete in place.
- **Offset**: a monotonically increasing position *within a partition* (not global across the topic).
- **Immutable history**: consumers can re-read old data by seeking to an earlier offset.

Contrast with classic queues (RabbitMQ / SQS style): once a message is acked, it is typically gone. In Kafka, retention is time- or size-based; consumption does not delete the record.

## Topic

A **topic** is a named category of events (`orders`, `payments`, `clickstream`). Producers write to a topic; consumers subscribe to one or more topics.

A topic is **not** a single queue. It is a collection of partitions.

## Partition

A **partition** is an ordered, immutable sequence of records. Ordering is guaranteed **per partition**, not across the whole topic.

Why partition?
- **Scale writes/reads**: each partition can live on a different broker.
- **Parallel consume**: one consumer in a group can be assigned one or more partitions.
- **Ordering where it matters**: put related events in the same partition (usually via key).

## Keys and partitioning

When you produce with a **key**, Kafka’s default partitioner hashes the key and maps it to a partition:

```
partition = hash(key) % numPartitions
```

Same key → same partition → **order preserved for that key**.

No key → records are distributed (sticky/round-robin style depending on client version). You lose per-entity ordering.

## Replication (preview)

Each partition has a **leader** and zero or more **followers** (replicas). Producers/consumers talk to the leader. Replication factor (RF) is how many copies exist. Our Docker lab uses RF=1 for simplicity — fine for learning, not for production.

## Lab checklist

1. Start Kafka: `docker compose up -d`
2. Create topic: `mvn -q exec:java -Dexec.mainClass=com.learn.kafka.lesson01.CreateOrdersTopic`
3. Produce: `mvn -q exec:java -Dexec.mainClass=com.learn.kafka.lesson01.SimpleProducer`
4. Observe: same `cust-*` key always hits the same partition; offsets increase within that partition.

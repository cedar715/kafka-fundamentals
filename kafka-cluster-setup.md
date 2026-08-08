# 3-Node Kafka Cluster (KRaft, Docker Compose)

Combined mode: all 3 nodes act as both broker and controller. Simplest way to see replication, leader election, and ISR behavior without ZooKeeper.

## 0. Prereqs

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER   # then log out/in, or `newgrp docker`
docker --version
docker compose version
```

## 1. (Optional) Generate your own cluster ID

The compose file ships with a placeholder CLUSTER_ID. Any 3+ nodes sharing one ID form one cluster — fine to reuse the placeholder for learning, or generate your own:

```bash
docker run --rm apache/kafka:latest /opt/kafka/bin/kafka-storage.sh random-uuid
```

Paste the result into `CLUSTER_ID` in `docker-compose.yml` (all 3 brokers inherit it via the shared `x-kafka-env` block, so you only edit it once).

## 2. Start the cluster

```bash
docker compose up -d
docker compose ps
docker compose logs -f kafka1   # watch for "Kafka Server started"
```

Kafka UI (visual view of brokers/topics/partitions): http://localhost:8080

Resource check on your machine (4c/8t, 30GB RAM) — 3 JVM brokers + UI is comfortably within budget; each broker defaults to ~1GB heap.

## 3. Verify the cluster is up

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status
```

Should list all 3 node IDs, one as leader (controller).

## 4. Create a replicated topic

```bash
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic test-topic --partitions 3 --replication-factor 3

docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic test-topic
```

Note which broker is the Leader for each partition, and the Isr (in-sync replica) list.

## 5. Produce / consume

```bash
# producer (from host, using external listener)
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic test-topic

# consumer, separate terminal
docker exec -it kafka2 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

## 6. Test failover (the actual point of a multi-node cluster)

```bash
# find current leader for partition 0 from step 4's describe output, then kill it
docker stop kafka<leader-id>

# re-describe the topic — a new leader should have been elected, Isr shrinks by one
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic test-topic

# producer/consumer above should keep working uninterrupted (replication factor 3,
# min.insync.replicas 2 means it survives one node down)

# bring it back
docker start kafka<leader-id>
# watch it rejoin ISR
```

Caveat with combined mode: killing a node removes both its broker and controller roles at once. If you kill 2 of 3, you lose controller quorum majority and the cluster can't process metadata changes (existing topics keep working, but you can't create/alter topics) — that's expected and itself a useful thing to observe.

## 7. Tear down

```bash
docker compose down          # stop, keep data volumes
docker compose down -v       # stop and wipe all data (fresh cluster next time)
```

## Where to go next

- Kill the controller quorum leader specifically (shown in step 3's describe output) vs a follower — compare recovery time.
- Change `min.insync.replicas` to 3 and watch producers with `acks=all` block when a broker is down.
- Split broker and controller roles onto dedicated nodes (`process.roles=controller` only vs `broker` only) to see a topology closer to a real production cluster — the current combined-mode setup is easier to run but hides some failure modes.

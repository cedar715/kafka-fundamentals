# 6-Node Kafka Cluster (KRaft, Dedicated Controllers + Brokers, Docker Compose)

Dedicated mode: 3 controller-only nodes (metadata/Raft quorum, not exposed to host) + 3 broker-only nodes (serve data, replicate partitions). Closer to a real production topology than combined mode — controller and broker failures are isolated from each other.

## 0. Prereqs

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER   # then log out/in, or `newgrp docker`
docker --version
docker compose version
```

## 1. (Optional) Generate your own cluster ID

The compose file ships with a placeholder CLUSTER_ID. All 6 nodes must share the same ID to form one cluster — fine to reuse the placeholder for learning, or generate your own:

```bash
docker run --rm apache/kafka:latest /opt/kafka/bin/kafka-storage.sh random-uuid
```

Paste the result into `CLUSTER_ID` in the `x-common-env` block in `docker-compose.yml` — both controllers and brokers inherit it from there, so you only edit it once.

## 2. Start the cluster

```bash
docker compose up -d
docker compose ps
docker compose logs -f controller1   # watch for it joining the quorum
docker compose logs -f broker1       # watch for "Kafka Server started"
```

Kafka UI (visual view of brokers/topics/partitions): http://localhost:8080

Resource check on your machine (4c/8t, 30GB RAM, 28GB free): 6 JVM nodes + UI. Controllers are capped at 512MB heap (they only hold the metadata log, no partition data), brokers at 1GB. Total footprint roughly 6-8GB including JVM overhead — comfortable headroom on this box. CPU is the more likely limit under real load, not memory.

## 3. Verify the cluster is up

```bash
docker exec -it controller1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server controller1:9093 describe --status
```

Should list all 3 controller node IDs (1-3), one as the quorum leader.

```bash
docker exec -it broker1 /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

Should list all 3 brokers (node IDs 4-6).

## 4. Create a replicated topic

```bash
docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic test-topic --partitions 3 --replication-factor 3

docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic test-topic
```

Note which broker (4, 5, or 6) is the Leader for each partition, and the Isr (in-sync replica) list.

## 5. Produce / consume

```bash
# producer (from host, using external listener)
docker exec -it broker1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic test-topic

# consumer, separate terminal
docker exec -it broker2 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

## 6. Test failover — two distinct failure modes now

Because roles are split, a broker dying and a controller dying are separate, isolated events. Worth testing both.

**Kill a broker (data-plane failure):**

```bash
# stop the current partition leader from step 4's describe output
docker stop broker<n>

# re-describe — a new leader elected from the remaining ISR
docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic test-topic

# producer/consumer keep working (replication factor 3, min.insync.replicas 2
# survives one broker down); controllers are untouched

docker start broker<n>   # watch it rejoin the ISR
```

**Kill a controller (control-plane failure):**

```bash
# find the quorum leader from step 3, stop a different one (a follower) first
docker stop controller<n>

# quorum still has 2/3 — still has majority, cluster keeps functioning normally
docker exec -it controller1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server controller1:9093 describe --status

# producer/consumer traffic is completely unaffected - brokers don't need
# the controller for steady-state reads/writes, only for metadata changes
# (new topics, partition reassignment, broker registration)

docker start controller<n>
```

Then try stopping 2 of 3 controllers — quorum drops below majority. Existing topic reads/writes on brokers keep working (data plane is independent), but you can't create/alter topics or register a new broker until quorum is restored. This is the behavior combined mode couldn't show you cleanly, since there a node loss always hit both planes together.

## 7. Tear down

```bash
docker compose down          # stop, keep data volumes
docker compose down -v       # stop and wipe all data (fresh cluster next time)
```

## Where to go next

- Compare recovery time: kill the controller quorum *leader* specifically vs a follower.
- Set `min.insync.replicas` to 3 and watch producers with `acks=all` block when a broker is down.
- Simulate a rolling restart (one node at a time, controllers first then brokers) the way you'd patch a real cluster.
- Add a 4th and 5th controller to see how quorum size trades off recovery speed vs write latency (every metadata write needs majority ack).

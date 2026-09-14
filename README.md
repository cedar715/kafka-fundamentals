# Kafka Schema Registry — short tutorial + hands-on lab

## 1. The concept, condensed

Kafka brokers move bytes and don't interpret them. Schema Registry is the
side service that gives those bytes a contract: producers register a
schema (Avro/Protobuf/JSON Schema) under a **subject**, get back a small
integer **schema ID**, and stamp that ID into the first 5 bytes of every
message (1 magic byte + 4-byte ID) instead of shipping the schema itself.
Consumers read the ID, fetch the matching schema (cached after the first
lookup), and decode.

The part that actually matters operationally is **compatibility
enforcement**: when a new schema version is registered against a subject,
the registry checks it against the subject's compatibility mode
(BACKWARD / FORWARD / FULL / NONE) before allowing it. A breaking change
gets rejected at registration time — before it ever reaches a topic — not
discovered later as a wall of consumer deserialization exceptions in prod.

Default subject naming is `<topic>-value` / `<topic>-key`
(TopicNameStrategy), so in a shared cluster your topic-naming convention
*is* your schema isolation boundary — this is the thread connecting back
to the multi-tenancy discussion.

## 2. What you're about to build

Your existing 3-broker KRaft cluster, plus one more container:
Schema Registry, backed by its own internal `_schemas` topic. Then a
producer/consumer pair that proves the two things that actually matter
day to day:

1. A consumer written once keeps working as the producer adds fields (backward compatibility).
2. A schema that breaks the contract gets **rejected at produce time**, not silently corrupted downstream.

## 3. Setup

This `docker-compose.yml` is your existing 6-node cluster (3 dedicated
controllers + 3 brokers + kafka-ui) with one new service appended:
`schema-registry`, pointed at your brokers' internal listener
(`broker1:9092,broker2:9092,broker3:9092`) the same way `kafka-ui`
already is. kafka-ui's config also got a `KAFKA_CLUSTERS_0_SCHEMAREGISTRY`
line added, so once it's up you can browse subjects/versions visually at
`localhost:8080` instead of only via curl.

```bash
cd kafka-schema-registry-lab
docker compose up -d
# or: podman-compose up -d

# give it ~30-45s (SR waits on the brokers being ready), then confirm:
curl -s http://localhost:8081/subjects
# -> [] on a fresh cluster
```

Your cluster has `KAFKA_AUTO_CREATE_TOPICS_ENABLE: false`, so the `users`
topic needs to be created explicitly before the producer can write to it:

```bash
docker exec -it broker1 /opt/kafka/bin/kafka-topics.sh \
  --create --topic users \
  --bootstrap-server broker1:9092 \
  --partitions 3 --replication-factor 3
```

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## 4. Exercise A — register and read (baseline)

```bash
python producer.py schemas/user_v1.avsc
```

Then in another terminal:

```bash
python consumer.py
```

You should see the record printed. Now check what actually landed in the
registry:

```bash
curl -s http://localhost:8081/subjects | jq
curl -s http://localhost:8081/subjects/users-value/versions/1 | jq
curl -s http://localhost:8081/config/users-value   # compatibility mode for this subject (defaults to global BACKWARD)
```

## 5. Exercise B — evolve the schema, backward-compatible

`schemas/user_v2_compatible.avsc` adds `loyaltyTier` with a default value.
Leave the consumer from Exercise A **running**, and in another terminal:

```bash
python producer.py schemas/user_v2_compatible.avsc
```

Watch the still-running v1 consumer — it keeps consuming without a
restart or a code change (the deserializer's Avro resolution rules apply
the reader's schema against the writer's). Then confirm the registry now
has two versions:

```bash
curl -s http://localhost:8081/subjects/users-value/versions | jq
```

## 6. Exercise C — break compatibility on purpose

`schemas/user_v3_incompatible.avsc` removes the `email` field with no
default — under BACKWARD compatibility (the default mode), a schema
without a default for a removed field fails, because old consumers reading
new data would have no way to fill that field in.

```bash
python producer.py schemas/user_v3_incompatible.avsc
```

Expect this to fail loudly at registration/produce time. Read the error —
it names exactly which compatibility check failed. That failure, happening
here instead of in a consumer's exception logs three hops downstream, is
the entire point of the registry.

Optional: flip the subject to `FORWARD` or `NONE` and re-run to see the
same schema get accepted, to build intuition for what each mode actually
buys you:

```bash
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "NONE"}' \
  http://localhost:8081/config/users-value
```

## 7. Comprehension check (answer before moving on)

1. If the consumer in Exercise B had been the one deployed with the *old*
   schema and the producer sends *new*-schema data, which compatibility
   mode is being exercised — BACKWARD or FORWARD? Why does that direction
   matter more for a consumer-heavy fan-out topic than a single-consumer one?
2. Why does the registry embed a 4-byte schema *ID* in the message rather
   than a full schema, and what would break if a consumer lost network
   access to the registry mid-stream?
3. In a multi-tenant cluster using TopicNameStrategy, what specifically
   stops Tenant A from registering a schema that clobbers Tenant B's
   subject? What does RBAC add on top of that?

## 8. Where this connects to the operator role

This is developer-level fluency. Operator-level adds: HA design for the
registry cluster itself (leader election via `_schemas`, what happens on
registry unavailability — producers/consumers with a warm cache keep
working, cold ones don't), retention/compaction settings on `_schemas`,
and RBAC/ACL scoping per subject. Worth queuing as a follow-on once
partitions/replication (next on your plan) is done.

# Apache Kafka Learning Session — Resume Guide

## Purpose of this file

This file records (and plans) an **incremental, hands-on Apache Kafka
learning session focused on two production-level themes: Kafka messaging
done right, and consumer-side resilience with the Resilience4j
CircuitBreaker**.

It exists so the human learner and any AI coding agent can **resume the
conversation later** without losing the teaching style, the project state,
or the progress made. **Read this file first after resuming.**

The two themes of the session:

- **Kafka at production level** — partitions and ordering, delivery
  semantics, offset management, non-blocking retries and a DLT, poison
  messages, consumer lag, security, schema evolution, the transactional
  outbox, and operations.
- **CircuitBreaker at production level** — Resilience4j around the
  consumer's downstream calls, paired with Kafka pause/resume so an OPEN
  breaker sheds load instead of stalling a partition or forcing a rebalance.

Nothing in this session is learning-only or toy-shaped: every design is the
way a shipped, high-load service does it, and whenever a shortcut is **not**
production-safe the failure mode is named explicitly.

---

## Critical instructions for the AI agent (READ FIRST)

1. **YOU ARE A TEACHER, NOT A TYPIST.**
   The learner types or copy-pastes **every** piece of code themselves so
   they understand it thoroughly. You must never write the Java sources,
   the YAML, or the Maven pom for them.
   - Show the code **in the chat** for the learner to copy.
   - Explain *why* each part exists before moving on.
   - The **only** files you write yourself are documentation
     (`.md` files under `docs/`).

2. **Incremental, one step at a time.**
   Never jump ahead. Each step must be small and build on the previous one.
   Before showing a new step, confirm the previous step is complete.

3. **Mandatory verification loop** — after the learner says **"Done"**:
   1. Read the file(s) they created/edited.
   2. Check the content matches what was asked (structure, class names).
   3. Compile it (and run it where possible) from the project root.
   4. Only then advance to the next step, or guide fixes if something is off.

4. **Teach the concepts, not just the syntax.**
   After each step, summarize the mental model (why a partition, why an
   offset, why `acks=all`, how rebalancing works, etc.). Use tables and
   plain-language analogies. The learner values deep understanding.

5. **Debug together.**
   If the learner hits an error, explain how to **read the stack trace**
   (root cause is often buried at the bottom — `Caused by:`), and teach the
   fix instead of silently patching it.

6. **One question/screen of information at a time.**
   Do not overwhelm the learner with the whole roadmap in one burst. Present
   the current step, let them implement, verify, then proceed.

7. **Credentials warning.**
   Never hardcode broker credentials, SASL/SCRAM passwords, or API keys.
   Remind the learner that production uses env vars, IAM/IRSA, Vault, or a
   secrets manager, and that `.yaml` files holding secrets must not be
   committed. Broker the topic when it comes up.

8. **Documentation formatting rule (from .clinerules):**
   Explanation/teaching `.md` files do **not** include code snippets — only
   list class names, method names, property keys, and CLI commands for
   reference.

9. **PRODUCTION-FIRST, ALWAYS.**
   Every design must be the way a real, shipped, high-load service does it —
   never a simplified or learning-only shortcut. When a simple approach is
   **not** production-safe, say so explicitly and explain the failure mode
   (data loss, duplicate processing, rebalance storms, unbounded memory,
   silently skipped messages, etc.).

10. **TWO THEMES ONLY: KAFKA + CIRCUITBREAKER.**
   Keep the curriculum on Kafka (production-level messaging) and the
   Resilience4j CircuitBreaker (consumer-side resilience). Do not drift into
   unrelated queueing services or platforms; every analogy should be a
   Kafka-internal one (log vs offset, partition vs key, poll vs heartbeat).

---

## How to teach THIS learner (proven in session 1 — keep doing exactly this)

This learner learns best with the following approach. Follow it every session.

1. **DOUBLE-CHECK MY CODE — every single time.** When the learner says they
   created or edited something, the agent must:
   1. read the actual file(s) from disk,
   2. check them line by line against what was asked,
   3. compile the project, and (where possible) run it,
   4. then state precisely what is **correct** and what is **wrong** — naming
      the file, the line, and the exact fix — *before* moving on.
   Never take "done" at face value; verify it yourself.

2. **Be precise about mistakes.** e.g. "`application.yaml`: `kafka:` is at the
   top level; it must be indented under `spring:`". Show the corrected snippet
   in chat so the learner can copy it.

3. **Plain, simple language — short sentences, everyday analogies.** Kafka = a
   post office / a notebook; topic = mailbox; partition = lane; offset = line
   number; consumer group = a team of readers; CircuitBreaker = a fuse box.
   Define every term the first time it appears. If the learner says "I did not
   understand", immediately simplify and re-explain with a concrete example.

4. **One thing at a time.** One file, or one command, then stop and wait for the
   learner. Do not dump five files or the whole roadmap in one go.

5. **Explain each line / command in plain words** before the learner runs it.

6. **Debug together; read the FIRST error, not the last symptom.** e.g. a runtime
   `ClassNotFoundException` was caused by a Maven POM error earlier. Explain the
   root cause and teach the fix; never silently patch.

7. **The learner types the code; the agent runs the verification.** The learner
   writes every Java/YAML/pom file (that is the learning). The agent then runs
   the build and the CLI/broker checks to confirm the result.

8. **Record every gotcha live** in the gotchas section, and keep the session
   state table + architecture table current after each step.

9. **Skip theory the learner already knows** (e.g. CircuitBreaker internals) and
   offer to revisit it hands-on at implementation time.

10. **Production-first, always** — real patterns, real failure modes, real
    defaults; say explicitly when a shortcut is not production-safe.

---

## Learning objectives (what "done" looks like)

By the end of this session the learner should be able to, **without help**:

- explain brokers, topics, partitions, offsets, replication, leader/ISR, and retention;
- justify a partition count and a partitioning key, and explain the ordering guarantee it buys;
- write a production-grade producer (idempotent, `acks=all`, compression, batching, bounded timeouts);
- write a production-grade consumer (manual/controlled commits, correct `max.poll.*`, no blocking);
- reason about **at-least-once vs at-most-once vs exactly-once**, and know which one their code actually has;
- implement **non-blocking retries + a dead-letter topic** and know when blocking retries are acceptable;
- handle **poison messages and deserialization errors** so one bad record cannot halt a partition;
- use **consumer lag** as the primary health signal and alert on it;
- secure a cluster (TLS + SASL/SCRAM + ACLs) and keep credentials out of the repo;
- evolve a schema safely, and know when a change is breaking;
- describe how a DB write plus a Kafka publish can be made atomic (**transactional outbox**);
- plan partition counts, retention, compaction, and DR (MirrorMaker 2) for a real service;
- place a Resilience4j CircuitBreaker around a downstream dependency and wire it to Kafka pause/resume so an OPEN breaker sheds load instead of stalling a partition;
- read breaker metrics (state, failure rate, slow-call rate, buffered calls) and decide what to alert on;
- run **many producers and many subscribers at once** from a repeatable load harness, and read what the brokers and the metrics tell you;
- size and compress payloads deliberately, and explain why large records are an anti-pattern (the claim-check pattern instead);
- run the same codebase against local Docker (**dev**) and an AWS cluster (**prod**) using **Spring profiles**, with no code change and no committed secret;
- choose where Kafka runs on AWS (**MSK provisioned vs MSK Serverless vs self-managed on EKS/EC2**), and describe the VPC, auth and sizing around it;
- **scale** a Kafka deployment along its three axes (consumers, partitions, brokers), reassign partitions, and explain what breaks ordering and what does not.

## Environment — decisions to lock in at the start (Step 0)

| Item | Options | Recommendation |
|---|---|---|
| Local broker | Docker Compose, **KRaft mode** (no ZooKeeper); `apache/kafka` image | KRaft — ZooKeeper is removed in Kafka 4.x; **chosen: Kafka 4.2.x → `apache/kafka:4.2.2`** (current supported line, KRaft-only) |
| Realism | 1 broker (dev) vs 3 brokers (prod-like) | start 1 broker, then move to 3 + `replication-factor=3` + `min.insync.replicas=2` before the reliability phase |
| Managed option | Confluent Cloud, AWS MSK, Redpanda | AWS MSK chosen as the production home to learn (Step 29); local stays Docker |
| AWS target | MSK (provisioned) vs MSK Serverless vs self-managed (EC2/EKS + Strimzi) | decide in Step 29; MSK provisioned is the default teaching target |
| Config per environment | Spring profiles (`dev`, `prod`) | one codebase; `application-dev.yaml` / `application-prod.yaml`; secrets via env vars only (Step 28) |
| CLI tools | `kafka-topics.sh`, `kafka-console-producer.sh`, `kafka-console-consumer.sh`, `kafka-consumer-groups.sh`, `kcat` | include `kafka-consumer-groups.sh` early — lag checking is a daily habit |
| UI | AKHQ, Redpanda Console, Conduktor | optional but great for seeing partitions/offsets |
| Schema Registry | Confluent Schema Registry, Apicurio | introduce in the schema phase, not at the start |
| Project shape | one project vs **two projects** (`producer` + `consumer`) | two projects — separate producer and consumer apps, deployed, versioned and scaled independently |
| Spring dependency | `spring-kafka` (version managed by the Boot parent) + `spring-kafka-test` | verify the resolved version, do not guess |
| Build | Maven, Java 21, Spring Boot | fixed toolchain; `spring-kafka` version managed by the Boot parent, verified not guessed |
| Resilience | `resilience4j-spring-boot3` + `spring-boot-starter-aop` | CircuitBreaker (and its Actuator metrics) around the consumer's downstream calls, wired to Kafka pause/resume |

**Toolchain resolved on this machine (verified):** JDK **21.0.10 (LTS)**
lives at `C:\Program Files\Java\jdk-21.0.10` (also reachable via the
`C:\Program Files\Java\latest\jdk-21` junction), so the "Java 21" target below
is real. However, the default `java` / `javac` on PATH is **17.0.18
(Microsoft)**, `JAVA_HOME` is unset, and `mvn -v` therefore runs on **17.0.18**.
Point `JAVA_HOME` at the JDK 21 directory (or use a Maven toolchain) when
building, otherwise a pom that requests `java.version` / `maven.compiler.release`
21 will fail — a 17 JDK cannot emit 21 bytecode. Setting the compiler level
alone is not enough.

## How to run the session (the loop)

1. The agent reads this file first.
2. The agent confirms the current step from the roadmap below.
3. The loop: **teach concept → show code in chat → learner implements →
   learner says "Done" → agent reads + compiles + verifies → next step.**
4. After each phase, the agent updates this file: tick the step, record
   gotchas discovered, and note the next step. Only `.md` files are written
   by the agent.

---

## The one idea to teach before any code

> **Kafka is a log, not a queue.** A queue is drained and a message
> disappears once acknowledged; a log is appended to and every reader moves
> its own cursor at its own pace. Everything else in Kafka — partitions,
> offsets, consumer groups, retention, replay — follows from this one
> sentence.

| Concept | Meaning |
|---|---|
| Broker | one Kafka server; a cluster is several |
| Topic | a named, append-only stream of records |
| Partition | an ordered, append-only slice of a topic; **the unit of parallelism and ordering** |
| Offset | a record's position in a partition; the consumer's bookmark, per partition, never reused |
| Consumer group | a set of consumers that split the partitions between them; one partition is owned by exactly one consumer in the group |
| Commit | recording "I have processed up to offset N"; the record itself is **not** removed |
| Retention | records survive for a configured time/size regardless of consumption |
| Rebalance | reassigning partitions when group membership changes |
| Replication factor / ISR | copies of a partition on other brokers; the in-sync set |
| Log compaction | keep only the latest value per key |

**The properties that make a log different, and that the whole session
depends on:**

1. **Replay.** Records are not deleted when read. You can reset offsets and
   reprocess history — which is exactly why reprocessing must be idempotent.
2. **Fan-out.** Several consumer groups each receive **every** record, each
   with independent progress, at no extra cost.
3. **Ordering is per partition**, and the producer key chooses the partition —
   so the key is the ordering contract.

## Production concepts that MUST appear in the curriculum

These are the things that separate a demo from a shipped service. Each one is
tied to a roadmap phase below so nothing is missed.

| Concept | Why it matters in production |
|---|---|
| Partition count & key design | sets max consumer parallelism, ordering unit, and future re-keying cost (partitions can only be **increased**, which breaks key→partition stability) |
| `acks=all` + `min.insync.replicas` | durability; `acks=1` silently loses data on leader failure |
| Idempotent producer | retries must not create broker-side duplicates |
| Manual/controlled offset commit | auto-commit or commit-before-processing = **silent data loss** |
| `max.poll.interval.ms` vs real processing time | too-low = endless rebalances and duplicate work |
| Non-blocking retries (`@RetryableTopic`) | blocking retries stall the partition and blow up heartbeat/poll deadlines |
| Dead-letter topic + replay tooling | on-call must be able to inspect **and** re-drive |
| `ErrorHandlingDeserializer` | a malformed record otherwise kills the consumer and halts the partition forever |
| Consumer lag monitoring + alerting | the single most useful health metric |
| Cooperative rebalancing + static membership | avoids stop-the-world rebalances on every deploy |
| Schema evolution + compatibility mode | producers and consumers deploy independently |
| Transactional outbox | Kafka transactions **cannot** cover a database write |
| TLS + SASL/SCRAM + ACLs | security baseline; per-service credentials |
| Retention / compaction / tiered storage | cost control and replayability |
| Idempotent consumer (dedupe key) | at-least-once delivery means **you will** see duplicates |
| CircuitBreaker + pause/resume | an OPEN breaker that keeps polling spins on `CallNotPermittedException` and blows `max.poll.interval.ms`; the breaker must shed load by **pausing the container**, not by failing records |
| Payload size + compression | large records burn page cache, widen fetch/poll windows, and hit a broker/consumer limit mismatch; the claim-check pattern keeps records small |
| Load harness (N producers, M subscribers) | only real concurrency reveals the partition ceiling, hot keys, rebalance churn and lag growth before production does |

## Real-world use cases (what a shipped service uses Kafka for)

These are the production patterns the roadmap builds toward. Each names the
failure mode it must survive, so the step that teaches it is obvious.

| Use case | What it looks like in production | Watch out for | Roadmap |
|---|---|---|---|
| **Service decoupling / event-driven microservices** | an order service publishes `OrderPlaced`; payment, inventory and notification each consume it in their own group | a shared `group.id` across unrelated services turns them into competing consumers; events need a schema contract | Steps 2, 8, 18 |
| **Fan-out notifications** | one event drives email + push + audit + analytics, each an independent group with its own offsets | every group must be monitored for lag; a slow group does not slow the others (that is the point) | Steps 5, 16 |
| **Stream processing / real-time analytics** | clickstream, metrics and fraud signals processed with Kafka Streams or Flink; windowed aggregations | reprocessing must be idempotent; state stores need sizing | Step 19 |
| **CDC / data integration (ETL/ELT)** | Debezium captures database changes into Kafka; sinks feed a warehouse or data lake | ordering per key; exactly-once across systems needs the outbox | Step 22 |
| **Event sourcing / current-state tables** | a compacted topic holds the latest value per key, replayable as the source of truth | tombstones and `delete.retention.ms`; compaction is not a database | Step 19 |
| **Task/command distribution** | work spread across a consumer group (competing consumers) up to the partition count | Kafka is a log, not a queue: per-key ordering, no per-message ack, extra consumers idle | Steps 9, 25 |
| **Reliable publish after a DB write** | the transactional outbox: write the event row in the same DB transaction, a relay publishes it | Kafka transactions do not cover the DB write | Step 22 |
| **Replay / backfill** | reset offsets to rebuild a cache or backfill a new consumer with history | a replay is a deployment: it must be idempotent | Steps 19, 31 |
| **Resilience: poison messages, skip, DLQ** | a bad record must not stall its partition; it is retried, then skipped or parked in a DLT | silent skips lose data; an unmonitored DLT is a quiet data-loss bin | Steps 13, 14, 23 |

### Use-case spotlight — poison message, skip, and DLQ

- **The problem.** A record that can never be processed (malformed JSON, an
  unknown event type, a downstream 400) blocks **its whole partition**: every
  later record waits behind it and lag climbs. Kafka has no built-in redrive —
  the escape hatch is yours to design.
- **Retry first, and classify the error.** A transient failure (timeout,
  downstream 5xx) goes to **non-blocking retry topics** (`@RetryableTopic`) so
  the partition keeps flowing. A permanent failure (bad payload, business rule
  violation) must not burn the retry budget — it goes straight to the DLT.
- **Skip — but never silently.** The production answer to "skip a poison
  message" is to move past it **while preserving it**: emit a metric and a log
  line, and park the original in a **dead-letter topic**. A silent skip is data
  loss with no audit trail; a skip that also writes to the DLT is an auditable
  decision.
- **The DLT is a subsystem, not a `try/catch`.** A dedicated `<topic>-dlt`
  with its own retention, the original payload plus headers (original
  topic/partition/offset, exception class and message, first-failure
  timestamp), a depth alert, and **idempotent replay tooling**. On-call triages
  from the DLT, fixes, and re-drives.
- **Decision table:**

| Situation | Action | Why |
|---|---|---|
| Transient error (timeout, 5xx) | non-blocking retry with backoff, then DLT | do not stall the partition; let the dependency recover |
| Permanent error (bad payload, 400) | straight to DLT, skip retries | retrying cannot succeed; it only delays and burns budget |
| Deserialization failure | `ErrorHandlingDeserializer` → DLT | otherwise the record kills the consumer before your code runs |
| Downstream is *down* (not slow) | CircuitBreaker OPEN + pause the container | shed load and stop polling instead of spinning or rebalancing (Step 15) |
| Replayed DLT record | idempotent handler (Step 31) | a replay re-delivers; the side effect must happen once |

---
# Roadmap (phases and steps)

Legend: `[ ]` not started · `[~]` in progress · `[x]` done and verified.
The agent must only move to the next step after the verification loop passes.

## Phase A — Foundations (concepts before code)

- [x] **Step 0 — Environment: broker up + CLI smoke test**
      Goal: a running broker and confidence with the CLI before writing Java.
      Teach: KRaft vs ZooKeeper; `bootstrap.servers`; where topic metadata lives;
      what a broker id/listener is; Docker Compose for a local broker.
      Do: `kafka-topics.sh --create/--list/--describe`, produce and consume a
      few records with `kafka-console-producer.sh` / `kafka-console-consumer.sh`.
      Exit criteria: learner can create and describe a topic and can explain
      what `--partitions` and `--replication-factor` mean in the output.

- [x] **Step 1 — The log model (no code)**
      Goal: the mental model that makes Kafka make sense.
      Teach: append-only log; partitions as the ordering + parallelism unit;
      offsets are per partition and never reused; a record is NOT removed when
      read; leader/follower/ISR; retention is time- or size-based; a consumer
      group is a set of readers sharing partitions.
      Exit criteria: learner can answer "why can two consumers in the same
      group never read the same partition?" and "what happens to offsets when
      partitions are increased?" (partitions can only be **added**, and the
      key-to-partition mapping changes, so ordering for existing keys breaks).

- [x] **Step 2 — Topic provisioning the production way**
      Goal: topics exist because code or IaC says so, never by accident.
      Teach: `auto.create.topics.enable` is an anti-pattern (a typo silently
      creates a topic with the wrong partition count); `KafkaAdmin` + `NewTopic`
      beans; when to move provisioning to Terraform/CI; naming conventions
      (`env.domain.entity.vN`); defaults to set on every topic
      (`retention.ms`, `cleanup.policy`, `min.insync.replicas`).
      Exit criteria: learner can justify a partition count from an expected
      throughput and a consumer-parallelism target.

- [x] **Step 3 — CircuitBreaker foundations (Resilience4j, no Kafka yet)** *(theory skipped — learner knows breakers; hands-on revisited at Step 15)*
      Goal: lock in exactly what a CircuitBreaker protects, before wiring it
      into a Kafka consumer.
      Teach: the breaker states (CLOSED → OPEN → HALF_OPEN) and what each
      transition means; sliding window (COUNT_BASED vs TIME_BASED) and
      minimumNumberOfCalls; failureRateThreshold / slowCallRateThreshold and
      slowCallDurationThreshold; waitDurationInOpenState and
      permittedNumberOfCallsInHalfOpenState; ignoredExceptions vs
      recordExceptions, so a permanent business error does not trip the
      breaker; `executeRunnable` / `executeSupplier` and how
      `CallNotPermittedException` surfaces when the breaker is OPEN; fallback
      and what a fallback may and may not do; why the breaker must wrap only
      the *remote* call, never the whole listener.
      Exit criteria: learner can name the breaker's state from its metrics and
      explain why `CallNotPermittedException` is a **shed** signal, not a bug.

## Phase B — First end-to-end (producer and consumer in Spring Boot)

- [x] **Step 4 — Producer skeleton + `KafkaTemplate`**
      Goal: send a record from Java and see it on the broker.
      Teach: `spring.kafka.bootstrap-servers`; `KafkaTemplate`; key vs value;
      serializers (`StringSerializer` first, on purpose); how the key decides
      the partition; what `send(...)` returns (`CompletableFuture` /
      `SendResult` carrying partition + offset) and why the result **must** be
      handled (fire-and-forget loses failures silently).
      Exit criteria: `kafka-console-consumer.sh` shows the records, and the
      learner can point at the partition and offset the app reported.

- [x] **Step 5 — Consumer skeleton + `@KafkaListener`**
      Goal: read records in a group and understand commit behaviour.
      Teach: `group.id`; `@KafkaListener(topics, groupId)`; the listener
      container; `auto.offset.reset` (the client default is **latest**, so a new
      group silently skips existing records — a classic production surprise);
      commit modes, and that Spring Kafka's container manages commits itself
      (batching by default) instead of relying on `enable.auto.commit`.
      Exit criteria: learner can explain what happens to the first message
      after a restart, and why `group.id` must be unique per consumer role.

- [ ] **Step 6 — JSON payloads, headers, and the mistakes to avoid**
      Goal: replace raw strings with a typed DTO.
      Teach: `JsonSerializer` / `JsonDeserializer`; the `trusted.packages`
      setting and why `*` is risky; why deserialization failures must be
      handled (see Step 14); headers for trace/version propagation; keeping the
      DTO and the partition key consistent.
      Exit criteria: the consumer logs a deserialized DTO and reads a header.

- [ ] **Step 7 — End-to-end verification + reading offsets**
      Goal: prove the flow and learn to *see* it.
      Teach: `kafka-consumer-groups.sh --describe`, and how to read
      `CURRENT-OFFSET` vs `LOG-END-OFFSET` and `LAG`; what lag means while a
      consumer is stopped.
      Exit criteria: learner deliberately stops the consumer, sends records,
      observes rising LAG, restarts, and watches it drain to zero.

## Phase C — Ordering, partitioning and scaling

- [ ] **Step 8 — Keys and partitioning (the ordering contract)**
      Goal: understand that **the key is the ordering unit**.
      Teach: order is guaranteed **only within a partition**; a key always maps
      to the same partition (so all events for one entity stay ordered); `null`
      keys spread records (sticky partitioning) with no ordering; the default
      `murmur2` hash and how to override it with a custom partitioner;
      **hot partitions** (one key carrying 10x traffic) and how to composite-key
      around it; partitions can only be **increased**, and that silently changes
      the mapping for existing keys.
      Exit criteria: learner can choose a key for a real entity, state the
      ordering guarantee it gives, and predict the partition for a sample key.

- [ ] **Step 9 — Consumer groups and rebalancing**
      Goal: understand group scaling and why rebalances hurt.
      Teach: partitions in a group are split across consumers, so **max useful
      consumers = partition count** (extra consumers sit idle); eager
      (stop-the-world) vs cooperative (incremental) assignment, and why
      `CooperativeStickyAssignor` is the production default;
      `session.timeout.ms` vs `heartbeat.interval.ms` vs `max.poll.interval.ms`
      (the last is the real killer — exceed it and the broker evicts the
      consumer mid-processing, causing duplicates); static membership via
      `group.instance.id` to avoid rebalances on every rolling restart;
      `ConsumerRebalanceListener` and committing before partitions are revoked
      (`onPartitionsRevoked`).
      Exit criteria: learner can explain why a 10-second processing pause is
      safer than a 6-minute one, and how to add consumers without a storm.

- [ ] **Step 10 — Throughput and latency tuning**
      Goal: make throughput a deliberate choice, not an accident.
      Teach: `concurrency` on the listener container (keep it at or below the
      partition count); `max.poll.records` and its direct effect on
      `max.poll.interval.ms` risk; producer `batch.size`, `linger.ms`,
      `compression.type` (`lz4`/`zstd`), `max.in.flight.requests.per.connection`
      and how it interacts with ordering and idempotence; `delivery.timeout.ms`
      vs `retries` vs `request.timeout.ms` (which one actually bounds the send);
      consumer `fetch.min.bytes` / `max.partition.fetch.bytes`; the
      latency-vs-throughput tradeoff of `linger.ms`.
      Exit criteria: learner can justify every non-default value they set, and
      knows which values must **not** be changed casually (e.g. disabling
      idempotence to "fix" ordering).

## Phase D — Reliability: the production core

- [ ] **Step 11 — Delivery semantics: at-least-once, at-most-once, exactly-once**
      Goal: know which guarantee the code actually has.
      Teach: producer durability (`acks=all` + `min.insync.replicas=2` +
      `unclean.leader.election.enable=false`) and why `acks=1` is data loss
      waiting to happen; the idempotent producer (on by default in modern Kafka)
      and what it does and does **not** prevent; at-most-once = commit before
      processing (loss); at-least-once = process then commit (duplicates);
      exactly-once via transactions + `isolation.level=read_committed`;
      `enable.idempotence` vs full EOS.
      Exit criteria: learner can state, for their consumer, exactly which
      guarantee holds and why duplicates are still possible.

- [ ] **Step 12 — Offset management done right**
      Goal: never lose progress or silently reorder it.
      Teach: Spring Kafka `AckMode` values and the container's default batching
      behaviour; when batch commits are acceptable and when they are not;
      `MANUAL` vs `MANUAL_IMMEDIATE` and committing **after** the side effect;
      why committing mid-loop inside a batch creates partial-progress
      ambiguity; committing on shutdown; the interaction with retries (a retry
      must not advance the offset).
      Exit criteria: learner deliberately crashes between processing and
      commit, observes the duplicate on restart, and explains why it happened.

- [ ] **Step 13 — Error handling: blocking vs non-blocking retries**
      Goal: retry without stalling the partition.
      Teach: `DefaultErrorHandler` + `ExponentialBackOffWithMaxRetries` and its
      blocking nature (the container sleeps in-thread, so the partition stalls
      and `max.poll.interval.ms` becomes a real risk); `@RetryableTopic`
      (non-blocking: retry topics with delays, then a DLT) and when to prefer
      it; `DeadLetterPublishingRecoverer` and DLT partition selection; maximum
      retry duration; classifying retryable vs permanent exceptions so a
      permanent error goes straight to the DLT instead of burning the budget.
      Exit criteria: learner demonstrates a record that fails twice, is retried
      without blocking, and lands in the DLT with headers showing the cause.

- [ ] **Step 14 — Poison messages and deserialization safety**
      Goal: one bad record must never halt a partition.
      Teach: `ErrorHandlingDeserializer` wrapping the delegate and surfacing
      `DeserializationException` to the error handler; why a deserialization
      failure would otherwise kill the consumer **before** your code runs; DLT
      for un-deserializable payloads; why the poison record and the DLT are
      **designed, named, retained and monitored**, never automatic;
      inspecting and re-driving a DLT safely (replay tooling, not manual
      pasting).
      Exit criteria: learner publishes a deliberately malformed record, the
      consumer keeps running, and the bad record is parked in the DLT.

- [ ] **Step 15 — Consumer-side resilience (CircuitBreaker + backpressure)**
      Goal: connect the Resilience4j breaker from Step 3 to the consumer loop,
      safely.
      Teach: a rejected or delayed record blocks **its partition**, so a
      CircuitBreaker must be paired with pause/resume
      (`MessageListenerContainer.pause()` / `resume()` via
      `KafkaListenerEndpointRegistry`), otherwise an OPEN breaker makes the
      container spin on `CallNotPermittedException` and blows
      `max.poll.interval.ms`; why the breaker wraps only the downstream call
      while the pause/resume decision sits in the listener; pause instead of
      fail when a downstream is *down* (a downstream outage means pause, do not
      spin); how to resume once the breaker closes (half-open probe or a
      scheduled health check); why the breaker plus non-blocking retry topics
      are a deliberate pairing — the breaker sheds load, the retry topics
      absorb transient faults — not redundancy; the interaction with
      `max.poll.interval.ms` and how a paused container still heartbeats.
      Exit criteria: learner can describe how they would shed load
      (breaker + pause) without triggering a rebalance, and which metrics
      (`state`, `failureRate`, `bufferedCalls`) they would alert on.

## Phase E — Production hardening

- [ ] **Step 16 — Observability: lag is the metric that matters**
      Goal: know at a glance whether the service is healthy.
      Teach: **consumer lag** (per partition) as the #1 signal, and why a
      constantly-rising lag is the first symptom of almost every incident;
      `kafka-consumer-groups.sh --describe` for a manual check; Micrometer
      `KafkaClientMetrics` (binds client JMX metrics) and Actuator exposure;
      broker-side metrics (under-replicated partitions, ISR shrink, request
      latency, disk usage); end-to-end latency vs throughput; alerting
      thresholds (lag growth rate beats absolute lag); distributed tracing
      (correlating a produce span to the consuming span via headers).
      Exit criteria: learner can produce a lag graph, and explain which alert
      they would page on and why.

- [ ] **Step 17 — Security: TLS, SASL/SCRAM, ACLs, credential hygiene**
      Goal: a cluster that is not wide open.
      Teach: `security.protocol=SASL_SSL`; SASL mechanisms (PLAIN vs SCRAM) and
      why SCRAM is preferred; TLS truststore/keystore properties and common
      handshake errors; ACLs (`kafka-acls.sh`) and least-privilege per service
      (`Write` on a topic, `Read`+`GroupRead` on a group); separate credentials
      per service via env vars/secrets manager, never in the repo;
      `application-local.yaml`-style gitignored credentials, while the
      committed `application.yaml` holds only non-secret placeholders.
      Exit criteria: learner can list the exact minimum ACLs their producer and
      consumer need.

- [ ] **Step 18 — Schema management and evolution**
      Goal: producers and consumers must deploy independently without breaking.
      Teach: why JSON without a contract breaks silently; Schema Registry
      (Avro/Protobuf/JSON Schema) and subject naming; compatibility modes
      (`BACKWARD` default, `FORWARD`, `FULL`, transitive variants) and what each
      one permits; which changes are breaking (rename, type change, removing a
      required field) vs safe (adding an optional field with a default);
      schema ids in the message; when a new topic/version is cleaner than an
      in-place change.
      Exit criteria: learner classifies a list of schema changes as
      safe/breaking and picks the matching compatibility mode.

- [ ] **Step 19 — Retention, compaction and replay**
      Goal: control cost, and use replay as a superpower.
      Teach: `retention.ms` / `retention.bytes` and the disk-cost math;
      `cleanup.policy=delete` vs `compact`; log compaction semantics (latest
      value per key is kept) and tombstones (a `null` value marks a delete,
      cleaned after `delete.retention.ms`); compacted topics as a "table" of
      current state (event sourcing / changelog); **resetting offsets** to
      replay history (`kafka-consumer-groups.sh --reset-offsets`) and the
      operational discipline required (a replay is a code deployment — it must
      be idempotent); tiered storage as the cost answer for long retention.
      Exit criteria: learner replays a topic into a fresh consumer group and
      proves idempotency by processing the same records twice.

- [ ] **Step 20 — Operations and failure drills**
      Goal: know what breaks and what the system does about it.
      Teach: broker failure and leader election; ISR shrink and why
      `min.insync.replicas=2` with `acks=all` makes producers fail **instead of**
      losing data (that is the correct trade); `unclean.leader.election`;
      `NotLeaderForPartitionException` / retriable exceptions and how the client
      recovers; `ProducerFencedException` and zombie producers; under-replicated
      partitions as an alert; rolling restarts without rebalance storms (static
      membership + cooperative assignment); partition rebalancing tooling;
      cross-cluster replication with MirrorMaker 2 for DR (and what it does
      **not** replicate, e.g. offsets by default); capacity planning
      (partitions per broker, disk, network).
      Exit criteria: learner can describe what happens to their service when one
      broker of three dies, both while it is merely slow and while it is gone.

- [ ] **Step 21 — Testing Kafka code**
      Goal: tests that do not need a real cluster, plus tests that do.
      Teach: unit-testing the listener/service logic without Kafka; `@EmbeddedKafka`
      (from `spring-kafka-test`) for fast integration tests;
      Testcontainers Kafka for tests that must match a real broker; asserting
      on `KafkaTemplate` sends with a mocked producer; testing error paths
      (retry + DLT) rather than only the happy path; running the suite in CI.
      Exit criteria: a green integration test that sends a record, asserts the
      consumer processed it, and asserts a bad record went to the DLT.

## Phase F — Architectural patterns and a production review checklist

- [ ] **Step 22 — Exactly-once across systems: the transactional outbox**
      Goal: the single most misunderstood guarantee in Kafka.
      Teach: Kafka transactions make *Kafka-to-Kafka* work atomic — they do
      **not** cover a database write or an HTTP call; therefore "write to DB
      then publish to Kafka" is at-least-once at best, and a crash in between
      loses or duplicates the event; the fix is the **transactional outbox**
      (write the event row in the same DB transaction, then a relay/CDC
      publishes it), or CDC with Debezium; the alternative of publishing first
      and updating the DB afterwards and why it also fails; consumer-side
      idempotency (dedupe key, natural key upsert) as the practical safety net;
      `KafkaTransactionManager` + `@Transactional` for the produce side, and
      `read_committed` on the consume side.
      Exit criteria: learner can explain, with a crash timeline, why the outbox
      pattern is required and what each alternative loses.

- [ ] **Step 23 — Retry and DLT topology as a designed subsystem**
      Goal: treat failure handling as architecture, not a try/catch.
      Teach: naming and retention of retry topics and the DLT (`<topic>-dlt`);
      partitioning of the DLT; what headers to attach (original topic,
      partition, offset, exception class/message, stack trace, first-failure
      timestamp) so on-call can triage without the log store; **replay** of a
      DLT (a purpose-built consumer/tool, idempotent); alerting on DLT depth;
      a runbook: what on-call does at 3am when the DLT is growing; retention on
      retry topics to prevent unbounded growth; avoiding infinite retry loops.
      Exit criteria: learner writes the runbook, then triggers a DLT record and
      follows it end to end.

- [ ] **Step 24 — Antipatterns and the production review checklist**
      Goal: recognize the mistakes before shipping them.
      Antipatterns to name explicitly: auto-commit (or commit-before-process)
      losing data; blocking forever inside a listener; unbounded retries;
      disabling idempotence to "fix" ordering; `auto.offset.reset=latest` on a
      new group silently skipping history; one partition for a hot topic;
      increasing partitions on a keyed topic and breaking ordering; storing
      large payloads in a record; using Kafka as a database or a request/reply
      transport; sharing a `group.id` across unrelated services (accidental
      competing consumption); `trusted.packages=*`; secrets in `application.yaml`;
      no lag alerting; a DLT nobody monitors.
      Checklist for any Kafka service: partitions/key justified · `acks=all` +
      `min.insync.replicas>=2` · idempotent producer · commit-after-process ·
      `max.poll.interval.ms` budgeted against worst-case processing ·
      deserialization errors handled · retries non-blocking · DLT + alert +
      runbook · lag dashboard · idempotent consumer · schema compatibility mode
      set · TLS/SASL/ACLs · retention sized against disk · topics provisioned by
      code or IaC, not by accident · tests cover the failure paths.
      Exit criteria: learner reviews their own project against the checklist
      and reports the gaps.

## Phase G — Load simulation: many producers, many subscribers, large messages

- [ ] **Step 25 — The load harness: N producers and M subscribers**
      Goal: a repeatable way to put real, concurrent load on the topics — not
      one lonely console producer.
      Teach: the two levels of "many" — (a) **many producer instances** (the
      producer app started several times, or a dedicated load profile) versus
      (b) **many clients inside one JVM** (`concurrency`, extra
      `KafkaTemplate`s); why a load generator must be **external processes**,
      not threads faking a service, so the JVM, the poll loop and the network
      behave like production; the built-in, production-grade tools Kafka
      already ships — `kafka-producer-perf-test.sh` and
      `kafka-consumer-perf-test.sh` (record count, `--record-size`,
      `--throughput`, `--num-threads`) for raw throughput, and the real Spring
      apps for end-to-end semantics; running **several subscriber processes in
      one group** (partitions split, the extras sit idle) versus **several
      groups** (true fan-out: every group gets every record); a Spring profile
      (`--spring.profiles.active=load`) so the harness is not wired into the
      normal app; driving a fixed duration over a fixed key space so runs are
      comparable.
      Exit criteria: learner starts 3 producer instances and 3 subscriber
      instances, shows the partitions splitting across the subscribers, shows a
      4th subscriber gaining nothing, and shows a second group receiving every
      record.

- [ ] **Step 26 — Simulating large messages, safely**
      Goal: understand the payload-size limits and the cost of big records.
      Teach: the three size limits that must agree — producer
      `max.request.size`, broker/topic `message.max.bytes` (and the per-topic
      override), and consumer `fetch.max.bytes` / `max.partition.fetch.bytes` —
      and the confusing error you get when only one of them is raised;
      `compression.type` (`none` / `lz4` / `zstd` / `snappy` / `gzip`) applied
      per batch, and why `lz4`/`zstd` are the production defaults; how
      compression interacts with `batch.size` / `linger.ms` (bigger batches
      compress better, trading latency for throughput); why large records are
      an anti-pattern — they burn page cache, force bigger broker buffers,
      lengthen fetch and `max.poll` time, and can starve small records sharing
      the partition; the **claim-check pattern** (put the blob in object
      storage and publish a small record carrying its reference) as the
      production answer; when a large record is genuinely unavoidable and how
      to size the limits for it.
      Exit criteria: learner sweeps `--record-size` (e.g. 1 KB → 1 MB → 10 MB),
      records the throughput/latency effect, deliberately triggers the size-limit
      error, then fixes it by aligning all three settings — and can state when
      they would use a claim check instead.

- [ ] **Step 27 — Reading the system under load**
      Goal: turn raw load into evidence about the design.
      Teach: observing per-partition **lag** while the harness runs
      (`kafka-consumer-groups.sh --describe` plus the Step 16 metrics) and the
      difference between a healthy saw-tooth and a monotonic climb; deliberately
      creating a **hot partition** with a skewed key space and showing that no
      amount of consumer `concurrency` fixes it; watching a **rebalance** when a
      subscriber is added or killed mid-load and the duplicate work it causes;
      saturation symptoms — rising produce latency, `retries`,
      `RecordTooLargeException`, `TimeoutException`, ISR shrink; the disk cost of
      the run (`retention.ms` × bytes written) per broker; and recording a short
      baseline table (producers · subscribers · record size · compression ·
      partitions · throughput · p95 latency · peak lag) that later steps must
      not regress.
      Exit criteria: learner produces a one-page baseline of the harness run and
      can point at the first metric that degrades when the load doubles.

## Phase H — Production home (AWS), scaling, and environment configuration

- [ ] **Step 28 — Spring profiles: one codebase, dev and prod**
      Goal: the same application runs locally and in production with no code
      change, only configuration.
      Teach: Spring profiles (`spring.profiles.active`); the shared
      `application.yaml` plus per-profile `application-dev.yaml` /
      `application-prod.yaml` (and profile-specific document blocks in one
      file); overriding `spring.kafka.bootstrap-servers`, `security.protocol`
      and the SASL/TLS properties per profile; `@Profile` on beans that must
      differ (the load harness, local-only topic provisioning);
      `@ConfigurationProperties` for typed, validated config;
      externalised-config precedence (env vars > profile file > default) so a
      secret is injected at deploy time and never committed; activating the
      profile via `SPRING_PROFILES_ACTIVE` or `--spring.profiles.active`; why
      dev and prod differ in security and replication yet share one code path.
      Exit criteria: learner runs the app under `dev` against local Docker and
      under `prod` pointed at the AWS cluster, with no code change and no
      secret in the repo.

- [ ] **Step 29 — Deploying Kafka on AWS**
      Goal: know where production Kafka actually lives on AWS and how the app
      reaches it.
      Teach: the three AWS homes — **Amazon MSK (provisioned)** (brokers you
      size, AWS-managed), **MSK Serverless** (capacity managed, per-partition
      throughput) and **self-managed on EC2/EKS** (Strimzi operator; you own
      patching, disk and upgrades); VPC/subnet/AZ layout and why replicas sit
      in different AZs; security groups and the client's bootstrap endpoints;
      auth choices — IAM (MSK) vs SASL/SCRAM vs mTLS — mapped to
      `security.protocol`; TLS in transit and KMS at rest; how an app in
      EKS/ECS/EC2 reaches a private cluster (private subnets, VPC endpoints, no
      public brokers); sizing (broker instance type, EBS gp3, `log.dirs`);
      monitoring through CloudWatch + MSK metrics and the same lag metric as
      Step 16; cost drivers (broker-hours, storage, cross-AZ transfer); why,
      with profiles done right, `bootstrap.servers` is the only thing that
      changes between local and AWS; credential hygiene via IAM roles/IRSA or
      Secrets Manager, never in `application.yaml`.
      Exit criteria: learner draws the VPC/AZ layout, names the exact
      `bootstrap.servers`, auth mechanism and ACLs the app needs, and justifies
      which of the three homes they would choose.

- [ ] **Step 30 — Scaling in production (simulated)**
      Goal: see, with real numbers, how Kafka scales and where it does not.
      Teach: the three independent scaling axes — **consumers** (add instances
      up to the partition count; extras idle — Step 9), **partitions**
      (`kafka-topics.sh --alter --partitions`; increase-only, and the
      key→partition remapping that breaks ordering — Step 8) and **brokers**
      (add a broker, then `kafka-reassign-partitions.sh` to move partitions and
      balance load; Cruise Control for continuous rebalancing and
      self-healing); why adding partitions is cheap but re-keying is a
      migration; replica placement, rack awareness and `min.insync.replicas`
      during a scale-out; how a scale-out triggers rebalances and what
      cooperative assignment + static membership do to soften it; the per-broker
      ceiling (disk, network, partitions per broker, file descriptors);
      simulating it locally by growing the Compose setup from 1 broker to 3,
      raising `replication-factor` and re-creating a topic with more partitions,
      then watching partitions and lag redistribute via
      `kafka-consumer-groups.sh --describe`; mapping each operation to MSK
      (broker-count changes, storage autoscaling, partition changes).
      Exit criteria: learner takes a 1-broker/3-partition topic to 3
      brokers/6 partitions under load, shows partitions and consumers
      rebalancing, and explains what broke ordering and what did not.

## Phase I — Consumer idempotency (exactly-once processing)

- [ ] **Step 31 — Idempotent consumer: surviving duplicate delivery**
      Goal: a record delivered more than once must not be applied twice.
      Teach: why duplicates are guaranteed under at-least-once (Step 11) — a
      rebalance after `max.poll.interval.ms` (Step 9), a crash between
      processing and commit (Step 12), a non-blocking retry (Step 13) or a DLT
      replay (Step 23) all redeliver; the difference between the **idempotent
      producer** (broker-side, no duplicates on retry — Step 11) and an
      **idempotent consumer** (application-side, no double side effects); the
      dedupe techniques — a natural-key upsert, an idempotency/dedupe table
      keyed by a business id or by topic-partition-offset,
      `INSERT ... ON CONFLICT DO NOTHING`, and a Redis `SETNX` with a TTL;
      committing the side effect and the dedupe record atomically so a crash
      cannot leave them inconsistent; the trade-offs of dedupe-key choice
      (business key vs the record's topic/partition/offset) and the storage cost
      of a dedupe window; when exactly-once via Kafka transactions +
      `read_committed` is enough (Kafka-to-Kafka only) versus when
      application-level idempotency is the only real answer (a DB write, an HTTP
      call); proving it by replaying the same records (offset reset — Step 19)
      and showing the side effect happens exactly once.
      Exit criteria: learner replays a batch into the same consumer and the
      downstream state is unchanged — the duplicate is absorbed, not re-applied.

---

## Session state — update this after EVERY step

| Field | Value |
|---|---|
| Status | **Step 6a in progress** — Steps 0–5 done (producer → broker → consumer verified, LAG 0); Step 6a (producer sends a JSON DTO) code given but not yet applied; 3 small consumer fixes pending |
| Current step | Step 6a — producer sends a typed DTO as JSON (`JsonSerializer`); then 6b — consumer `JsonDeserializer` + trusted packages |
| Last verified step | Step 5 (complete) — consumer read all 4 records; `kafka-consumer-groups --describe --group learning-consumer` shows LAG 0 on partitions 0 and 1 |
| Project location | `F:\code\KodeGravity\CODE\Kafka\kafka_rev1` — sibling `producer` and `consumer` Maven projects |
| Broker | local Docker, KRaft **combined-mode single node**, image `apache/kafka:4.2.2` (= Apache Kafka 4.2.x), host port 9092 — **UP and verified** (Kafka 4.2.2 started, `isFenced: false`, API versions OK) |
| Toolchain | build with JDK 21 (`C:\Program Files\Java\jdk-21.0.10`) via `JAVA_HOME`; the PATH default is JDK 17 |
| Stack (resolved) | Spring Boot **4.1.1**, spring-kafka **4.1.1**, kafka-clients **4.2.1**, Java 21; deps: `spring-boot-starter-web`, `spring-boot-starter-aspectj`, `spring-boot-starter-kafka` |
| Topics | `local.learning.smoke.v1` — 3 partitions, RF 1 (created in Step 0; now also declared in code via `KafkaTopicConfig.smokeTopic()`) |
| Consumer groups | `learning-consumer` (consumer app) — 4 records read; LAG 0 on partitions 0 and 1 |
| Open questions | 1 broker now → 3 brokers before the reliability phase; AWS home (MSK provisioned vs Serverless vs self-managed) chosen in Step 29; Spring profiles dev/prod introduced in Step 28. Java 17-vs-21 is resolved: use 21. |
| Resume here (next session) | 1) apply the 3 consumer fixes: Lombok `<scope>annotationProcessor</scope>` → `<optional>true</optional>`, rename package `listner` → `listener`, log text `partation` → `partition`; 2) apply the Step 6a producer JSON changes (`OrderEvent` DTO, `JsonSerializer`, publisher uses `KafkaTemplate<String,Object>`, controller `POST /api/orders`); 3) test the POST; 4) then Step 6b (consumer `JsonDeserializer` + `spring.json.trusted.packages`) |

## Architecture built so far (class / method names for reference)

*Nothing yet.* Populate this as the session progresses:

| Class | Package | Role | Key methods / fields |
|---|---|---|---|
| `ProducerApplication` | `com.electronik54.producer` | Spring Boot entry point | `main` |
| `MessageController` | `...producer.controller` | REST API | `POST /api/messages` → `publish(PublishRequest)` → `ResponseEntity<PublishResponse>` |
| `MessagePublisher` | `...producer.service` | `KafkaTemplate` wrapper | `publish(topic, key, value)` → `PublishResponse`; throws `PublishException` |
| `PublishRequest` / `PublishResponse` | `...producer.dto` | API wire shapes | `key`,`value` / `topic`,`partition`,`offset` |
| `ErrorResponse` | `...producer.dto` | error body shape | `timestamp`,`status`,`error`,`message`,`path` |
| `GlobalExceptionHandler` | `...producer.exception` | `@RestControllerAdvice` | `PublishException`→503, validation→400, `Exception`→500 |
| `PublishException` | `...producer.exception` | custom runtime exception | — |
| `KafkaTopicConfig` | `...producer.config` | topic provisioning by code | `smokeTopic()` → `NewTopic` (3 partitions, RF 1) |
| `LoggingAspect` | `...producer.aspect` | AOP SLF4J logging | `@Around` on `controller` + `service` packages |
| `ConsumerApplication` | `com.electronik54.consumer` | Spring Boot entry point | `main` |
| `MessageListener` | `...consumer.listener` | `@KafkaListener` (receives records) | `onMessage(ConsumerRecord)` |
| `MessageService` | `...consumer.service` | business logic (the work) | `process(payload)` |

Planned shape (adjust to the learner's decisions):

- producer app — REST controller, DTO/model, `KafkaTemplate` wrapper service,
  topic config (`KafkaAdmin` + `NewTopic`), serializer config;
- consumer app — `@KafkaListener` handler(s), error handler + retry topic /
  DLT config, idempotency/dedupe store, pause-resume control, tests;
- load harness (Phase G) — `kafka-producer-perf-test.sh` /
  `kafka-consumer-perf-test.sh` invocations, a Spring `load` profile for the
  apps, a key-skew (hot-key) generator, and a recorded baseline results table.

## Configuration surface to cover (record actual values as they are set)

Producer: `bootstrap.servers` · `key.serializer` / `value.serializer` ·
`acks` (use `all`) · `enable.idempotence` · `compression.type` ·
`linger.ms` · `batch.size` · `max.request.size` (must be ≥ the biggest record) ·
`delivery.timeout.ms` · `max.in.flight.requests.per.connection` ·
`security.protocol` + SASL/TLS properties.

Consumer: `bootstrap.servers` · `group.id` · `key.deserializer` /
`value.deserializer` (wrapped in `ErrorHandlingDeserializer`) ·
`auto.offset.reset` (be explicit, never rely on the `latest` default) ·
`enable.auto.commit` · `fetch.min.bytes` · `fetch.max.bytes` /
`max.partition.fetch.bytes` (must be ≥ the biggest record) ·
`max.poll.records` · `max.poll.interval.ms` ·
`session.timeout.ms` / `heartbeat.interval.ms` ·
`partition.assignment.strategy` · `group.instance.id` · `isolation.level`.

Listener container: `concurrency` · `AckMode` · error handler ·
`@RetryableTopic` settings (attempts, backoff, DLT name) ·
pause/resume wiring.

Topic-level: `partitions` · `replication.factor` · `min.insync.replicas` ·
`retention.ms` · `cleanup.policy` · `max.message.bytes` (per-topic override of
the broker limit).

Harness (Phase G): `kafka-producer-perf-test.sh` (`--num-records`,
`--record-size`, `--throughput`, `--num-threads`) ·
`kafka-consumer-perf-test.sh` (`--messages`, `--threads`) · Spring profile
`load` · key-skew (hot-key) generator · baseline results table.

Per environment (Spring profiles, Phase H): `spring.profiles.active` ·
`application.yaml` (shared) · `application-dev.yaml` · `application-prod.yaml` ·
`@Profile` · `@ConfigurationProperties` · `SPRING_PROFILES_ACTIVE` ·
`bootstrap.servers` per profile · secrets via env vars, never committed.

AWS / scaling (Phase H): MSK bootstrap endpoints · IAM vs SASL/SCRAM vs mTLS ·
`security.protocol` per profile · VPC/subnet/AZ placement · broker count and
storage autoscaling · `kafka-topics.sh --alter --partitions` ·
`kafka-reassign-partitions.sh` · Cruise Control · `replication-factor` /
`min.insync.replicas` during a scale-out.

---

## Known gotchas / production traps (pre-seeded — confirm each live as we go)

These are the traps worth teaching **before** they are hit. Tick and annotate
them when the learner observes them for real.

### Data loss and duplication
1. **Auto-commit, or committing before processing, = silent data loss.** If the
   offset is committed and the process dies before the side effect, that record
   is gone forever. Commit **after**.
2. **At-least-once is the realistic default.** Duplicates are not a bug to fix —
   idempotency is a requirement to implement (dedupe key / upsert on a natural key).
3. **`acks=1` looks fine until the leader dies.** Records acked by a leader that
   then fails can be lost. Use `acks=all` with `min.insync.replicas >= 2`.
4. **Exceeding `max.poll.interval.ms` mid-processing evicts the consumer.** The
   partition is reassigned, the work is reprocessed by another consumer, and you
   get duplicates plus a rebalance storm. Budget this against worst-case processing time.

### Silently skipped work
5. **`auto.offset.reset` defaults to `latest`.** A brand-new consumer group
   ignores everything already in the topic — a classic "why did nothing process?"
   Make the choice explicit per listener.
6. **A deserialization failure kills the consumer before your code runs.** Without
   `ErrorHandlingDeserializer` the record can never be consumed, the offset never
   advances, and the partition is dead forever. Wrap the delegate.
7. **A poison record blocks its whole partition.** Nothing behind it is
   processed until it is handled — there is no automatic redrive, so the
   partition stalls and lag climbs.
8. **No DLT is built in.** Retry topics and a DLT are things you design,
   name, retain, alert on, and build replay tooling for.

### Ordering
9. **Ordering exists only within a partition.** A `null` key gives you spreading
   and no ordering. Choose the key deliberately.
10. **Partitions can only be increased, never decreased** — and increasing them
    changes the key-to-partition mapping, breaking ordering for existing keys.
    Changing the key is effectively a migration.
11. **A hot key is a hot partition.** One entity with dominant traffic creates a
    single-partition bottleneck no amount of consumer concurrency can fix.

### Scaling and rebalancing
12. **More consumers than partitions does nothing.** Extra instances idle —
    parallelism is capped at the partition count.
13. **Every deploy causes rebalances.** Eager assignment is stop-the-world.
    Use cooperative assignment plus static membership (`group.instance.id`).
14. **Consumers are not the only thing that must scale.** Partitions, brokers,
    disk, and network all need to be planned together.

### Correctness of the pipeline as a whole
15. **Kafka transactions do not cover a database write.** Use the transactional
    outbox or CDC; otherwise "DB then Kafka" is at-least-once with a loss window.
16. **`trusted.packages=*` is a deserialization gadget risk.** Whitelist your
    own packages.
17. **A shared `group.id` across unrelated services** silently turns them into
    competing consumers of each other's data. One group per logical consumer role.
18. **Third-party operational answers can be wrong for your version.** Verify
    client/property defaults against the actual resolved library version rather
    than trusting a blog post. *(Observed live: `apache/kafka` publishes no 3.5
    tag — its earliest is 3.7 — and Apache Kafka 3.5 is archived; the session
    therefore runs Kafka 4.2.x / `apache/kafka:4.2.2`, the current supported,
    KRaft-only line.)*

### Operations
19. **Consumer lag is the health signal; alert on its growth rate,** not on an
    absolute number.
20. **A DLT nobody monitors is just a quiet data-loss bin.** Alert on depth and
    write the 3am runbook.
21. **Retention has a disk cost.** `retention.ms` × throughput = bytes on every
    broker holding a replica. Tiered storage exists for a reason.
22. **`unclean.leader.election.enable=true` trades durability for availability.**
    Know which side you chose.

### Local environment (Docker)
23. **A corrupt cached image layer surfaces as `exec format error` /
    `input/output error`, not as a pull failure.** *(Observed live:
    `apache/kafka:4.2.2` started then immediately exited with
    `exec /__cacert_entrypoint.sh: input/output error`; `docker rmi` + re-pull
    reused the bad blob. Fix: `docker compose down`, `docker rmi <image>`,
    `docker system prune -a -f`, then re-pull — the fresh layers extract
    correctly.)*

### Topic design and naming
24. **A topic name containing `.` or `_` warns about metric-name collisions.**
    *(Observed live: `kafka-topics.sh --create` warned that periods and
    underscores fold to `_` in metric names, so `a.b` and `a_b` collide; pick
    one style — the session uses dotted names such as
    `local.learning.smoke.v1`.)*

### Framework / dependencies
25. **Spring Boot 4 renamed the AOP starter.** `spring-boot-starter-aop` stops at
    3.5.x (4.x had only milestones); Spring Boot 4 uses
    `spring-boot-starter-aspectj`, which pulls `spring-aop` + `aspectjweaver`.
    *(Observed live: adding `spring-boot-starter-aop` under the 4.1.1 parent gave
    `'dependencies.dependency.version' ... is missing` — the parent no longer
    manages it — which failed the build and surfaced at runtime as
    `ClassNotFoundException` for the main class. Read the Maven error, not the
    runtime symptom.)*
26. **Spring Boot 4 split auto-configuration into modules — `spring-kafka` alone
    is not enough.** `spring-boot-autoconfigure` is now a thin shell; Kafka
    auto-configuration lives in the `spring-boot-kafka` module, pulled by the
    `spring-boot-starter-kafka` starter. *(Observed live: with only
    `org.springframework.kafka:spring-kafka` on the classpath there was **no
    `KafkaTemplate` bean** — "Could not autowire" — because nothing
    auto-configured it. Fix: depend on `spring-boot-starter-kafka`.)*
27. **Kafka 4.x deprecates `--property` on the console consumer** in favour of
    `--formatter-property` (`--property` still works but warns). *(Observed live
    during the Step 4 verification.)*
28. **`<scope>annotationProcessor</scope>` is a Gradle concept, not Maven.**
    Lombok in Maven uses `<optional>true</optional>` (no scope); an invalid scope
    makes Maven warn that the POM is malformed and threatens build stability.
    *(Observed live: the consumer pom used the Gradle scope; Maven warned but
    still built.)*

---

## Verification commands to have ready

Adjust broker container names and paths to the chosen environment. These are
the commands the agent should reference when teaching and when verifying.

- Broker / cluster
  - `docker compose up -d` (KRaft broker), `docker compose logs -f kafka`
  - `kafka-broker-api-versions.sh --bootstrap-server localhost:9092` (connectivity)
- Topics
  - `kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <name> --partitions <n> --replication-factor <n>`
  - `kafka-topics.sh --bootstrap-server localhost:9092 --list`
  - `kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic <name>` (partitions, leaders, replicas, ISR)
  - `kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type topics --entity-name <name>` (retention, cleanup.policy)
  - `kafka-topics.sh ... --alter --topic <name> --partitions <n>` (increase only)
- Produce / consume by hand
  - `kafka-console-producer.sh --bootstrap-server localhost:9092 --topic <name>`
  - `kafka-console-producer.sh ... --property parse.key=true --property key.separator=:` (see partitioning)
  - `kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <name> --from-beginning`
  - `kafka-console-consumer.sh ... --property print.key=true --property print.partition=true`
- Offsets and lag (the daily habit)
  - `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list`
  - `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <group-id>`
    (read `CURRENT-OFFSET`, `LOG-END-OFFSET`, `LAG`, and the partition columns)
  - `kafka-consumer-groups.sh ... --reset-offsets --to-earliest --topic <name> --execute` (**replay** — dry-run first)
- Deletion and cleanup
  - `kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic <name>`
  - `kafka-consumer-groups.sh ... --delete --group <group-id>`
- App-level checks
  - compile each project from its root; read the app log for the produced
    partition + offset and the consumed record
  - confirm the DLT received the deliberate bad record, and inspect its headers

## Kafka log semantics — quick reference (for the learner's own revision)

| Property | Kafka |
|---|---|
| Core abstraction | append-only log |
| Delete on read? | no — records expire by `retention.ms` / `retention.bytes` |
| Replay? | yes, by resetting consumer-group offsets |
| Fan-out | independent consumer groups, each with its own offsets |
| Ordering | per partition, chosen by the record key |
| Ack | offset commit |
| Retry | retry topics / `seek` — you design it |
| DLQ | a DLT you build, name, retain and monitor |
| Progress metric | consumer lag (per partition) |
| Parallelism limit | partition count (per consumer group) |
| Ops | you run the brokers |
| Retention | configurable; effectively forever with compaction |

## Optional / advanced topics (only if the core roadmap is done)

- Kafka Streams for stateful processing (joins, windows, aggregations, KTables).
- MirrorMaker 2 topology design for DR, including offset translation caveats.
- Kafka Connect for source/sink connectors (and Debezium for CDC).
- Tiered/remote storage for cheap long retention.
- Multi-tenant cluster design: quotas, throttling, per-team ACLs.
- Partition rebalancing tooling and broker-level rebalancing of existing topics.
- Rack awareness and follower fetching for cross-AZ cost reduction.
- Schema Registry deep-dive: subject strategies, compatibility testing in CI.
- Running Kafka on Kubernetes (Strimzi) vs managed (MSK/Confluent).

## How to resume the session

1. The agent reads this file first, then reads the current step's row in the
   session-state table and the roadmap checkboxes to find where we left off.
2. The agent confirms the current step with the learner and restates the
   concept to be taught.
3. Restore the loop: **teach concept → show code in chat → learner implements
   → learner says "Done" → agent reads + compiles + verifies → next step.**
4. The agent updates the roadmap checkboxes, the session-state table, the
   configuration values actually used, and appends any newly discovered gotcha.
5. Keep the focus on the two themes — Kafka at production level and the
   Resilience4j CircuitBreaker around the consumer's downstream calls — and
   relate every new step back to the production failure mode it prevents.




# 37. Exactly-Once, Explained Precisely

The most misused phrase in streaming. Almost everything written about it is imprecise. This chapter is about being exact.

> **Key idea**
> Flink guarantees exactly-once **state** semantics, not exactly-once **delivery**.
> Each record affects your operator state exactly once. Records **are** reprocessed after a restore — the guarantee is about the *effect on state*, not about the number of times `processElement` runs.
> End-to-end exactly-once needs two more things, neither of which Flink can provide alone: a **replayable source** and a **transactional or idempotent sink**.

---

## Two different claims

### Claim 1: exactly-once STATE (what Flink gives you)

```
Job counts events per user.
Checkpoint 42 saved: u1 = 500, Kafka offset = 1,000,000

Records 1,000,001 .. 1,000,120 arrive → in-memory count reaches 620
💥 crash
Restore from checkpoint 42        → count reset to 500, offset reset to 1,000,000
Records 1,000,001 .. 1,000,120 processed AGAIN → count reaches 620

Final state: u1 = 620.
```

`processElement` ran **240 times** for 120 records. Every one of those 120 records was processed **twice**. And yet the count is 620, exactly as if each record had been counted once.

That is the guarantee, precisely stated:

> The state, at any consistent point, is **as if** each input record had been processed exactly once.

It works because the rewind undoes the effect *and* the input together. It is atomic in the same way a database transaction rollback is atomic.

### Claim 2: exactly-once DELIVERY (what Flink does *not* give you)

```
Same job, but it also INSERTs each result into Postgres.

Before the crash: 120 INSERTs executed and committed.
After the restore: the same 120 records processed again → 120 MORE INSERTs.

Flink's state: u1 = 620.  Correct.
Postgres:      240 rows.  Wrong.
```

The INSERT escaped Flink's boundary. Flink cannot roll back your database. Checkpoints control what Flink owns; they do not control the outside world.

> **The one sentence to say in an interview:**
> *"Flink's exactly-once is a statement about internal state. Records after the last checkpoint are definitely reprocessed. Whether the outside world sees that twice is entirely a property of your sink."*

---

## The three ingredients of end-to-end exactly-once

```
┌──────────────┐      ┌──────────────┐      ┌──────────────────────┐
│ REPLAYABLE   │      │ CHECKPOINTED │      │ TRANSACTIONAL or     │
│ SOURCE       │ ───► │ STATE        │ ───► │ IDEMPOTENT SINK      │
│              │      │              │      │                      │
│ can rewind   │      │ exactly-once │      │ replay does not      │
│ to an offset │      │ mode         │      │ duplicate the effect │
└──────────────┘      └──────────────┘      └──────────────────────┘
    Kafka ✅              Flink ✅               your problem ⚠️
    Kinesis ✅
    Pulsar ✅
    files ✅
    a socket ❌
    an HTTP push ❌
```

**Replayable source.** Kafka, Kinesis, Pulsar, files: you can ask for "everything from position X" again. A raw TCP socket or a webhook cannot replay — data that arrived during the failed window is simply gone, and no amount of configuration recovers it. If your source is not replayable you cannot have exactly-once, full stop.

**Checkpointed state.** `EXACTLY_ONCE` mode, chapter 33.

**The sink.** Three options, below.

---

## Path 1: Two-phase commit (transactional sinks)

### The protocol, tied to checkpointing

The insight: a checkpoint is already a **distributed atomic commit** across all operators. So bolt the sink's transaction onto it.

```
 JobManager        Source          Operator          Sink            Kafka
     │               │                │               │                │
 (1) │─ trigger CP N ►                │               │                │
     │               │                │               │                │
     │          begin txn ────────────────────────────────────────────►│
     │               │  (a transaction is open, records go into it,
     │               │   but readers with read_committed see NOTHING)
     │               │                │               │                │
     │               │─ e e ▮ ────────►               │                │
     │               │        (barrier)               │                │
     │               │                │─ e e ▮ ──────►│                │
     │               │                │               │                │
 (2) │               │                │        ┌──────┴───────┐        │
     │               │                │        │ PRE-COMMIT   │        │
     │               │                │        │ flush all    │────────►│
     │               │                │        │ buffered recs│        │
     │               │                │        │ into the txn │  data is IN Kafka,
     │               │                │        │ FLUSH, do    │  but NOT VISIBLE
     │               │                │        │ NOT commit   │        │
     │               │                │        │ snapshot the │        │
     │               │                │        │ txn HANDLE   │        │
     │               │                │        │ into state   │        │
     │               │                │        └──────┬───────┘        │
     │◄── ACK checkpoint N ───────────────────────────┤                │
     │               │                │               │                │
 (3) │  ALL subtasks ACKed → checkpoint N is COMPLETE                  │
     │  JM writes _metadata                                            │
     │               │                │               │                │
 (4) │── notifyCheckpointComplete(N) ─────────────────►               │
     │               │                │        ┌──────┴───────┐        │
     │               │                │        │   COMMIT     │────────►│
     │               │                │        │  the txn     │  NOW VISIBLE,
     │               │                │        └──────────────┘  atomically
```

Two phases, exactly like a database 2PC:

- **Phase 1 (pre-commit)**, on the barrier: write everything into an open transaction, flush it durably, and save the *transaction handle* into Flink state. The sink is now voting "I can commit". Nothing is visible to readers.
- **Phase 2 (commit)**, on `notifyCheckpointComplete`: the JobManager confirms every operator ACKed, and the sink commits.

### Why every failure point is safe

| Crash at | What happens on restore |
|---|---|
| Before pre-commit | Transaction never flushed. Restore from checkpoint N-1, rewind, redo. Kafka aborts the orphan transaction on timeout. Nothing visible. |
| After pre-commit, before checkpoint N completes | Checkpoint N never completed, so restore uses N-1. The pre-committed transaction is never committed and eventually aborts. Nothing visible. |
| After checkpoint N completes, before commit | **This is the important one.** Checkpoint N *is* the restore point, and it contains the transaction handle. On restore the sink reads that handle and **commits the pending transaction**. This is why the handle must be in state. |
| After commit | Restore from N; the transaction is already committed; committing again is a no-op. |

There is no window where a partial result becomes visible. That is the whole point.

### The API: Sink V2 and the committer model

`TwoPhaseCommitSinkFunction` was the old (`SinkFunction`-based) abstraction and is deprecated along with the whole `SinkFunction` API. The modern equivalent is the **Sink V2** interface, where the same protocol is expressed as three pieces:

```
Sink<IN>
 ├── SinkWriter<IN, CommT>          writes records; on checkpoint, produces
 │                                  "committables" (e.g. an open txn handle)
 ├── Committer<CommT>               commits a committable when the checkpoint
 │                                  completes.  ← phase 2
 └── (optional) GlobalCommitter     for sinks needing one commit for the whole job
```

You almost never implement this yourself. You use a connector that already does:

- `KafkaSink` with `DeliveryGuarantee.EXACTLY_ONCE`
- `FileSink` — writes to in-progress files, renames them to final on checkpoint complete
- JDBC `exactly-once` sink via XA transactions (`JdbcSink.exactlyOnceSink(...)`)
- Iceberg / Delta / Hudi connectors — commit a table snapshot per checkpoint

The mental model that transfers to all of them: **write to a place nobody is looking at, and make it visible on `notifyCheckpointComplete`.** For Kafka that is a transaction; for files it is a rename; for Iceberg it is a metadata commit.

---

## Kafka exactly-once: the specifics

```java
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.Properties;

Properties producerProps = new Properties();
producerProps.setProperty("transaction.timeout.ms", "900000");   // 15 minutes

KafkaSink<Alert> sink = KafkaSink.<Alert>builder()
        .setBootstrapServers("broker:9092")
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix("fraud-detector-v1-")
        .setKafkaProducerConfig(producerProps)
        .setRecordSerializer(
                KafkaRecordSerializationSchema.<Alert>builder()
                        .setTopic("alerts")
                        .setValueSerializationSchema(new AlertSerializer())
                        .build())
        .build();

alerts.sinkTo(sink).uid("alert-sink");
```

Line notes:

- `KafkaSink.<Alert>builder()` — the `.<Alert>` before the method name is an **explicit type witness**, telling the compiler what `T` is when there is no argument to infer it from. Same construct as in `WatermarkStrategy.<Event>forBoundedOutOfOrderness(...)` from chapter 10.
- `DeliveryGuarantee` has three values: `NONE` (fire and forget), `AT_LEAST_ONCE` (flush on checkpoint, no transaction), `EXACTLY_ONCE` (transactional).
- `.uid("alert-sink")` — an exactly-once sink is a **stateful** operator (it stores transaction handles), so it needs a UID like any other. Chapter 35.

### The four things that go wrong

**1. Latency is now bound to your checkpoint interval.**

```
checkpoint interval = 60s
   → nothing is committed until a checkpoint completes
   → a read_committed consumer sees each record 0–60 seconds after Flink produced it
   → your end-to-end p99 latency FLOOR is 60 seconds
```

This is not a bug or a tuning problem. It is the definition: exactly-once means results become visible atomically at commit points, and the commit points *are* the checkpoints. Sub-second visibility and transactional exactly-once are mutually exclusive.

If you need both low latency and correctness, you need path 3 (idempotent sinks) — or you accept a much shorter checkpoint interval and pay the checkpointing overhead for it.

**2. Consumers must set `isolation.level=read_committed`.**

```properties
isolation.level=read_committed
```

Kafka's consumer default is `read_uncommitted`. A downstream consumer left on the default reads **uncommitted and aborted transaction data**, so you get duplicates anyway — having done everything else perfectly. This is the most common way an exactly-once Kafka pipeline silently isn't one, and it is not a Flink setting; it belongs to whoever wrote the consumer.

**3. `transactionalIdPrefix` must be unique per job.**

Kafka's transaction fencing uses the transactional ID: a producer registering with an existing ID **fences off** the previous one. Two Flink jobs sharing a prefix will kill each other's transactions, showing up as:

```
org.apache.kafka.common.errors.ProducerFencedException
```

Include the job name and a version in it: `"fraud-detector-v1-"`. And note the version part matters when you redeploy — if you change the prefix you also abandon any pending transactions under the old one.

**4. Transaction timeout must exceed your maximum checkpoint duration.**

```properties
# On the Kafka BROKER:
transaction.max.timeout.ms = 900000    # 15 min (default)

# On the PRODUCER (Flink's side) — the trap:
transaction.timeout.ms = 60000         # 1 min DEFAULT — far too low
```

If a checkpoint takes longer than the producer's `transaction.timeout.ms`, the broker aborts the transaction before Flink can commit it. The consequence is not duplicates — it is **data loss**, plus a job failure when the commit finds its transaction gone.

The producer value also cannot exceed the broker's `transaction.max.timeout.ms`, or the producer is rejected at startup.

```java
producerProps.setProperty("transaction.timeout.ms", "900000");
```

**Rule: `transaction.timeout.ms` ≥ (checkpoint timeout + restart time + safety margin)**, and ≤ the broker's `transaction.max.timeout.ms`. Fifteen minutes is a sane default for both.

The reason recovery time matters: after a restart, Flink must commit transactions that were pre-committed *before* the failure. If the job is down for 10 minutes and the timeout is 5, those transactions are gone.

### The Kafka source side

For exactly-once from Kafka, offsets must live in **Flink state**, not in Kafka's `__consumer_offsets`:

```java
KafkaSource<Event> source = KafkaSource.<Event>builder()
        .setBootstrapServers("broker:9092")
        .setTopics("events")
        .setGroupId("fraud-detector")
        .setStartingOffsets(OffsetsInitializer.committedOffsets(
                OffsetResetStrategy.EARLIEST))
        .setValueOnlyDeserializer(new EventDeserializer())
        .build();
```

This is already the default behaviour: `KafkaSource` checkpoints its offsets and rewinds to them on restore. It also commits offsets back to Kafka on checkpoint completion, but **only for your monitoring tools** — Flink itself never reads those back during recovery. If it did, the rewind would be wrong.

---

## Path 2: At-least-once — often the right, cheaper answer

```java
env.enableCheckpointing(10_000);
env.getCheckpointConfig()
   .setCheckpointingConsistencyMode(CheckpointingMode.AT_LEAST_ONCE);
```

```java
KafkaSink<Alert> sink = KafkaSink.<Alert>builder()
        .setBootstrapServers("broker:9092")
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)   // no transactions
        .setRecordSerializer(...)
        .build();
```

What you get: no barrier alignment, no transactions, no commit latency. Results are visible immediately. Records after the last checkpoint may be processed and emitted twice.

**When at-least-once is genuinely correct:**

- **Metrics and approximations.** A dashboard counting page views. A 0.001% overcount during a rare restart is not worth a 60-second latency floor.
- **Idempotent state.** `MAX(amount)`, "last seen value", set membership. Applying the same record twice changes nothing.
- **Downstream deduplicates anyway.** A data warehouse doing MERGE on a primary key, an upsert store, a search index keyed by document id.
- **Latency dominates correctness.** Alerting where a duplicate alert is annoying and a 60-second delay is unacceptable.

Choosing at-least-once *deliberately* and articulating why is a stronger answer than reflexively demanding exactly-once. Exactly-once has a real cost, in latency, complexity, and operational fragility.

---

## Path 3: Idempotent sinks — usually the best answer

Rather than making the write transactional, make it **repeatable without effect**.

```java
public class IdempotentSink extends RichSinkFunction<Result> {

    private transient Connection conn;
    private transient PreparedStatement stmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection(url, user, pass);
        stmt = conn.prepareStatement(
                "INSERT INTO results (id, user_id, window_start, val) " +
                "VALUES (?, ?, ?, ?) " +
                "ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
    }

    @Override
    public void invoke(Result r, Context ctx) throws Exception {
        // The id must be DETERMINISTIC: derived only from the data.
        String id = r.getUserId() + ":" + r.getWindowStart();
        stmt.setString(1, id);
        stmt.setString(2, r.getUserId());
        stmt.setLong(3, r.getWindowStart());
        stmt.setDouble(4, r.getVal());
        stmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
    }
}
```

Line notes:

- `transient` tells Java's serializer "do not try to serialize this field". Flink ships your function object to the TaskManagers by serializing it, and a JDBC `Connection` is not serializable. The pattern is: declare it `transient`, create it in `open()`, close it in `close()`.
- `RichSinkFunction` (as opposed to plain `SinkFunction`) is what gives you `open()` and `close()` lifecycle hooks — that's what "Rich" means throughout Flink's API.
- `ON CONFLICT (id) DO UPDATE` is Postgres upsert syntax. MySQL: `ON DUPLICATE KEY UPDATE`. Cassandra: every write is already an upsert. Elasticsearch: index by document id.

### The whole thing hinges on determinism

```java
// ❌ different on every replay — every retry creates a NEW row
String id = UUID.randomUUID().toString();
String id = r.getUserId() + ":" + System.currentTimeMillis();
String id = String.valueOf(counter++);

// ✅ derived purely from the input record and deterministic window boundaries
String id = r.getUserId() + ":" + r.getWindowStart();
String id = r.getSourceEventId();
```

The same trap applies to **processing-time windows**: their boundaries depend on wall-clock time, so a replay produces different windows and therefore different keys. This is a correctness reason to prefer event time, on top of the completeness reasons from chapter 10.

### Related trap: side effects inside operators

```java
// ❌ fires again on every replay, no matter what your sink does
public class AlertProcessor extends KeyedProcessFunction<String, Event, Alert> {
    @Override
    public void processElement(Event e, Context ctx, Collector<Alert> out) {
        if (e.amount > 10000) {
            pagerDuty.trigger(e);      // outside every transaction
            emailService.send(e);      // same
        }
        out.collect(new Alert(e));
    }
}
```

No sink configuration helps: the call happened inside `processElement`, which *definitely* runs more than once. Emit to a sink and make **that** idempotent. Same category: writing files directly, incrementing an external counter, calling a webhook.

> Exactly-once is a property of **the boundary**, not of your business logic.

---

## Choosing

| Approach | Latency | Complexity | Choose when |
|---|---|---|---|
| At-least-once + **idempotent sink** | low | low | **The default.** Sink supports upsert by a deterministic key. |
| Exactly-once **2PC** | = checkpoint interval | high | Kafka→Kafka, append-only sinks, or a real transactional DB with no natural key. |
| At-least-once, no dedup | lowest | none | Metrics, approximations, replay-idempotent state. |
| Exactly-once with sub-second latency | — | — | **Does not exist** with 2PC. Use an idempotent sink. |

The decision procedure:

```
Does my sink support upsert by a deterministic key?
  YES → at-least-once + idempotent sink. Done. Lowest latency, lowest complexity.
  NO  → Do duplicates actually matter for this data?
          NO  → at-least-once. Done.
          YES → Can I tolerate latency = checkpoint interval?
                  YES → exactly-once 2PC.
                  NO  → you have a design problem. Add a dedup key upstream
                        or introduce a deduplicating store.
```

---

## The deep dive

The scenario-based version of this — *"you have exactly-once enabled and you're still seeing duplicates in the database, why?"* — with the failure timelines, the four Kafka gotchas as interview traps, and the comparison table framed for an interviewer, is in:

**[`../../04-exactly-once.md`]**

Read this chapter for the model; read that file before an interview.

---

## Remember

- **Exactly-once STATE ≠ exactly-once DELIVERY.** Flink guarantees the first. The second needs a cooperating sink.
- Records after the last checkpoint **are** reprocessed. `processElement` runs more than once, by design. The state is what comes out right.
- End-to-end needs: **replayable source + checkpointed state + transactional or idempotent sink.** A socket source cannot ever have it.
- **2PC**: pre-commit on the barrier (write into an open transaction, save the handle to state), commit on `notifyCheckpointComplete`. Every crash window is safe because the handle is in the checkpoint.
- `TwoPhaseCommitSinkFunction` is deprecated; **Sink V2** expresses the same thing as `SinkWriter` + `Committer`.
- The universal pattern: **write somewhere invisible, make it visible on checkpoint complete.** Kafka transaction, file rename, table metadata commit.
- Kafka exactly-once: **latency floor = checkpoint interval**. Consumers must set `isolation.level=read_committed`. `transactionalIdPrefix` unique per job. `transaction.timeout.ms` (default 1 min) **must exceed** your max checkpoint duration plus restart time, or you lose data.
- **At-least-once is often correct and always cheaper.** Choose it deliberately.
- **Idempotent sinks are usually the best answer**: lowest latency, lowest complexity — but the key must be derived purely from the data. No UUIDs, no `System.currentTimeMillis()`, no processing-time windows.
- **Any external call inside an operator is outside every transaction** and fires again on replay.

**Interview one-liners**

- *"What does Flink's exactly-once actually guarantee?"* → That state reflects each input record exactly once. Records after the last checkpoint are reprocessed on recovery; the rewind undoes the state effect at the same time, so the net effect is once.
- *"What's required for end-to-end exactly-once?"* → A replayable source, checkpointed state, and a sink that is either transactional (2PC) or idempotent. Flink can only supply the middle one.
- *"Explain the two-phase commit."* → On the checkpoint barrier the sink flushes into an open transaction and stores the transaction handle in state — that's the pre-commit vote. When the JobManager confirms the checkpoint completed globally, `notifyCheckpointComplete` triggers the commit. A crash between the two is safe because the handle is in the checkpoint and is committed on restore.
- *"What's the latency cost?"* → Nothing is visible until a checkpoint commits, so your end-to-end latency floor equals the checkpoint interval.
- *"Exactly-once enabled but you still see duplicates — why?"* → Most often a non-transactional sink, or downstream consumers left on `isolation.level=read_uncommitted`.
- *"When would you choose at-least-once?"* → When the sink upserts by a deterministic key, when the state is replay-idempotent, or when latency matters more than a rare duplicate — metrics, approximations, alerting.
- *"Why is `transaction.timeout.ms` dangerous?"* → It defaults to one minute; if a checkpoint or a restart takes longer, Kafka aborts the pre-committed transaction and you *lose* data rather than duplicate it.

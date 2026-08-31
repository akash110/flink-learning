# Exactly-Once — What It Actually Guarantees

> Typical questions: *"You have exactly-once enabled but you're seeing duplicates in the
> database. How?"* — *"Explain the two-phase commit."* — *"What's the latency cost?"*

The single most misunderstood topic in streaming. Most candidates recite "Flink supports
exactly-once" and fall apart on the follow-up.

---

## The claim that wins the question

> **Flink guarantees exactly-once *state* semantics, not exactly-once *delivery*.**
> End-to-end exactly-once requires the sink to cooperate — either transactionally or
> idempotently. Flink alone cannot give it to you.

If you say only this, you're ahead of most candidates. Everything below is the detail.

---

## Why `EXACTLY_ONCE` mode alone gives you duplicates

```java
env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);   // ✅ state is safe

// ❌ ...but this sink is not transactional
stream.addSink(new RichSinkFunction<Result>() {
    public void invoke(Result r, Context ctx) throws Exception {
        jdbc.execute("INSERT INTO results VALUES (?)", r);
    }
});
```

The failure:

```
t=0   checkpoint 5 completes.  Kafka offset 1000 recorded in state.
t=10  records 1000..1500 processed, 500 INSERTs committed to the DB.
t=20  💥 TaskManager dies.
t=30  Flink restores from checkpoint 5 → rewinds Kafka to offset 1000.
t=40  records 1000..1500 processed AGAIN → 500 MORE INSERTs.

Result: 500 duplicate rows. Flink's state is perfectly correct.
        The database is wrong.
```

Flink's contract is: *the internal state reflects each input record exactly once.* Side
effects sent outside the system are entirely your responsibility.

---

## Fix A: Two-phase commit (transactional sink)

```java
// ✅ Kafka sink with 2PC
KafkaSink<Result> sink = KafkaSink.<Result>builder()
    .setBootstrapServers(brokers)
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .setTransactionalIdPrefix("my-job-")     // ⚠️ MUST be unique per job
    .setRecordSerializer(...)
    .build();
```

### The protocol

```
1. PRE-COMMIT  — on checkpoint barrier, sink flushes into an OPEN Kafka transaction
                 and votes "ready". Data is in Kafka but NOT visible to readers.
2. Checkpoint completes across ALL operators.
3. COMMIT      — JobManager sends notifyCheckpointComplete; sink commits the transaction.
                 Data becomes visible atomically.
```

Failure between 1 and 3 → transaction is aborted or replayed on restore. Nothing partial
becomes visible.

### The four gotchas (this is where interviews go)

**1. Latency is now bound to your checkpoint interval.**
```
checkpoint interval = 60s  →  downstream consumers see data up to 60s late
```
That surprises people. Exactly-once with 2PC means your *end-to-end latency floor is your
checkpoint interval*, because nothing is visible until the checkpoint commits. If you need
sub-second visibility, you cannot have transactional exactly-once. That's a real
architectural trade, and naming it is a strong signal.

**2. Consumers must set `isolation.level=read_committed`.**
```properties
isolation.level=read_committed
```
Default is `read_uncommitted` — your downstream consumer reads aborted transaction data and
you get duplicates *anyway*, despite doing everything else right.

**3. `transactionalIdPrefix` must be unique per job.**
Two jobs sharing a prefix will fence each other off — one job's transactions abort the
other's. Symptom is a confusing `ProducerFencedException`.

**4. Transaction timeout must exceed your checkpoint interval.**
```properties
# Kafka broker: transaction.max.timeout.ms  (default 15 min)
# Producer:     transaction.timeout.ms      (default 1 min)  ← too low!
```
If a checkpoint takes longer than `transaction.timeout.ms`, Kafka aborts the transaction and
**you lose data** — not duplicate it, *lose* it. Set producer timeout comfortably above your
max checkpoint duration.

```java
Properties p = new Properties();
p.setProperty("transaction.timeout.ms", "900000");   // 15 min
```

---

## Fix B: Idempotent writes (usually the better answer)

Often simpler, cheaper, and lower-latency than 2PC.

```java
// ✅ deterministic key → replay overwrites instead of duplicating
public class IdempotentSink extends RichSinkFunction<Result> {
    public void invoke(Result r, Context ctx) throws Exception {
        // key derived from the DATA, not from time/random/counter
        String id = r.getEventId() + ":" + r.getWindowStart();
        jdbc.execute(
            "INSERT INTO results (id, val) VALUES (?, ?) " +
            "ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val", id, r.getVal());
    }
}
```

**The requirement is determinism.** The key must be reproducible on replay:

```java
// ❌ breaks idempotency — different on every replay
String id = UUID.randomUUID().toString();
String id = r.getUserId() + ":" + System.currentTimeMillis();

// ✅ derived purely from the input record + deterministic window boundary
String id = r.getUserId() + ":" + r.getWindowStart();
```

Same trap applies to **processing-time windows** and any use of `System.currentTimeMillis()`
in your logic — replays produce different results. This is why event-time processing matters
for correctness, not just for lateness ([[05-watermarks-and-time]]).

---

## Fix C: `AT_LEAST_ONCE` + downstream dedup

```java
env.enableCheckpointing(10_000, CheckpointingMode.AT_LEAST_ONCE);
```

Faster (no alignment, no 2PC latency), and correct if downstream can dedup. Perfectly
respectable answer when the sink is an upsert store (Cassandra, Elasticsearch by doc id,
any key-value store). Choosing this deliberately and explaining *why* is stronger than
reflexively picking exactly-once.

---

## The comparison table

| Approach | Latency | Complexity | When |
|---|---|---|---|
| At-least-once + idempotent sink | low | low | **default choice** — upsert-capable sink |
| Exactly-once 2PC | = checkpoint interval | high | Kafka→Kafka, or a real transactional DB |
| At-least-once, no dedup | lowest | none | metrics/approximations where dupes are ok |

---

## Related trap: side effects in operators

```java
// ❌ this fires again on every replay, no matter what your sink does
public class AlertProcessor extends KeyedProcessFunction<String, Event, Alert> {
    public void processElement(Event e, Context ctx, Collector<Alert> out) {
        if (e.getAmount() > 10000) {
            pagerDuty.trigger(e);        // 🔴 external call, not covered by checkpoints
            emailService.send(e);        // 🔴 same
        }
        out.collect(new Alert(e));
    }
}
```

Any external call from inside an operator is outside the transaction. On replay, the pager
fires again. The fix is to emit to a sink and make *that* idempotent — never call external
services directly from a `process` function.

Same category: writing to files, incrementing external counters, publishing to a webhook.
Interviewers like this one because it shows whether you understand that exactly-once is a
property of the *boundary*, not of the code.

See also: [[01-checkpointing-slow]], [[05-watermarks-and-time]], [[03-state-and-skew]]

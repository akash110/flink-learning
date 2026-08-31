# 29. Partitioning: How Records Move Between Subtasks

Chapter 28 explained that an operator runs as *p* subtasks. This chapter answers the next question: when subtask `map[2]` produces a record, **which** downstream subtask receives it?

That decision is the **partitioning strategy** on the edge between two operators. Flink picks a default; you can override it.

---

## The default: `forward`

When two connected operators have the **same parallelism** and you have not asked for anything else, Flink uses **FORWARD**: subtask *i* sends only to subtask *i*.

```
FORWARD (default when parallelism matches)
  map[0] ─────────► filter[0]
  map[1] ─────────► filter[1]
  map[2] ─────────► filter[2]
  map[3] ─────────► filter[3]

  no shuffle, no serialization, no network — and it is the
  precondition for operator CHAINING (ch. 28)
```

This is the cheapest possible edge. It is why keeping parallelism uniform through a pipeline matters.

If the parallelisms **differ** and you have not specified anything, Flink falls back to **REBALANCE** (round-robin) automatically. You did not ask for a shuffle, but you got one, because there is no sensible one-to-one mapping between 4 and 7 subtasks.

> **Key idea:** Same parallelism + no explicit partitioner = **forward, free**. Different parallelism = **rebalance, full network shuffle**. Uniform parallelism is not a style preference, it is a performance decision.

---

## `keyBy` — hash partitioning

```java
DataStream<Event> keyed = events.keyBy(e -> e.userId);
```

`keyBy` is a **HASH partition**. The target subtask is computed from the key:

```java
// conceptually, what Flink computes:
keyGroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;
subtask  = keyGroup * parallelism / maxParallelism;
```

Note it goes **through key groups** (chapter 28) rather than `hash % parallelism` directly — that indirection is what makes rescaling from a savepoint possible.

```
HASH (keyBy)
  map[0] ──┐  ┌──► window[0]   keys: u1, u7, u9 ...
  map[1] ──┼──┼──► window[1]   keys: u2, u4 ...
  map[2] ──┼──┼──► window[2]   keys: u3, u8 ...
  map[3] ──┘  └──► window[3]   keys: u5, u6 ...

  every upstream subtask can send to every downstream subtask
  GUARANTEE: the same key ALWAYS goes to the same subtask
```

That guarantee is the entire basis of keyed state. It is also the source of skew, covered at the end of this chapter.

There is a lower-level form if you want hash partitioning without entering the keyed world:

```java
// Partitions by key hash but does NOT create a KeyedStream,
// so no keyed state or timers. Rarely what you want.
DataStream<Event> p = events.partitionCustom(
        (key, numPartitions) -> Math.abs(key.hashCode()) % numPartitions,
        (Event e) -> e.userId);
```

---

## `rebalance()` — round robin, everywhere

```java
DataStream<Event> balanced = events.rebalance();
```

Each upstream subtask sends its records round-robin to **all** downstream subtasks.

```
REBALANCE
  src[0] ──┬──► op[0]
           ├──► op[1]
           ├──► op[2]
           └──► op[3]
  src[1] ──┬──► op[0]
           ├──► op[1]     every source subtask connects to
           ├──► op[2]     EVERY target subtask
           └──► op[3]
  ...

  connections = upstream_p × downstream_p     ← full mesh
```

Result: perfectly even distribution, at the cost of a full network shuffle and `p × p` connections.

---

## `rescale()` — round robin, but local

```java
DataStream<Event> rescaled = events.rescale();
```

Same round-robin behaviour, but each upstream subtask only distributes to a **subset** of downstream subtasks. Flink partitions the subtasks into disjoint groups.

```
REBALANCE, p 2 → 6              RESCALE, p 2 → 6
 src[0] ──┬──► op[0]             src[0] ──┬──► op[0]
          ├──► op[1]                      ├──► op[1]
          ├──► op[2]                      └──► op[2]
          ├──► op[3]
          ├──► op[4]             src[1] ──┬──► op[3]
          └──► op[5]                      ├──► op[4]
 src[1] ──┬──► op[0]                      └──► op[5]
          ├──► op[1]
          ├──► ...  (all 6)      12 connections → 6 connections
                                  and each group can be scheduled
 12 connections total             on the SAME TaskManager, so the
                                  handoff is often LOCAL (no network)
```

The difference that matters:

| | `rebalance()` | `rescale()` |
|---|---|---|
| Distribution | Global round robin | Round robin within a subset |
| Connections | `up × down` (full mesh) | `max(up, down)` |
| Network | Always | Often none — subsets can be co-located |
| Evenness | Perfect across the whole job | Even only within each subset |
| Use for | Fixing genuine skew | Cheap fan-out/fan-in at proportional parallelism |

`rescale()` is the right tool when parallelisms are **integer multiples** (2→6, 8→4). If upstream subtask 0 has 10× the data of subtask 1, `rescale()` will **not** fix it — its group stays overloaded. Use `rebalance()` then.

> **Key idea:** `rescale()` = "spread out locally, keep it cheap". `rebalance()` = "spread out globally, pay for the network". Reach for `rescale()` first when the parallelisms are proportional and the imbalance is not upstream.

---

## The rest of the family

```java
events.broadcast();      // every record to EVERY downstream subtask
events.shuffle();        // random target, uniform distribution
events.global();         // everything to subtask 0
events.forward();        // explicit one-to-one (requires equal parallelism)
events.partitionCustom(partitioner, keySelector);
```

### `broadcast()`

```
BROADCAST
  src[0] ──► op[0], op[1], op[2], op[3]     each record is DUPLICATED
  src[1] ──► op[0], op[1], op[2], op[3]     to every subtask
```

Total volume becomes `records × downstream_parallelism`. Only ever use for **small** streams: config updates, rule sets, lookup tables. The real use is a `BroadcastStream` connected to a main stream via `connect()` with a `BroadcastProcessFunction` (Phase 6) — that is how you push dynamic rules into a running job.

### `shuffle()`

Random target per record, drawn uniformly. Statistically even over many records; `rebalance()` is deterministically even and generally preferred. `shuffle()` is useful when you specifically want randomness (sampling, breaking a pathological pattern).

### `global()`

Everything to subtask 0. A deliberate bottleneck. Legitimate uses: a final single-threaded write, an ordered output, a global counter. `windowAll` implies this.

### `partitionCustom()`

```java
import org.apache.flink.api.common.functions.Partitioner;

// Route by geographic region so region-local subtasks handle
// region-local data (data locality by design).
DataStream<Event> routed = events.partitionCustom(
        new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                // MUST return 0 <= n < numPartitions
                if (key.startsWith("EU-")) return 0;
                if (key.startsWith("US-")) return 1 % numPartitions;
                return Math.abs(key.hashCode()) % numPartitions;
            }
        },
        (Event e) -> e.userId);
```

Two arguments: a `Partitioner<K>` that maps a key to a subtask index, and a `KeySelector` extracting the key from the record. It does **not** produce a `KeyedStream`, so no keyed state.

---

## Summary table

| Method | Pattern | Network | Typical use |
|---|---|---|---|
| `forward()` (default) | i → i | none | Equal parallelism; enables chaining |
| `keyBy(...)` | hash | full | Keyed state, windows, joins |
| `rebalance()` | round robin, global | full | Fix skew, unequal parallelism |
| `rescale()` | round robin, local subset | often none | Proportional fan-out/in |
| `broadcast()` | 1 → all (duplicated) | full ×p | Small config/rule streams |
| `shuffle()` | random | full | Randomisation |
| `global()` | all → subtask 0 | full | Deliberate serialization point |
| `partitionCustom()` | your function | full | Custom routing/locality |

---

## When you actually need `rebalance()`

### Case 1 — a filter that drops unevenly

```java
DataStream<Event> events = env.fromSource(kafkaSource, ws, "kafka")
        .setParallelism(4);

// Kafka partitions are keyed by region. Partition 0 is EU (dense),
// partition 3 is a tiny test region.
DataStream<Event> purchases = events.filter(e -> "purchase".equals(e.type));
```

```
BEFORE the filter (source partitions are already uneven):
  src[0]: 1,000,000 events    src[2]: 500,000
  src[1]:   800,000 events    src[3]:  10,000

AFTER the filter, FORWARD keeps the imbalance:
  proc[0]: 300,000    proc[2]: 150,000
  proc[1]: 240,000    proc[3]:   3,000   ← idle 99% of the time

  proc[0] takes 100× longer than proc[3]. Watermarks advance at the
  speed of the slowest subtask. Throughput is capped by proc[0].
```

```java
// FIX: force even distribution after the filter.
DataStream<Event> purchases = events
        .filter(e -> "purchase".equals(e.type))
        .rebalance()                        // ← full shuffle, now even
        .map(new ExpensiveEnrichment());
```

Now every enrichment subtask gets ~173,000 records. Do this **only if the downstream work is expensive** — a shuffle costs serialization + network, and rebalancing before a cheap `map` is a net loss.

### Case 2 — fewer Kafka partitions than parallelism

```java
// Topic has 3 partitions. Parallelism is 12.
DataStream<String> raw = env.fromSource(source, ws, "kafka").setParallelism(12);
```

```
  source[0]  reads partition 0    ┐
  source[1]  reads partition 1    ├─ 3 subtasks doing all the work
  source[2]  reads partition 2    ┘
  source[3..11]  read NOTHING     ← 9 idle subtasks

FORWARD then means: parse[3..11] also do nothing.
Your 12-way parallel enrichment is really 3-way.
```

```java
DataStream<Event> parsed = raw
        .rebalance()                        // spread 3 → 12
        .map(new ParseAndEnrich())
        .setParallelism(12);
```

Better still: **set the source parallelism to the partition count** and rebalance after it, so you do not deploy nine dead subtasks and nine idle-watermark problems:

```java
env.fromSource(source, ws, "kafka")
   .setParallelism(3)          // matches the topic
   .rebalance()
   .map(new ParseAndEnrich())
   .setParallelism(12);
```

And whenever some subtasks may read nothing, remember `withIdleness(...)` on the `WatermarkStrategy` (chapter 25), or the idle subtasks freeze the watermark at `Long.MIN_VALUE` and no window ever fires.

### Case 3 — highly variable per-record cost

If records vary wildly in processing cost (a 2 KB JSON vs a 2 MB JSON), round-robin still gives even *counts* but uneven *work*. `rebalance()` helps statistically; if it does not, you need a cost-aware `partitionCustom` or an async operator.

---

## The cost of a shuffle

Every non-forward edge means, per record:

```
 upstream subtask
   ├─ serialize the record to bytes         (CPU)
   ├─ copy into a network buffer            (memory)
   ├─ buffer fills OR the timeout fires     (latency: buffer timeout, default 100 ms)
   ├─ TCP to the target TaskManager         (network)
 downstream subtask
   ├─ read from the buffer                  (memory)
   └─ deserialize into an object            (CPU + GC pressure)
```

Concretely:

- Serialization/deserialization is often **the single largest CPU cost** in a Flink job.
- It creates garbage → GC pressure → latency spikes.
- It breaks the operator chain, so you also lose the method-call optimisation.
- Buffer timeout adds latency (`env.setBufferTimeout(ms)`; lower = lower latency, worse throughput).

Rules:

1. **Do not add `rebalance()` reflexively.** Measure the imbalance in the Web UI first (per-subtask `numRecordsIn`).
2. **Rebalance before expensive work, never before cheap work.**
3. **Filter as early as possible** — before any shuffle — so you shuffle less.
4. **Keep parallelism uniform** so edges stay FORWARD and chains stay intact.

---

## Data skew on `keyBy`

`rebalance()` cannot help here. `keyBy` must send equal keys to the same subtask — that is the contract keyed state depends on. If one key is hot, one subtask is hot.

```
keyBy(e -> e.userId), and one bot account is 60% of traffic:

  window[0]:  60,000,000 records   ████████████████████  <-- the bot
  window[1]:   8,000,000 records   ███
  window[2]:   7,500,000 records   ██
  window[3]:   8,200,000 records   ███

  Symptoms in the Web UI:
   - window[0] busy ~100%, others ~15%
   - backpressure upstream of window
   - checkpoints slow: the barrier waits for window[0]
   - window[0]'s state is huge; its TaskManager may OOM
```

### Detecting it

The Web UI's subtask table for the operator: compare `numRecordsIn` across subtasks, and the Busy % column. **Max/median > 5 is a skew problem** — the same rule of thumb as Spark.

Or count in code:

```java
events.map(e -> Tuple2.of(e.userId, 1L))
      .returns(Types.TUPLE(Types.STRING, Types.LONG))
      .keyBy(t -> t.f0)
      .sum(1)
      .filter(t -> t.f1 > 1_000_000)     // suspiciously hot keys
      .print();
```

`.returns(...)` supplies the `TypeInformation` for a lambda, whose generic types are erased — same problem as chapter 27, different syntax.

### Mitigation 1 — two-phase aggregation with salted keys

The standard fix, and it is the same idea as Spark's salting. Split each hot key into *N* artificial sub-keys, aggregate locally, then combine.

```
ONE PHASE (skewed)                TWO PHASE (balanced)
  u_bot ──► subtask 0               u_bot#0 ──► subtask 0 ─┐
  u_bot ──► subtask 0               u_bot#1 ──► subtask 1 ─┤
  u_bot ──► subtask 0               u_bot#2 ──► subtask 2 ─┼─► keyBy(u_bot)
  u_bot ──► subtask 0               u_bot#3 ──► subtask 3 ─┘   final combine
    all on one subtask                spread across 4         (tiny volume:
                                                               4 partials, not
                                                               60,000,000 records)
```

```java
import java.util.concurrent.ThreadLocalRandom;

final int SALT = 16;   // fan-out factor for the first phase

DataStream<Tuple2<String, Double>> partials = events
        // PHASE 1 KEY: append a random salt to spread the hot key.
        .map(e -> Tuple2.of(
                e.userId + "#" + ThreadLocalRandom.current().nextInt(SALT),
                e.amount))
        .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
        .keyBy(t -> t.f0)                                  // 16× more keys
        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
        .sum(1);                                           // partial sums

DataStream<Tuple2<String, Double>> totals = partials
        // PHASE 2: strip the salt back off and combine the partials.
        .map(t -> Tuple2.of(t.f0.substring(0, t.f0.lastIndexOf('#')), t.f1))
        .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
        .keyBy(t -> t.f0)
        .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
        .sum(1);
```

Phase 2 still routes all of `u_bot` to one subtask — but it now receives **16 partial sums per window** instead of 60 million records. That is trivial.

Caveats:

- **Only works for associative, commutative aggregations**: sum, count, min, max, and sketches like HyperLogLog. It does **not** work for "the last event", median, or anything order-dependent.
- Windows must line up. Both phases use the same window definition so partials from one window combine into one final result.
- `lastIndexOf('#')` not `indexOf` — user ids may contain `#`. Better still, use a separator you have guaranteed cannot appear, or carry the key in a proper field.

### Mitigation 2 — salt only the hot keys

Salting everything multiplies your key count and state size by *N*. Salt selectively:

```java
// A known set of hot keys, or one loaded via a broadcast stream so it
// can be updated without a redeploy.
private static final Set<String> HOT = Set.of("bot-1", "scraper-7");

.map(e -> Tuple2.of(
        HOT.contains(e.userId)
                ? e.userId + "#" + ThreadLocalRandom.current().nextInt(SALT)
                : e.userId,                        // cold keys untouched
        e.amount))
```

Cold keys skip phase 1 entirely (their "partial" is already the total), so state stays small.

### Mitigation 3 — pre-aggregate before the shuffle

If the aggregation is associative, reduce locally **before** the `keyBy` so far fewer records cross the network:

```java
// A processing-time tumbling pre-aggregation per source subtask,
// then keyBy on the much smaller partial stream.
events.windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(1)))
      // ... local combine ...
```

More commonly this is expressed with a `ReduceFunction` in the window (Flink's incremental window aggregation already avoids buffering all records) or with `AggregateFunction`, which is why using `reduce`/`aggregate` instead of `process` on a window matters so much for skew.

### Mitigation 4 — fix the key

Sometimes the honest answer is that the key is wrong. `keyBy(userId)` where one "user" is an internal service account is a modelling bug. Filter the bot out, or key by `(userId, sessionId)`, or route it to a separate pipeline.

> **Key idea:** `rebalance()` fixes **subtask** imbalance. It cannot fix **key** imbalance, because `keyBy` must be deterministic. Key skew is fixed by changing the key: salt it, split the aggregation into two phases, or model it differently.

---

## Remember

- Same parallelism, no partitioner → **forward**, free, chainable. Different parallelism → **rebalance** automatically.
- `keyBy` = **hash partition** through key groups; same key always lands on the same subtask. That is the basis of keyed state *and* of skew.
- `rebalance()` = global round robin, full mesh, always network.
- `rescale()` = round robin within a local subset, `max(up,down)` connections, often no network. Use when parallelisms are proportional.
- `broadcast()` duplicates every record to every subtask — small streams only.
- `global()` = everything to subtask 0 = an intentional bottleneck.
- Rebalance after an **uneven filter** or when the **source has fewer partitions than your parallelism** — and only when the downstream work is expensive enough to pay for the shuffle.
- A shuffle costs serialize + buffer + network + deserialize, breaks chaining, and adds buffer-timeout latency.
- Skew detection: per-subtask `numRecordsIn` and Busy % in the UI. **max/median > 5** = problem.
- Key skew fix: **two-phase aggregation with salted keys**, ideally salting only the known hot keys. Requires an associative, commutative aggregate.

**Interview one-liners**

- *"rebalance vs rescale?"* → Both round-robin. Rebalance is a global full mesh across all downstream subtasks; rescale distributes only within a local subset, so far fewer connections and often no network hop. Rescale won't fix upstream imbalance.
- *"What's the default partitioning?"* → Forward when parallelism matches; rebalance when it doesn't.
- *"When do you add rebalance()?"* → After a filter that drops unevenly, or when the source has fewer partitions than the downstream parallelism — and only before expensive work.
- *"Cost of a shuffle?"* → Serialization on both sides, network transfer, GC pressure, buffer-timeout latency, and it breaks operator chaining.
- *"How do you fix skew on keyBy?"* → You can't rebalance it away. Two-phase aggregation: salt the hot keys to spread phase one, strip the salt and combine tiny partials in phase two. Only valid for associative/commutative aggregates.
- *"How do you detect skew?"* → Per-subtask record counts and busy time in the Web UI; max/median > 5.

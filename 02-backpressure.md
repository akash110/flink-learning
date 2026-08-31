# Backpressure — Finding the Real Bottleneck

> Typical question: *"Your job is lagging. Kafka consumer lag is growing. How do you find
> where the problem is?"*

Backpressure is the root cause behind most streaming incidents, including slow
checkpointing ([[01-checkpointing-slow]]). This note is about **locating** it.

---

## The mental model

Flink uses **credit-based flow control**. A downstream task tells upstream "I have N free
buffers." Upstream only sends N. When downstream is slow, it stops issuing credits,
upstream's output queue fills, and the stall propagates **backwards** to the source.

```
Source ← Map ← Window ← Sink
                          ↑ ACTUALLY SLOW (e.g. sink can't keep up)

Observed backpressure:
Source:  HIGH  ← backpressured
Map:     HIGH  ← backpressured
Window:  HIGH  ← backpressured
Sink:    LOW   ← NOT backpressured  ← ⭐ THIS IS THE CULPRIT
```

## The one rule that answers the question

> **The bottleneck is the FIRST operator (going downstream) that is NOT backpressured.**

Everything upstream of it shows red. The guilty operator itself shows green, because nothing
downstream is holding *it* back — it's just slow on its own. People instinctively blame the
red operators. The red ones are victims.

In the Flink UI: `Backpressured` and `Busy` percentages per operator.
- **Busy ~100%, Backpressured ~0%** → this operator is the bottleneck. It's working flat out.
- **Backpressured ~100%** → it's waiting on someone downstream. Not your problem.
- **Busy low, Backpressured low, but lag growing** → the *source* is the limit (not enough
  Kafka partitions, or a slow external source).

That last case is worth remembering — it's the one where nothing looks wrong but throughput
is still capped.

---

## Cause 1: Blocking I/O in the hot path

Covered in [[01-checkpointing-slow]] Part 2. The arithmetic:

```
per-subtask throughput = 1 / latency_seconds
required parallelism   = target_throughput × latency_seconds
```

```java
// 1M events/sec target, 20ms lookup
// required parallelism = 1_000_000 × 0.020 = 20,000 subtasks. Absurd.
// → you cannot solve this with parallelism. Architecture must change.
```

Options in order of preference:
1. **AsyncIO** — 20k in-flight requests instead of 20k threads
2. **Local cache / broadcast state** — don't make the call at all
3. **Pre-join upstream** — enrich in Kafka Streams / at write time
4. **Bloom filter** — skip the lookup for the 95% of keys that will miss

---

## Cause 2: Slow sink

```java
// ❌ per-record synchronous write
public class SlowSink extends RichSinkFunction<Result> {
    @Override
    public void invoke(Result r, Context ctx) throws Exception {
        httpClient.post("/api/results", r);   // 50ms round trip = 20 rec/sec
    }
}
```

```java
// ✅ batch + async
public class BatchingSink extends RichSinkFunction<Result>
        implements CheckpointedFunction {

    private transient List<Result> buffer;
    private transient ListState<Result> checkpointed;

    @Override
    public void invoke(Result r, Context ctx) throws Exception {
        buffer.add(r);
        if (buffer.size() >= 1000) flush();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        flush();                      // ⚠️ MUST flush before the checkpoint completes,
        checkpointed.clear();         //    or you lose data on restore
        checkpointed.addAll(buffer);
    }
}
```

**Gotcha:** if you buffer in a sink and *don't* flush in `snapshotState`, you have silently
broken exactly-once. The checkpoint says "I processed record X" while X is still in a heap
buffer that vanishes on restart. This is a very popular interview trap.

---

## Cause 3: Serialization

Quietly enormous at 1M events/sec.

```java
// ❌ Kryo fallback — Flink can't treat this as a POJO
public class Event {
    private Map<String, Object> attributes;   // Object → Kryo
    private LocalDateTime timestamp;          // no default Kryo support
    public Event(String id) { ... }           // ⚠️ no no-arg constructor → NOT a POJO
}
```

Kryo is roughly **10x slower** than Flink's POJO serializer. At 1M/sec that alone can be your
bottleneck.

```java
// Detect it — fail loudly instead of silently degrading
env.getConfig().disableGenericTypes();   // throws if anything falls back to Kryo
```

POJO requirements: public class, **public no-arg constructor**, all fields public or with
getter+setter, and field types themselves serializable by Flink.

```java
// ✅ best at high throughput — Avro/Protobuf with a schema
public class Event extends SpecificRecordBase { ... }
```

---

## Cause 4: The `rebalance()` you didn't need

```java
// ❌ full network shuffle — every record serialized + sent over the wire
stream.rebalance().map(new Transform());
```

If `map` is stateless, this shuffle is pure waste. Flink can **chain** the operators into
one thread with zero serialization — but only for `forward` connections.

```java
// ✅ chained: no network, no serialization
stream.map(new Transform());
```

Watch for accidental shuffles: `keyBy`, `rebalance`, `shuffle`, `rescale`, `broadcast`, and
**any change in parallelism between operators**. That last one is sneaky — setting a
different `setParallelism()` on one operator inserts a shuffle you didn't ask for.

```java
// disable chaining only when you deliberately want operators on separate threads
// (e.g. to isolate a slow operator for profiling)
stream.map(new A()).disableChaining().map(new B());
```

---

## Diagnostic checklist

```
1. Flink UI → find first non-backpressured operator going downstream ← the culprit
2. Check its Busy %.  ~100% = CPU/logic bound.  Low = waiting on external I/O.
3. Check GC: long pauses look exactly like backpressure
     jstat -gcutil <pid> 1000
     → full GCs every few seconds = heap problem, not logic problem
4. Check for skew: Flink UI → subtask list → compare "Records Received" per subtask
     one subtask 100x the others = hot key (see [[03-state-and-skew]])
5. Check serialization: env.getConfig().disableGenericTypes() in a test run
6. Check the sink: is it batching? is it async? what's its p99?
```

Step 3 deserves emphasis. **GC pauses are indistinguishable from backpressure in the UI.**
A 30-second full GC looks like a stuck operator. Always rule out GC before you go
restructuring the job.

See also: [[01-checkpointing-slow]], [[03-state-and-skew]], [[06-scale-arithmetic]]

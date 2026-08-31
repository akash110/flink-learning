# 45. Joins and Enrichment

Three ways to combine a stream with something else, in the order you should reach for them.

| You want to combine | Use |
|---|---|
| Two streams, matched within a time interval around each event | **`intervalJoin`** |
| Two streams, matched within the same fixed window | **windowed join** |
| A stream with an **external system** (REST, DB, cache) | **Async I/O** |

Chapter 40's `KeyedCoProcessFunction` is the fourth option — the escape hatch when none of these fit.

> **Key idea**
> A stream-to-stream join has to be time-bounded, because otherwise Flink would have to remember every record of both sides forever. The time bound *is* the state-cleanup policy.

---

# Part 1 — Interval join

## The API

```java
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

DataStream<Enriched> joined =
    orders.keyBy(o -> o.userId)                         // both sides keyed the SAME way
          .intervalJoin(payments.keyBy(p -> p.userId))
          .between(Duration.ofMinutes(-5), Duration.ofMinutes(5))
          .process(new ProcessJoinFunction<Order, Payment, Enriched>() {
              @Override
              public void processElement(Order left,
                                         Payment right,
                                         Context ctx,
                                         Collector<Enriched> out) {
                  // Called once PER MATCHED PAIR.
                  out.collect(new Enriched(left, right));

                  // ctx.getLeftTimestamp() / getRightTimestamp() / getTimestamp()
              }
          });
```

## The semantics, precisely

For each element `L` of the left stream, it is joined with every element `R` of the right stream where:

```
L.timestamp + lowerBound  <=  R.timestamp  <=  L.timestamp + upperBound
```

With `between(Duration.ofMinutes(-5), Duration.ofMinutes(5))`:

```
                       L (an order at 10:00)
                              │
   ──────────────────────────●──────────────────────────►  event time
        09:55                10:00                10:05
          ├────────────── join window ──────────────┤
          │                                         │
      lowerBound                               upperBound
      L.ts - 5min                              L.ts + 5min

   Any payment with a timestamp in [09:55, 10:05] joins with this order.
```

Bounds can be asymmetric, and both can be on the same side of zero:

```java
// "the payment must come AFTER the order, within 10 minutes"
.between(Duration.ZERO, Duration.ofMinutes(10))

// "the click must have happened BEFORE the purchase, within an hour"
.between(Duration.ofHours(-1), Duration.ZERO)
```

Bounds are inclusive by default; exclude them with `.lowerBoundExclusive()` / `.upperBoundExclusive()`.

## Five things that surprise people

**1. Inner join only.** An element with no match on the other side produces **nothing**. There is no left/outer interval join in the DataStream API. If you need outer-join semantics — "emit the order even if no payment arrived" — you must hand-roll it with `KeyedCoProcessFunction` (ch. 40) or use Flink SQL, which does support outer interval joins.

This is the number-one reason people abandon `intervalJoin` for a hand-rolled version: silent data loss on unmatched records is usually unacceptable in a payments or order pipeline.

**2. Event time only.** `intervalJoin` requires timestamps and watermarks on both streams. Processing time is not supported.

**3. It's symmetric.** Every element of both streams is buffered, and both sides trigger matching:

```
An order arrives  -> look back in the payment buffer for matches
A payment arrives -> look back in the order   buffer for matches
```

So each pair is emitted exactly once, whichever side arrives second.

**4. State cleanup is automatic and bounded by the interval.** Flink keeps a `MapState<Long, List<T>>` per side, keyed by timestamp, and registers a cleanup timer. A left element is discarded once the watermark exceeds `L.ts + upperBound`; a right element once the watermark exceeds `R.ts - lowerBound`.

```
STATE HELD ≈ (input rate of both streams) × (upperBound − lowerBound)
```

Concretely: 50 000 events/s combined, a 10-minute window, ~200 bytes per event → 50 000 × 600 × 200 ≈ **6 GB**. Interval joins are one of the most state-hungry operators in Flink. Size the interval as tightly as the business allows, and use RocksDB.

**5. Late elements are dropped silently.** An element arriving after the watermark has passed its join window is discarded with no side output. There is no `sideOutputLateData` on `intervalJoin`. If you need to see them, filter for lateness upstream.

## Duplicate fan-out

The join is a **cross product within the interval**. If a user has 3 orders and 4 payments all inside one interval, you get **12** output records.

```
orders:    O1  O2  O3         all within ±5 min of each other and of the payments
payments:  P1  P2  P3  P4

output:    O1P1 O1P2 O1P3 O1P4
           O2P1 O2P2 O2P3 O2P4     -> 12 rows
           O3P1 O3P2 O3P3 O3P4
```

If you wanted "each order matched to its one payment", you need a stronger key (join on `orderId`, not `userId`) or dedup logic in the `ProcessJoinFunction`. This is the second-most-common interval-join bug.

---

# Part 2 — Windowed join

Different semantics: both elements must fall in **the same window**, rather than within an interval of each other.

```java
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.api.common.functions.JoinFunction;

DataStream<Enriched> joined =
    orders.join(payments)
          .where(o -> o.userId)          // key selector for the LEFT stream
          .equalTo(p -> p.userId)        // key selector for the RIGHT stream
          .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
          .apply(new JoinFunction<Order, Payment, Enriched>() {
              @Override
              public Enriched join(Order o, Payment p) {
                  return new Enriched(o, p);     // called once per matched pair
              }
          });
```

Note the different shape: `.where()/.equalTo()` instead of two `keyBy`s, `.apply()` instead of `.process()`.

## Interval join vs windowed join

```
INTERVAL JOIN — the window travels WITH each element
      L1        L2
  ────●─────────●────────►
   [────]     [────]           each L gets its own ±bound window

WINDOWED JOIN — fixed grid, elements fall into buckets
  [─── W1 ───][─── W2 ───]
   L1  R1      L2      R2
   ✅ L1×R1               ❌ L2 and R2 are in the same window W2 -> ✅ actually match
   
  the failure mode:
  [─── W1 ───][─── W2 ───]
          L1 │ R1
             └─ 1 second apart, but the boundary splits them -> NO MATCH
```

> **Key idea**
> The windowed join's fatal flaw is the **boundary problem**: two events one second apart never match if a window boundary falls between them. The interval join has no boundaries, so it doesn't have this problem.

| | Interval join | Windowed join |
|---|---|---|
| Matching rule | R within `[L.ts+lower, L.ts+upper]` | both in the same window |
| Boundary artifacts | none | **yes** — adjacent events can miss |
| Join type | inner only | inner only (`coGroup` for outer) |
| Time semantics | event time only | event or processing time |
| State | per-element buffers + cleanup timers | window contents |
| Typical use | correlating related events (order↔payment) | comparing aggregates per period |

For "match these two related events", **use the interval join**. Windowed joins are mostly right when the window itself is meaningful (e.g. "impressions and clicks per 5-minute reporting bucket").

## `coGroup` — the outer-join escape hatch

```java
orders.coGroup(payments)
      .where(o -> o.userId).equalTo(p -> p.userId)
      .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
      .apply(new CoGroupFunction<Order, Payment, Result>() {
          @Override
          public void coGroup(Iterable<Order> os, Iterable<Payment> ps, Collector<Result> out) {
              // Called once per key per window, with BOTH sides' elements —
              // and either Iterable may be EMPTY. That's what makes outer joins possible.
              boolean hasPayments = ps.iterator().hasNext();
              for (Order o : os) {
                  if (!hasPayments) out.collect(Result.unmatched(o));   // left outer
                  else for (Payment p : ps) out.collect(Result.matched(o, p));
              }
          }
      });
```

`coGroup` is the only built-in DataStream way to get outer-join semantics. It still has the boundary problem.

---

# Part 3 — Async I/O (the one you'll actually need most)

## The problem

You need data that isn't in either stream: a user profile from a REST API, a merchant record from DynamoDB, a risk score from an internal service.

The obvious code is a disaster:

```java
// NEVER DO THIS.
public class BlockingEnrich extends RichMapFunction<Event, Enriched> {
    private transient HttpClient client;

    @Override
    public Enriched map(Event e) throws Exception {
        Profile p = client.get("/profile/" + e.userId);   // blocks ~10 ms
        return new Enriched(e, p);
    }
}
```

```
One subtask, one thread, 10 ms per call:
   throughput = 1000 / 10 = 100 records/second   PER SUBTASK

To handle 100 000 rec/s you'd need parallelism 1000.
And your subtask thread is idle 99.9% of the time — waiting on a socket.
```

The thread isn't computing. It's *waiting*. Async I/O lets one thread have hundreds of requests in flight simultaneously.

```
SYNCHRONOUS                              ASYNCHRONOUS (capacity = 100)
  req1 ──wait 10ms──► resp1               req1 ┐
  req2 ──wait 10ms──► resp2               req2 ├─ all in flight together
  req3 ──wait 10ms──► resp3               ...  │
  ...                                     req100┘
  30 ms for 3 records                     ~10 ms for 100 records
```

## The API

```java
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.concurrent.TimeUnit;

DataStream<Enriched> enriched = AsyncDataStream.unorderedWait(
        input,                          // the stream
        new ProfileLookup(),            // your AsyncFunction
        200, TimeUnit.MILLISECONDS,     // TIMEOUT per request
        100);                           // CAPACITY: max in-flight requests per subtask
```

Two variants:

| | `unorderedWait` | `orderedWait` |
|---|---|---|
| Emits a result | as soon as it's ready | only in the input order |
| Latency | **lower** — a fast result isn't stuck behind a slow one | higher — head-of-line blocking |
| Buffer | smaller | holds completed results waiting for earlier ones |
| Use when | order doesn't matter (the common case) | downstream requires input order |

**Important nuance:** "unordered" does not mean "no ordering at all" in event time. With event time, `unorderedWait` still emits all results between two watermarks before letting the watermark pass — so ordering is preserved *across* watermarks, just not within them. Watermarks are never overtaken.

Start with `unorderedWait`. Only use `orderedWait` if a downstream operator genuinely depends on order — and note that if you `keyBy` afterwards anyway, order across keys is already irrelevant.

## `RichAsyncFunction`

```java
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class ProfileLookup extends RichAsyncFunction<Event, Enriched> {

    /**
     * `transient` = don't serialize this field when shipping the function to the
     * cluster. Clients are not serializable, so they must be built in open().
     */
    private transient HttpClient client;

    @Override
    public void open(Configuration parameters) {
        // RichAsyncFunction still uses the older open(Configuration) signature.
        // ONE client per subtask, with its own connection pool and thread pool.
        client = HttpClient.newBuilder().build();
    }

    @Override
    public void close() throws Exception {
        // Release resources. HttpClient in modern Java is auto-closing; a JDBC
        // pool or Kafka producer would need an explicit close here.
    }

    @Override
    public void asyncInvoke(Event e, ResultFuture<Enriched> resultFuture) {

        // ── THE RULE: this method MUST RETURN IMMEDIATELY. ────────────────
        // It runs on the operator's main thread. Blocking here blocks the
        // ENTIRE subtask — every record, every checkpoint barrier.

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://profiles/api/" + e.userId))
                .build();

        // sendAsync returns a CompletableFuture immediately. No blocking.
        CompletableFuture<HttpResponse<String>> future =
                client.sendAsync(req, HttpResponse.BodyHandlers.ofString());

        future
            // thenAccept registers a CALLBACK: "when the response arrives, run this".
            // It runs on the HTTP client's thread, not the operator thread.
            .thenAccept(response -> {
                Profile p = Profile.parse(response.body());

                // complete() hands the result back to Flink. It takes a COLLECTION
                // because one input may produce zero, one, or many outputs.
                resultFuture.complete(Collections.singleton(new Enriched(e, p)));
            })
            // exceptionally handles a failed future. WITHOUT THIS, the future
            // never completes, the request occupies capacity until the timeout,
            // and you leak throughput.
            .exceptionally(ex -> {
                resultFuture.completeExceptionally(new RuntimeException("lookup failed", ex));
                return null;
            });
    }

    @Override
    public void timeout(Event e, ResultFuture<Enriched> resultFuture) {
        // Called when the request exceeds the timeout passed to unorderedWait.
        //
        // DEFAULT BEHAVIOUR IF YOU DON'T OVERRIDE THIS: the job FAILS with a
        // TimeoutException. That is almost never what you want in production.
        //
        // Choose one:
        resultFuture.complete(Collections.singleton(new Enriched(e, Profile.UNKNOWN)));  // degrade
        // resultFuture.complete(Collections.emptyList());        // drop the record
        // resultFuture.completeExceptionally(new TimeoutException()); // fail the job
    }
}
```

## The rules, stated bluntly

**1. NEVER block in `asyncInvoke`.**

```java
// ALL WRONG — each of these blocks the operator thread:
Profile p = future.get();                    // ❌ blocking get
Thread.sleep(100);                           // ❌
Profile p = jdbcConnection.query(...);       // ❌ synchronous JDBC
resultFuture.complete(...);                  // (fine on its own, but not after a block)
```

If your client library has **no async API** (JDBC is the classic case), you must submit the work to your own `ExecutorService` and complete the future from there:

```java
private transient ExecutorService pool;   // built in open(), e.g. Executors.newFixedThreadPool(20)

@Override
public void asyncInvoke(Event e, ResultFuture<Enriched> resultFuture) {
    // supplyAsync returns immediately; the blocking work happens on `pool`.
    CompletableFuture
        .supplyAsync(() -> blockingJdbcLookup(e.userId), pool)
        .thenAccept(p -> resultFuture.complete(Collections.singleton(new Enriched(e, p))))
        .exceptionally(ex -> { resultFuture.completeExceptionally(ex); return null; });
}
```

Size that pool at least as large as `capacity`, or you've just reintroduced the bottleneck.

**2. `resultFuture` must be completed exactly once, always.** Every path — success, failure, timeout — must call `complete` or `completeExceptionally`. A future that is never completed occupies a capacity slot until the timeout expires. Enough of those and throughput collapses. The missing `.exceptionally(...)` is the most common cause.

**3. Never call `resultFuture.complete` more than once for the same record.** The second call is ignored, but it's a sign of a logic bug.

**4. Override `timeout`.** The default fails the whole job. Decide: degrade to a default, drop the record, or genuinely fail — but decide deliberately.

**5. Don't use keyed state inside `asyncInvoke`.** The callback runs on a different thread with no keyed context. `AsyncFunction` is not thread-safe for state access, and Flink does not synchronise it. Do stateful work in an operator before or after the async one.

## Choosing `capacity` and `timeout`

```
capacity = target throughput per subtask × average latency

Example: you want 5000 rec/s per subtask, the service responds in 20 ms:
   capacity = 5000 × 0.020 = 100
```

`capacity` is also **backpressure**: when `capacity` requests are in flight, `asyncInvoke` stops being called and backpressure propagates upstream. That's the desired behaviour — it stops you from DDoSing your own service.

- **Too low** → throughput ceiling, unnecessary backpressure.
- **Too high** → you overwhelm the downstream service, and each in-flight request is buffered in checkpointed state, so checkpoints grow.

`timeout` should be comfortably above the service's p99 latency. Too tight and you convert normal slow responses into timeouts; too loose and a hung service holds capacity slots for a long time.

## Checkpointing

In-flight async requests are **part of the checkpoint**. On restore, Flink replays them by calling `asyncInvoke` again. Two consequences:

- Your lookup should be **idempotent** — a read is; a write is not. Don't do side-effecting writes in `asyncInvoke`.
- Large `capacity` means a larger checkpoint. Another reason not to set it to 10 000.

## The alternative you should consider first

Before reaching for async I/O, ask whether the reference data is small enough to keep **in the job**:

| Reference data | Approach | Latency |
|---|---|---|
| Tens/hundreds of rules | **Broadcast state** (ch. 41) | ~0 |
| Millions of records, keyed, available as a stream | **`KeyedCoProcessFunction`** (ch. 40) | ~0 |
| Huge, or owned by another team, only available via API | **Async I/O** | 1–50 ms + failure modes |

Async I/O introduces an external dependency into your streaming job: its outages become your outages, its latency becomes your latency, and its rate limits become your throughput ceiling. **It is the last resort, not the first.**

If you must use it, add a local cache in front:

```java
@Override
public void asyncInvoke(Event e, ResultFuture<Enriched> resultFuture) {
    Profile cached = localCache.getIfPresent(e.userId);   // e.g. Caffeine, per-subtask
    if (cached != null) {
        resultFuture.complete(Collections.singleton(new Enriched(e, cached)));
        return;                                           // no network call at all
    }
    // ... async path, and populate the cache in the callback
}
```

A per-subtask cache with a size bound and a TTL routinely takes 90%+ of the traffic off the external service. It's plain-Java state, not Flink state — so it isn't checkpointed, and it's cold after a restart. That's an acceptable trade for a cache.

---

## Remember

- `intervalJoin`: `L.ts + lower <= R.ts <= L.ts + upper`, event time only, **inner join only**, late elements dropped silently.
- Interval-join state ≈ combined input rate × interval width. Keep the interval tight; use RocksDB.
- The interval join is a cross product within the interval — 3 lefts × 4 rights = 12 outputs. Key tightly.
- Windowed join (`join/where/equalTo/window/apply`) matches only within the same window and therefore has a **boundary problem**. Prefer the interval join for correlating related events.
- `coGroup` is the only built-in DataStream route to outer-join semantics.
- Async I/O: `AsyncDataStream.unorderedWait(stream, fn, timeout, unit, capacity)` with a `RichAsyncFunction`.
- **Never block in `asyncInvoke`** — it runs on the operator thread. Use a real async client, or submit to your own `ExecutorService`.
- Always complete `resultFuture` exactly once on every path; always add `.exceptionally(...)`.
- Always override `timeout` — the default fails the job.
- `capacity` ≈ throughput × latency, and doubles as backpressure. In-flight requests are checkpointed, so keep lookups idempotent.
- `unorderedWait` for latency, `orderedWait` only when downstream needs input order. Watermarks are never overtaken by either.
- Prefer broadcast state or a `KeyedCoProcessFunction` over async I/O when the reference data can live in the job. Add a per-subtask cache when it can't.

## Interview one-liners

- *"How do you join two streams in Flink?"* → Interval join (each element matched within a time offset), windowed join (both in the same window), or a `KeyedCoProcessFunction` when you need custom semantics.
- *"Interval join vs windowed join?"* → The interval travels with each element so there are no boundary artifacts; a windowed join can miss two events one second apart if a boundary falls between them.
- *"Is `intervalJoin` an outer join?"* → No, inner only. Unmatched elements are dropped silently; use `coGroup` or a `KeyedCoProcessFunction` for outer semantics.
- *"How much state does an interval join hold?"* → Roughly combined input rate × interval width, buffered per side with automatic watermark-driven cleanup.
- *"How do you call a REST API from Flink?"* → Async I/O: `AsyncDataStream.unorderedWait` with a `RichAsyncFunction` that returns immediately and completes a `ResultFuture` from a callback.
- *"Why can't you just call the API in a `map`?"* → It blocks the operator thread, capping throughput at 1/latency per subtask and stalling checkpoint barriers.
- *"What does `capacity` do?"* → Caps in-flight requests per subtask; hitting it applies backpressure. Size it as target throughput × average latency.
- *"What happens on an async timeout?"* → `timeout()` is called; if you don't override it the job fails, so always implement a degrade-or-drop policy.
- *"Ordered vs unordered async?"* → Unordered emits as results arrive (lower latency); ordered preserves input order at the cost of head-of-line blocking. Neither lets a result overtake a watermark.
- *"When would you avoid async I/O entirely?"* → When the reference data fits in broadcast state or can be streamed and keyed — that keeps lookups local and removes an external failure dependency.

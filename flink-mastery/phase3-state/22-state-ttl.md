# 22. State TTL — Making State Expire

Every chapter so far ended with the same warning: state grows. This chapter is the fix.

## The problem, stated plainly

```java
events.keyBy(e -> e.userId).process(new RunningBalanceFn());
```

Innocent code. Now count keys:

```
Day   1: 10,000 users seen      ->      10,000 state entries
Day  30: 300,000 users seen     ->     300,000 state entries
Day 365: 3,000,000 users seen   ->   3,000,000 state entries
```

Ninety-five percent of those users never came back. Their `ValueState<Double>` sits in RocksDB forever, because **nothing in Flink ever deletes state on its own.**

Now make the key something worse:

```java
.keyBy(e -> e.sessionId)      // a new key every session
.keyBy(e -> e.requestId)      // a new key every request
.keyBy(e -> e.userId + ":" + e.deviceId)   // multiplies the keyspace
```

That's an **unbounded keyspace**: the number of distinct keys grows without limit as the stream runs. State grows linearly with it. Then:

```
state grows
   -> checkpoints get bigger and slower
      -> checkpoint duration exceeds the interval
         -> checkpoints start timing out
            -> a failure occurs, recovery must restore 400 GB
               -> recovery is slow, or disk fills
                  -> job dies, restarts, dies again
```

> **Key idea:** Unbounded keyspace with no expiry is the **#1 cause of production Flink jobs dying.** It's insidious because the job is perfectly healthy for weeks before it isn't.

Two fixes: `clear()` state explicitly when a key is finished (works only when you can *detect* "finished"), or attach a TTL so Flink expires it for you. TTL is the general answer.

## `StateTtlConfig` — the full anatomy

```java
import org.apache.flink.api.common.state.StateTtlConfig;
import java.time.Duration;

StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Duration.ofHours(24))                  // 1. how long
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 2. what resets it
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 3. reads
        .cleanupIncrementally(10, true)                     // 4. how it's removed
        .build();
```

Then attach it to a descriptor:

```java
ValueStateDescriptor<Double> desc = new ValueStateDescriptor<>("balance", Double.class);
desc.enableTimeToLive(ttlConfig);       // MUST be called before getState()
balanceState = getRuntimeContext().getState(desc);
```

`StateTtlConfig` uses the **builder pattern**: `newBuilder()` returns a builder, each `setX()` returns the same builder so calls chain, and `build()` produces the immutable config. Very common in Java.

Note `newBuilder(Duration)`. Older code uses `Time.hours(24)` (`org.apache.flink.api.common.time.Time`), which is deprecated. Use `java.time.Duration`.

### 1. The TTL duration

```java
.newBuilder(Duration.ofHours(24))
.newBuilder(Duration.ofMinutes(30))
.newBuilder(Duration.ofDays(7))
```

How long an entry lives after its last qualifying access. What counts as "qualifying" is `setUpdateType`.

### 2. `setUpdateType` — what resets the clock

```java
UpdateType.OnCreateAndWrite   // DEFAULT: create and update() reset the timer
UpdateType.OnReadAndWrite     // create, update() AND value() all reset the timer
UpdateType.Disabled           // TTL is off (the default when you don't call enableTimeToLive)
```

```
TTL = 10 minutes

OnCreateAndWrite:
  t=0    update()  -> expires at t=10
  t=3    value()   -> expires at t=10   (read does NOT extend)
  t=6    value()   -> expires at t=10
  t=10             -> EXPIRED, even though it was read at t=6

OnReadAndWrite:
  t=0    update()  -> expires at t=10
  t=3    value()   -> expires at t=13   (read extends)
  t=6    value()   -> expires at t=16
  t=10             -> still alive
```

Which to pick:

- **`OnCreateAndWrite`** = "expire N after last *modification*". Right for "delete a user's balance 24h after their last transaction". It's the default and usually correct.
- **`OnReadAndWrite`** = "expire N after last *access*", i.e. LRU semantics. Right for caches and lookup tables that stay hot by being read.

The cost of `OnReadAndWrite` is that **every read becomes a write** (it must persist the new expiry timestamp). On RocksDB that turns a cheap read into a read plus a write. Don't reach for it by default.

### 3. `setStateVisibility` — what reads return for expired-but-present data

```java
StateVisibility.NeverReturnExpired          // DEFAULT: expired data is invisible
StateVisibility.ReturnExpiredIfNotCleaned   // return it if it's physically still there
```

The subtlety: expiry is logical. An entry's timestamp says it expired at 10:00, but the bytes may still sit in RocksDB until cleanup runs.

```
                  entry expired at 10:00, physically deleted at 10:07
                  read at 10:03 ──┐
                                  v
NeverReturnExpired          -> returns null.  Deterministic. Correct.
ReturnExpiredIfNotCleaned   -> returns the value.  Non-deterministic — depends
                               entirely on cleanup timing.
```

`NeverReturnExpired` also carries a real guarantee: expired values are never handed back, which matters for compliance ("we must not use data older than 30 days"). It costs one timestamp comparison per read.

`ReturnExpiredIfNotCleaned` is best-effort caching semantics — "stale is better than nothing". Use it only when you genuinely don't care, and know that your results become non-reproducible.

**Default to `NeverReturnExpired`.**

### 4. Cleanup strategies — how bytes actually get removed

Logical expiry is free. Reclaiming the space is not. Choose how.

Every strategy has one thing in common: **expired entries are always removed lazily on access** regardless of configuration. If you read an expired entry, it's cleaned up then. Cleanup strategies handle the entries nobody ever touches again — which, for an abandoned key, is all of them.

#### `cleanupFullSnapshot()`

```java
.cleanupFullSnapshot()
```

Expired entries are filtered out **while writing a full snapshot**. The snapshot in storage is clean; the running state is not.

```
COST:    essentially zero at runtime.
BENEFIT: only the checkpoint/savepoint shrinks. Local state keeps growing
         until you restart from that snapshot.
CAVEAT:  does NOT work with RocksDB INCREMENTAL checkpoints — incremental
         checkpoints copy SST files, they don't rewrite records.
```

Useful only alongside another strategy, or for jobs that restart regularly.

#### `cleanupIncrementally(cleanupSize, runCleanupForEveryRecord)`

```java
.cleanupIncrementally(10, true)
//                    ^   ^
//                    |   └── also run a cleanup step on every record?
//                    └────── how many entries to inspect per cleanup step
```

Flink keeps a lazy iterator over the state. Each time cleanup triggers, it advances the iterator by `cleanupSize` entries and deletes any that have expired.

```
COST:    a small, bounded amount of work per record/access. Predictable.
BENEFIT: local state actually shrinks while the job runs.
LIMITS:  HEAP STATE BACKEND ONLY. Silently does nothing on RocksDB.
         Only touches entries the iterator reaches — a huge state with
         a small cleanupSize may never catch up.
```

Tuning: `cleanupSize = 10` with `runCleanupForEveryRecord = true` is a sane starting point. Raise `cleanupSize` if state isn't shrinking fast enough; lower it if you see latency creep.

#### `cleanupInRocksdbCompactFilter(queryTimeAfterNumEntries)`

```java
.cleanupInRocksdbCompactFilter(1000)
//                             ^ re-read the current timestamp every 1000 entries
```

RocksDB periodically **compacts** its SST files — merging and rewriting them to reclaim space. Flink installs a custom compaction filter that drops expired entries during that rewrite. Zero extra I/O: the file was being rewritten anyway.

```
COST:    slightly slower compaction (a timestamp check per entry).
BENEFIT: real physical deletion, works with incremental checkpoints,
         no per-record overhead.
LIMITS:  ROCKSDB ONLY. Cleanup happens on RocksDB's schedule, not yours —
         cold data in an unchanging SST file may sit for a long time.
```

The parameter is a performance knob: reading the current wall clock has a cost, so the filter caches it and refreshes every N entries. Lower = more accurate expiry, more clock calls. 1000 is the default and is almost always fine.

**This is enabled by default when you enable TTL on RocksDB.** Calling it explicitly is how you tune it.

#### Cleanup summary

| Strategy | Backend | Runtime cost | Shrinks live state? | Works with incremental checkpoints? |
|---|---|---|---|---|
| (lazy on access only) | both | zero | only touched entries | yes |
| `cleanupFullSnapshot()` | both | zero at runtime | no — only the snapshot | **no** |
| `cleanupIncrementally(n, b)` | **heap only** | small, per record | yes | n/a |
| `cleanupInRocksdbCompactFilter(n)` | **RocksDB only** | during compaction | yes | **yes** |

Recommended combinations:

```java
// HEAP backend
StateTtlConfig heapTtl = StateTtlConfig
        .newBuilder(Duration.ofHours(24))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupIncrementally(10, true)
        .cleanupFullSnapshot()
        .build();

// ROCKSDB backend
StateTtlConfig rocksTtl = StateTtlConfig
        .newBuilder(Duration.ofHours(24))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupInRocksdbCompactFilter(1000)
        .build();
```

## The critical warning: TTL is PROCESSING TIME

> **Key idea:** State TTL is measured against the **wall clock of the TaskManager**, not your event time or your watermark. There is no event-time TTL.

Read that twice, because it's the mistake that costs people a weekend.

```
Your job:            event-time windows, watermarks, event timestamps
Your TTL:            wall-clock time on the machine, completely independent
```

What goes wrong:

```
SCENARIO: Replaying 30 days of history from Kafka, with a 24-hour TTL.

  Wall clock:  the replay finishes in 40 minutes.
  Event time:  30 days advance.

  Every state entry lives the full 30 days of event time, because
  only 40 minutes of WALL CLOCK passed. Nothing expires.
  State grows to 30 days' worth. The job OOMs during a backfill
  that runs fine in steady state.
```

And the mirror image:

```
SCENARIO: A live job stalls for 2 hours (Kafka outage, deployment pause).

  Wall clock:  2 hours pass.
  Event time:  frozen.

  With a 1-hour TTL, state expires DURING the stall even though no
  events were processed. On resume, the state your logic depended
  on is silently gone. Results are wrong, no error is raised.
```

The mitigations:

1. **Set TTL generously** — comfortably longer than any plausible stall or replay skew. A TTL of 7 days for logic that needs 24 hours costs you 7x state but removes a whole class of bug.
2. **For event-time semantics, use event-time timers instead** (chapter 23). A timer registered at `eventTime + 24h` fires when the *watermark* passes it, and it's the only way to get true event-time expiry.
3. **Watch backfills specifically.** If you replay history, either raise the TTL for the backfill or accept a bigger state footprint.

The general rule: **TTL is a safety net against unbounded growth, not a correctness mechanism.** If expiry is part of your business logic, use timers. If it's "stop this from growing forever", use TTL.

## Worked example: user → last activity, with a 24h TTL

Track each user's last activity, and forget users who've been quiet for 24 hours.

```java
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class LastActivityFn extends KeyedProcessFunction<String, Event, String> {

    // Stores the last event's timestamp AND type, as "type:timestamp".
    // A small POJO would be cleaner; a String keeps this example focused.
    private transient ValueState<String> lastActivityState;

    @Override
    public void open(OpenContext ctx) {

        // ---- 1. Build the TTL config -------------------------------------
        StateTtlConfig ttlConfig = StateTtlConfig
                // Entries live 24 hours of WALL CLOCK time.
                .newBuilder(Duration.ofHours(24))

                // Only writes reset the clock. We update() on every event,
                // so this effectively means "24h after the user's last event".
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)

                // Never hand back a value past its expiry, even if the bytes
                // are still on disk. Deterministic behaviour.
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)

                // Physically reclaim space during RocksDB compaction.
                // Free — the SST files are being rewritten anyway.
                .cleanupInRocksdbCompactFilter(1000)

                .build();

        // ---- 2. Attach it to the descriptor ------------------------------
        ValueStateDescriptor<String> desc =
                new ValueStateDescriptor<>("last-activity", String.class);

        // MUST come before getState(). Enabling TTL after the handle exists
        // has no effect on that handle.
        desc.enableTimeToLive(ttlConfig);

        lastActivityState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<String> out)
            throws Exception {

        // With NeverReturnExpired, this returns null for a user whose entry
        // has expired — indistinguishable from a brand-new user. That's the
        // intended behaviour: "we've forgotten them".
        String previous = lastActivityState.value();

        String current = event.type + ":" + event.timestamp;

        if (previous == null) {
            out.collect(ctx.getCurrentKey() + " RETURNING/NEW, activity=" + current);
        } else {
            // split(":") cuts the string on the delimiter, returning a String[].
            // [1] is the timestamp half; parseLong turns it back into a long.
            long previousTs = Long.parseLong(previous.split(":")[1]);
            long gapMinutes = (event.timestamp - previousTs) / 60_000;
            //                                                  ^ underscores are
            //          legal in Java numeric literals for readability: 60000 ms

            out.collect(String.format("%s activity=%s (gap %d min, prev %s)",
                    ctx.getCurrentKey(), current, gapMinutes, previous));
        }

        // This write resets the 24h TTL clock for this key.
        lastActivityState.update(current);
    }
}
```

```java
events
    .keyBy(e -> e.userId)
    .process(new LastActivityFn())
    .print();
```

### Trace

Wall-clock times shown on the left; TTL is measured against these, not against event timestamps.

```
wall clock   record                    value()      emitted                                TTL expiry
──────────   ───────────────────────   ──────────   ────────────────────────────────────   ──────────
10:00:00     alice LOGIN    @t=1000    null         alice RETURNING/NEW, activity=...      alice: 10:00 +24h = next day 10:00
10:05:00     alice PURCHASE @t=1300    LOGIN:1000   alice activity=... (gap 5 min, ...)    alice: RESET -> 10:05 +24h
10:06:00     bob   LOGIN    @t=1360    null         bob RETURNING/NEW, activity=...        bob:   10:06 +24h

... 25 hours pass, no events for alice or bob ...

11:30:00     alice LOGIN    @t=...     null         alice RETURNING/NEW, activity=...      alice: reset again
 (next day)                            ^^^^
                                       expired: the 24h window from 10:05 the
                                       previous day elapsed. alice reads as new.
                                       Her old entry is dropped on this access
                                       (lazy cleanup) or during compaction.
```

Steady state with this TTL: your state holds roughly "users active in the last 24 hours", not "every user who ever existed". If you have 100 M lifetime users and 2 M daily actives, that's a 50x reduction in state.

## TTL on the other state types

TTL works everywhere, with per-type semantics:

```java
// ValueState — the single value expires
valueDescriptor.enableTimeToLive(ttlConfig);

// ListState — EACH ELEMENT expires individually, by when it was added
listDescriptor.enableTimeToLive(ttlConfig);

// MapState — EACH ENTRY expires individually, by when it was put
mapDescriptor.enableTimeToLive(ttlConfig);

// ReducingState / AggregatingState — the folded value expires as one unit
reducingDescriptor.enableTimeToLive(ttlConfig);
```

Per-element TTL on `ListState` and `MapState` is genuinely useful: a `MapState<String, Boolean>` seen-set with a 1-hour TTL automatically forgets device IDs an hour after they were last put, without you writing any eviction code.

## The gotchas

```java
// ❌ enableTimeToLive AFTER getState — no effect on the handle you already have
state = getRuntimeContext().getState(desc);
desc.enableTimeToLive(ttlConfig);

// ❌ cleanupIncrementally on RocksDB — silently does nothing
.cleanupIncrementally(10, true)   // heap only

// ❌ cleanupFullSnapshot with RocksDB incremental checkpoints — incompatible
.cleanupFullSnapshot()

// ❌ Assuming TTL follows event time — it does not. Wall clock only.

// ❌ Enabling TTL on a job restored from a savepoint whose state has no
//    TTL timestamps. Flink handles this (it treats existing entries as
//    freshly written), but the reverse — DISABLING TTL on state that has it —
//    throws a StateMigrationException. Changing the TTL DURATION is fine.
```

That last one is worth stating positively: you may **add** TTL, and you may **change the duration**, but you may not **remove** TTL from a savepoint that has it. The stored entries carry timestamps that a non-TTL serializer can't read.

## The pre-production checklist

For every piece of keyed state you write, answer these:

```
1. What is the key?                          e.g. userId
2. Is the keyspace bounded?                  100M users, growing = NO
3. Do I clear() when a key is done?           only on ACCOUNT_CLOSED = incomplete
4. Do I have a TTL?                          <- if 2 is NO and 3 is NO, this is MANDATORY
5. Is the TTL longer than my worst stall or backfill skew?
6. Is my cleanup strategy right for my state backend?
7. What is the projected steady-state size?  keys x bytes/key x safety factor
```

Estimating step 7:

```
2,000,000 daily active users
  x  ~200 bytes per state entry (a Double, a String, RocksDB overhead)
  =  400 MB

Without TTL, at 100,000 new users/day over a year:
  36,500,000 keys x 200 bytes = 7.3 GB, still growing linearly. Forever.
```

## Remember

- Nothing in Flink expires state automatically. TTL is opt-in, per descriptor.
- `StateTtlConfig.newBuilder(Duration)` → `setUpdateType` → `setStateVisibility` → a cleanup strategy → `build()`.
- `OnCreateAndWrite` (default) = expire after last write. `OnReadAndWrite` = LRU, but every read becomes a write.
- `NeverReturnExpired` (default) = deterministic. `ReturnExpiredIfNotCleaned` = best-effort, non-reproducible.
- `cleanupIncrementally` is heap-only. `cleanupInRocksdbCompactFilter` is RocksDB-only. `cleanupFullSnapshot` doesn't work with incremental checkpoints.
- Expired entries are always cleaned lazily on access, whatever else you configure.
- **TTL is processing time.** Backfills won't expire; stalls will expire too much.
- For event-time expiry semantics, use event-time timers (chapter 23).
- `enableTimeToLive()` before `getState()`, never after.
- You can add TTL and change its duration across restores; you cannot remove it.
- Unbounded keyspace + no TTL = the job dies. Only the date is uncertain.

## Interview one-liners

- *"Why do Flink jobs die in production?"* → Usually unbounded keyspace with no TTL. State grows until checkpoints time out or disk fills. It looks healthy for weeks first.
- *"Is state TTL event time or processing time?"* → Processing time, always. There is no event-time TTL; use event-time timers if you need event-time expiry.
- *"What breaks with TTL during a backfill?"* → Replaying 30 days in 40 minutes advances event time but not wall clock, so nothing expires and state balloons.
- *"`OnCreateAndWrite` vs `OnReadAndWrite`?"* → Whether reads reset the clock. `OnReadAndWrite` gives LRU semantics but turns every read into a write.
- *"What does `NeverReturnExpired` guarantee?"* → An expired value is never returned even if it's still physically present. `ReturnExpiredIfNotCleaned` makes results depend on cleanup timing.
- *"Which cleanup strategy for RocksDB?"* → `cleanupInRocksdbCompactFilter` — it piggybacks on compaction, so it's nearly free and compatible with incremental checkpoints. `cleanupIncrementally` is heap-only.
- *"Can you turn TTL off later?"* → No — removing TTL from state that has it throws a `StateMigrationException` on restore. Adding it, or changing the duration, is fine.
- *"How does TTL interact with `MapState`?"* → Per entry: each entry expires based on when it was put, which makes TTL-backed dedup sets trivial.

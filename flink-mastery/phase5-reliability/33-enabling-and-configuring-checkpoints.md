# 33. Enabling and Configuring Checkpoints

Chapter 32 was the mechanism. This is every knob, what it does, and what happens when you get it wrong.

> **Key idea**
> Checkpointing is **off by default**. One line turns it on. The other ten lines exist to stop checkpointing from eating your job.

---

## The minimum

```java
StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

env.enableCheckpointing(60_000);   // every 60 seconds
```

`60_000` is Java's **numeric literal underscore** — purely cosmetic digit grouping, identical to `60000`. Use it; `600_000` vs `6000000` is a real source of bugs.

The argument is the interval in **milliseconds**: how often the JobManager *triggers* a new checkpoint, measured from the *start* of one to the *start* of the next.

That single line gives you: state survives failures, sources rewind to the checkpointed offsets, `EXACTLY_ONCE` mode (the default). It does not give you: retained checkpoints after cancellation, sane behaviour under backpressure, or protection from checkpoint pile-up.

---

## The full production block

Everything below is real API in Flink 1.18/1.20.

```java
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointRetention;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.execution.CheckpointingMode;

import java.time.Duration;

StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

env.enableCheckpointing(60_000);

CheckpointConfig cp = env.getCheckpointConfig();

cp.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
cp.setMinPauseBetweenCheckpoints(30_000);
cp.setCheckpointTimeout(300_000);
cp.setMaxConcurrentCheckpoints(1);
cp.setTolerableCheckpointFailureNumber(3);
cp.setExternalizedCheckpointRetention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
cp.enableUnalignedCheckpoints();
cp.setAlignedCheckpointTimeout(Duration.ofSeconds(30));
```

`env.getCheckpointConfig()` returns a mutable settings object attached to the environment. Every `set...` call mutates it in place and returns `void` — this is **not** a fluent builder, so you cannot chain them. Assigning it to a local variable `cp` is just to avoid typing `env.getCheckpointConfig()` eight times.

Now each line.

---

### `setCheckpointingConsistencyMode(...)`

```java
cp.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);   // default
cp.setCheckpointingConsistencyMode(CheckpointingMode.AT_LEAST_ONCE);
```

| Mode | Alignment | State on restore | Cost |
|---|---|---|---|
| `EXACTLY_ONCE` | yes (barriers aligned) | each record affects state exactly once | alignment latency |
| `AT_LEAST_ONCE` | **no** — snapshot on the first barrier, don't wait | records between the first and last barrier are reprocessed → counted twice | none |

`AT_LEAST_ONCE` is genuinely the right choice when your state is idempotent-under-replay (`MAX`, "last value seen", a set membership) or when downstream deduplicates. It removes alignment entirely, so it is the lowest-latency option.

> **Naming note:** `CheckpointingMode` moved to `org.apache.flink.core.execution.CheckpointingMode` in 1.20; the old `org.apache.flink.streaming.api.CheckpointingMode` and the older setter name `setCheckpointingMode(...)` still work but are deprecated. Older tutorials also show `env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE)` — the two-arg form is fine and equivalent.

---

### `setMinPauseBetweenCheckpoints(...)` — the important one

```java
cp.setMinPauseBetweenCheckpoints(30_000);
```

Guarantees at least 30 seconds between the **end** of one checkpoint and the **start** of the next. This is the knob nobody sets and everybody needs.

```
INTERVAL alone (60s), checkpoint takes 55s:

 |--------- CP 1 (55s) ---------|-5s-|--------- CP 2 (55s) ---------|-5s-|
 └── job does real work for 5 out of every 60 seconds ──┘
 → throughput collapses → backpressure → checkpoints take longer → 60s
 → checkpoints now overlap or time out. Death spiral.

INTERVAL 60s + MIN PAUSE 30s, checkpoint takes 55s:

 |--------- CP 1 (55s) ---------|------ 30s of pure work ------|--- CP 2 ...
 └── the job is GUARANTEED 30s of unimpeded processing ──┘
```

The interval is a *wish*; the min pause is a *guarantee*. Under load, min pause is what actually controls your checkpoint frequency, and it degrades gracefully: if checkpoints get slower, they simply get less frequent, instead of stacking up.

**Side effect worth knowing:** setting a min pause implicitly forces `maxConcurrentCheckpoints = 1`, because "a gap between them" is meaningless if several run at once.

Rule of thumb: **min pause ≈ half the interval**, or ≈ your typical checkpoint duration, whichever is larger.

---

### `setCheckpointTimeout(...)`

```java
cp.setCheckpointTimeout(300_000);   // 5 minutes; default is 10 minutes
```

If a checkpoint has not completed within this, it is aborted and counted as a failure.

Do not treat a raised timeout as a fix. A checkpoint that needs 9 minutes on a 60-second interval is a broken job; making the timeout 10 minutes only delays when you find out. Set the timeout to something you would actually consider acceptable, and let it fail loudly.

---

### `setMaxConcurrentCheckpoints(...)`

```java
cp.setMaxConcurrentCheckpoints(1);   // default
```

How many checkpoints may be in flight simultaneously. Keep it at 1 in production.

Raising it is only defensible in one case: a job with **very long but very cheap** checkpoints (long async upload, low I/O cost) where you want a higher checkpoint frequency than the duration allows — e.g. exactly-once sink latency tied to the interval. Otherwise concurrent checkpoints just multiply the I/O contention that made them slow.

Note it is **mutually exclusive with min pause** — set min pause and this is forced to 1.

---

### `setTolerableCheckpointFailureNumber(...)`

```java
cp.setTolerableCheckpointFailureNumber(3);
```

How many *consecutive* checkpoint failures the job tolerates before the **job itself** is failed and restarted.

- Default is **0** in current versions: one failed checkpoint kills the job.
- A single S3 500, one transient timeout, one GC pause — these should not restart a job with 200 GB of state and a 10-minute recovery.
- Set 3–5. Do not set it enormous; a job that cannot checkpoint at all is a job that will lose everything on the next real failure, and you want to be paged.

The counter resets on any successful checkpoint.

---

### `setExternalizedCheckpointRetention(...)`

```java
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointRetention;

cp.setExternalizedCheckpointRetention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
```

Three values:

| Value | On job **cancel** | On job **failure** (terminal) |
|---|---|---|
| `DELETE_ON_CANCELLATION` | checkpoint deleted | retained |
| `RETAIN_ON_CANCELLATION` | **retained** | retained |
| `NO_EXTERNALIZED_CHECKPOINTS` (default) | deleted | deleted |

With the default, cancelling your job throws away every checkpoint and you cannot restart from where you were. `RETAIN_ON_CANCELLATION` is what makes `flink run -s <checkpoint-path>` possible, and it is what the lab in chapter 38 depends on.

The trade: retained checkpoints are **yours to delete**. Flink will not clean them up. Budget for that.

> **Naming note:** this replaced `setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)`, which was deprecated in 1.20. The old name still compiles; the enum values are identical.

---

### `enableUnalignedCheckpoints()` and `setAlignedCheckpointTimeout(...)`

```java
cp.enableUnalignedCheckpoints();
cp.setAlignedCheckpointTimeout(Duration.ofSeconds(30));
```

Together these give the **hybrid** behaviour, which is what you almost always want:

```
Checkpoint N starts.
  ├─ try ALIGNED first (cheap: state only, no buffers)
  ├─ if alignment has not completed after 30 seconds:
  └─ SWITCH to unaligned mid-flight → snapshot the buffers, finish fast
```

Normal operation costs nothing extra. Under a backpressure spike, checkpointing degrades to "bigger but still completes" instead of "times out and kills the job".

Setting `enableUnalignedCheckpoints()` alone (timeout defaults to 0) means *always* unaligned — you pay the extra I/O on every checkpoint whether or not you need it.

**Restrictions to know:**
- Requires `EXACTLY_ONCE` mode; ignored under `AT_LEAST_ONCE` (which never aligns anyway).
- Rescaling from an unaligned checkpoint is supported from Flink 1.15 onward.
- No help at all if the *source* is the bottleneck — there's nothing in flight to skip past.

---

## The equivalent config file keys

Everything above can be set outside the code, which is how you should do it in production — a config change should not need a recompile.

The file is `conf/flink-conf.yaml` in Flink ≤ 1.19 and `conf/config.yaml` in Flink 1.19+ (the new file is proper nested YAML; the old one was flat `key: value` lines). Both accept the same key names.

```yaml
# --- flat form, works in both files ---
execution.checkpointing.interval: 60s
execution.checkpointing.min-pause: 30s
execution.checkpointing.timeout: 5min
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.tolerable-failed-checkpoints: 3
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
execution.checkpointing.unaligned.enabled: true
execution.checkpointing.aligned-checkpoint-timeout: 30s

state.backend.type: rocksdb
state.backend.incremental: true
execution.checkpointing.dir: file:///tmp/flink-checkpoints
execution.checkpointing.savepoint-dir: file:///tmp/flink-savepoints
execution.checkpointing.num-retained: 3
```

Notes:

- Durations are **strings with units** here (`60s`, `5min`, `1h`), not raw milliseconds. `60` alone means 60 **ms** in some options — always write the unit.
- `execution.checkpointing.dir` and `.savepoint-dir` replaced `state.checkpoints.dir` and `state.savepoints.dir` in 1.19. The old keys still work.
- `execution.checkpointing.num-retained` (formerly `state.checkpoints.num-retained`, default 1) is how many *completed* checkpoints are kept on disk. Set it to 3 so that if the newest checkpoint is somehow corrupt you have a fallback. This is separate from the retention-on-cancel setting.

To set the same things per-job from code without touching the cluster config:

```java
import org.apache.flink.configuration.Configuration;

Configuration conf = new Configuration();
conf.setString("execution.checkpointing.interval", "60s");
conf.setString("execution.checkpointing.min-pause", "30s");
conf.setString("state.backend.type", "rocksdb");
conf.setString("state.backend.incremental", "true");

StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(conf);
```

`getExecutionEnvironment(Configuration)` seeds the environment from that config. Handy for tests, and it means one code path can be driven entirely by external settings.

Or from the CLI at submit time, with `-D`:

```bash
./bin/flink run \
  -Dexecution.checkpointing.interval=60s \
  -Dexecution.checkpointing.min-pause=30s \
  -Dstate.backend.type=rocksdb \
  -c com.example.MyJob target/my-job.jar
```

**Precedence, lowest to highest:** cluster config file → `-D` flags at submit → code (`env.enableCheckpointing(...)` etc.). Code wins. This is a common surprise: your `-D` flag is silently ignored because the job hard-codes the same setting. Prefer to *not* hard-code in the job.

---

## Incremental checkpoints (RocksDB only)

```java
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;

env.setStateBackend(new EmbeddedRocksDBStateBackend(true));   // true = incremental
```

or

```yaml
state.backend.type: rocksdb
state.backend.incremental: true
```

### What it does

RocksDB stores state in **immutable SST files**. A full checkpoint uploads all of them. An incremental checkpoint uploads only the SST files created *since the last checkpoint*, and records a reference to the ones already uploaded.

```
FULL                                INCREMENTAL
cp 5: upload 200 GB                 cp 5: upload 200 GB   (first one is full)
cp 6: upload 200 GB                 cp 6: upload   2 GB   + refs to cp 5's files
cp 7: upload 200 GB                 cp 7: upload   1.5 GB + refs to 5 and 6
```

At 200 GB / 60 s, a full checkpoint requires 3.4 GB/s of sustained upload, which is not achievable on most clusters. Incremental turns that into a manageable number. Above roughly 10 GB of state, this is not optional.

### The trade-offs — say these before someone else does

1. **Restore is slower.** You must fetch and reassemble the whole chain of files, not one blob. Restore duration is the metric nobody monitors and the one that hurts at 3 a.m.
2. **Storage grows in a way that surprises you.** An old SST file stays referenced until compaction rewrites it. So your *checkpoint directory* can hold far more than your state size, even though each individual checkpoint is small.
3. **"Checkpointed Data Size" in the UI is now the delta, not the state size.** People see 2 GB and conclude their state is 2 GB. Read **Full Checkpoint Data Size** for the real number.
4. **Not available on `HashMapStateBackend`.** Heap state has no immutable-file structure to diff against, so every heap checkpoint is full.

```java
new EmbeddedRocksDBStateBackend(true);    // incremental
new EmbeddedRocksDBStateBackend(false);   // full, every time
new EmbeddedRocksDBStateBackend();        // reads state.backend.incremental from config
```

---

## The changelog state backend (briefly)

Even with incremental RocksDB, checkpoint duration is spiky: it depends on when RocksDB happens to have flushed and compacted. The **generic changelog state backend** (Flink 1.15+, production-ready 1.16+) smooths that out.

```yaml
state.backend.changelog.enabled: true
state.backend.changelog.storage: filesystem
dstl.dfs.base-path: s3://bucket/changelog
```

```java
env.enableChangelogStateBackend(true);
```

How it works: every state *modification* is appended to a durable log continuously, in the background. A checkpoint then only needs to record "the log up to offset X" — it does not need to wait for a RocksDB flush. The underlying state backend (RocksDB or heap) still materialises periodically in the background.

- **Buys:** much shorter and much more *predictable* checkpoint durations, which means you can checkpoint more often, which means lower end-to-end latency for exactly-once sinks.
- **Costs:** continuous background writes to durable storage (more total I/O and money), extra space, and slower recovery (materialised state + log replay).

Reach for it when checkpoint *duration variance* is your problem, not when checkpoint *size* is. Most jobs never need it.

---

## Anti-patterns

```java
// ❌ Every second. At any real state size this never completes.
env.enableCheckpointing(1000);

// ❌ Interval with no min pause. Job spends its life checkpointing.
env.enableCheckpointing(60_000);
// (no setMinPauseBetweenCheckpoints)

// ❌ Timeout as a bandage. Hides the problem for 30 minutes.
cp.setCheckpointTimeout(1_800_000);

// ❌ Concurrency to "keep up". Multiplies the I/O contention.
cp.setMaxConcurrentCheckpoints(4);

// ❌ Default retention. Cancel the job and every checkpoint is gone.
// (nothing set)

// ❌ Full checkpoints on 500 GB of state.
env.setStateBackend(new EmbeddedRocksDBStateBackend(false));
```

The safe starting point for a stateful production job:

```java
env.enableCheckpointing(60_000);
CheckpointConfig cp = env.getCheckpointConfig();
cp.setMinPauseBetweenCheckpoints(30_000);
cp.setCheckpointTimeout(300_000);
cp.setMaxConcurrentCheckpoints(1);
cp.setTolerableCheckpointFailureNumber(3);
cp.setExternalizedCheckpointRetention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
cp.enableUnalignedCheckpoints();
cp.setAlignedCheckpointTimeout(Duration.ofSeconds(30));

env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
```

Then tune the interval down only if an exactly-once sink's latency requires it — and read chapter 37 before you do, because that latency is bound to this interval.

---

## Remember

- Checkpointing is **off by default**; `env.enableCheckpointing(ms)` turns it on. Interval is start-to-start.
- `setMinPauseBetweenCheckpoints` is the **highest-value knob**: it guarantees real work time and degrades gracefully. It implies `maxConcurrentCheckpoints = 1`.
- `setCheckpointTimeout` is not a fix; it is a detector. Don't inflate it.
- `setTolerableCheckpointFailureNumber` defaults to **0** — one blip restarts your job. Set 3–5.
- `RETAIN_ON_CANCELLATION` is what makes `flink run -s <checkpoint>` possible. Retained checkpoints are yours to clean up.
- `enableUnalignedCheckpoints()` + `setAlignedCheckpointTimeout(30s)` = try aligned, fall back to unaligned. Best default.
- Config file: `flink-conf.yaml` (≤1.19) or `config.yaml` (1.19+); keys are `execution.checkpointing.*`. Durations need units.
- **Precedence:** cluster config < `-D` at submit < code. Don't hard-code what you want to tune.
- Incremental checkpoints are **RocksDB only** and mandatory above ~10 GB of state — at the cost of slower restore and retained historical files.
- Changelog backend fixes checkpoint *variance*, not checkpoint *size*.

**Interview one-liners**

- *"How do you enable checkpointing?"* → `env.enableCheckpointing(interval)`, then configure via `env.getCheckpointConfig()`. Off by default.
- *"Interval vs min pause?"* → Interval is start-to-start and is only a wish; min pause guarantees a gap between the end of one and the start of the next, so a job under load checkpoints less often instead of continuously.
- *"Exactly-once vs at-least-once checkpointing mode?"* → Exactly-once aligns barriers so each record affects state once; at-least-once skips alignment and reprocesses the records between the first and last barrier. At-least-once is cheaper and correct when state is replay-idempotent.
- *"What does tolerableCheckpointFailureNumber default to?"* → 0 — a single failed checkpoint fails the job. Almost always wrong for large-state jobs.
- *"Why enable incremental checkpoints?"* → Upload only new RocksDB SST files instead of the full state; the trade is slower restore, a longer file-reference chain, and unbounded retention of historical files.
- *"Where do you configure this in production?"* → `config.yaml` / `-D` flags, not code — code has the highest precedence and silently overrides your operational settings.

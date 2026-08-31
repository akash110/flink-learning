# 36. Fault Tolerance and Restart Strategies

You have checkpoints. A TaskManager dies. Walk through exactly what happens, in order — then configure how aggressively Flink should try again.

> **Key idea**
> Flink's recovery model is **rewind and replay**. There is no partial repair. The whole job (or a whole failover region) goes back to the last completed checkpoint and re-processes everything since.
> Everything downstream of that fact — duplicates, exactly-once sinks, restart strategy tuning — follows from it.

---

## A TaskManager dies: the full sequence

```
t=0     Job running. Last COMPLETED checkpoint = 42 (taken at t=-25s).
        Kafka offsets in checkpoint 42: partition 0 → 1,000,000
        Aggregation state in checkpoint 42: user u1 count = 500

t=0     Records after offset 1,000,000 are being processed.
        In-memory count for u1 has reached 620.
        200 result rows have already been written to Postgres.

t=1     💥 TaskManager tm-3 is killed (OOM / node lost / kubectl delete pod)

────────────────────────────────────────────────────────────────────────
STEP 1 — DETECTION
        Two paths, whichever fires first:
          a) A task on a surviving TM tries to send data to tm-3 and the
             network connection breaks → immediate failure report to the JM.
          b) tm-3 misses its heartbeats to the JobManager.
             heartbeat.interval  = 10s   (default)
             heartbeat.timeout   = 50s   (default)
        Path (a) is usually near-instant. Path (b) is your worst case:
        up to ~50s before the JM even knows.

STEP 2 — JOB TRANSITIONS TO FAILING
        The JobManager cancels EVERY task in the affected failover region
        (see below), not just the ones on tm-3. State in those tasks is
        discarded — it is not trusted.

STEP 3 — WAIT FOR RESOURCES
        The JM asks the ResourceManager for replacement slots.
        - Standalone: only if another TM has free slots. Otherwise the job
          sits in RESTARTING until one appears.
        - Kubernetes/YARN active mode: a new TM is requested and must
          start up — tens of seconds, plus image pull.
        This step is usually the longest, and it is invisible in most dashboards.

STEP 4 — RESTORE STATE FROM CHECKPOINT 42
        Every restarted subtask reads its slice of checkpoint 42's state
        from checkpoint storage.
        - HashMapStateBackend: deserialize into heap. Fast.
        - RocksDB incremental: download the SST file chain from `shared/`
          and rebuild the local RocksDB instance. This can take MINUTES
          for large state, and it is the metric nobody monitors.
        u1's count is restored to 500.  (The 620 is gone.)

STEP 5 — REWIND THE SOURCES
        The Kafka source resets its consumer to the offsets stored in
        checkpoint 42: partition 0 → 1,000,000.
        This is the whole reason offsets live in Flink state and NOT in
        Kafka's __consumer_offsets. Flink must control the rewind.

STEP 6 — RESUME
        Records from offset 1,000,000 are processed AGAIN.
        u1's count climbs 500 → 620 → onward. State is correct.
────────────────────────────────────────────────────────────────────────

RECOVERY TIME = detection + resource wait + state restore + catch-up lag

The last term is the one people forget: after restoring you are 25+ seconds
behind real time, and you must consume FASTER than the incoming rate to
close that gap. If your job has no headroom, it never catches up.
```

### The duplicate problem, stated exactly

Look at step 6 against t=0:

```
Before the crash: 200 rows INSERTed into Postgres from records after offset 1,000,000.
After recovery:   those same records are processed again → 200 MORE INSERTs.

Flink's state:    perfectly correct. u1 = 620, exactly as if nothing happened.
Postgres:         400 rows where there should be 200.
```

> **Key idea**
> "Exactly-once" means **each record affects Flink's internal state exactly once**. It does **not** mean each record is *processed* once — records after the last checkpoint are provably reprocessed on every recovery.
> Anything you send *outside* Flink during that window happens twice unless the sink cooperates. That is chapter 37.

---

## Failover regions — why the whole job usually doesn't restart

Flink does not blindly restart everything. It computes **failover regions** from the connection types in your job graph.

```
PIPELINED connection (a shuffle: keyBy, rebalance, broadcast)
   → data flows continuously; a downstream task depends on live upstream tasks
   → both are in the SAME region

FORWARD / chained connection (1-to-1, no shuffle)
   → still pipelined, same region

BLOCKING connection (batch mode only: an intermediate result is materialised)
   → the downstream can be restarted independently
   → SEPARATE region
```

In **streaming** mode, everything connected by pipelined edges is one region. So:

```
EMBARRASSINGLY PARALLEL job — no keyBy anywhere:

  source-0 → map-0 → sink-0     region 1
  source-1 → map-1 → sink-1     region 2      ← independent pipelines!
  source-2 → map-2 → sink-2     region 3

  map-1 fails  →  ONLY region 2 restarts.
                  Regions 1 and 3 keep processing without interruption.


JOB WITH A keyBy — the usual case:

  source-0 ─┐              ┌─ agg-0 → sink-0
  source-1 ─┼─── keyBy ────┼─ agg-1 → sink-1     ALL ONE REGION
  source-2 ─┘              └─ agg-2 → sink-2

  agg-1 fails  →  EVERYTHING restarts. Every subtask, every source.
```

The shuffle is what couples them: `agg-1` receives data from *every* source, so restarting it means every source must rewind, so every other `agg` must also rewind to stay consistent.

Configured by:

```yaml
jobmanager.execution.failover-strategy: region     # default since Flink 1.10
# 'full' restarts the entire job on any failure — the old behaviour
```

Leave it at `region`. It is free when it doesn't apply and a large win when it does.

**Practical consequence:** if fast recovery matters and your logic allows it, avoiding an unnecessary `keyBy` is not just a shuffle optimisation — it is a *failover* optimisation. A map-only enrichment pipeline recovers one subtask at a time; add one `keyBy` and every failure is a full restart.

---

## Checkpoint failure vs job failure

Distinct things that people conflate.

| | Checkpoint failure | Job failure |
|---|---|---|
| What happened | A snapshot could not be completed: timeout, S3 error, a subtask failed mid-snapshot | A task threw, a TM died, a node was lost |
| Does the job stop? | **No** — processing continues, uninterrupted | Yes — restart strategy is consulted |
| Immediate consequence | The last *successful* checkpoint is now older. Your recovery point regressed. | Rewind to the last completed checkpoint |
| Counter | `numberOfFailedCheckpoints`, `lastCheckpointDuration` | `numRestarts`, `fullRestarts` |
| Governed by | `setTolerableCheckpointFailureNumber(n)` | the restart strategy |
| Escalation | After n consecutive checkpoint failures, the **job** is failed → becomes a job failure | — |

The subtle danger of checkpoint failures: **the job looks perfectly healthy.** Throughput normal, no restarts, dashboards green. But your last successful checkpoint is 4 hours old, so the next real failure replays 4 hours of Kafka.

**Alert on the age of the last successful checkpoint, not on the checkpoint duration.** That is the metric that tells you how much you would lose:

```
lastCheckpointCompletedTimestamp    → alert if now() - this > 3 × interval
numberOfFailedCheckpoints           → alert on any increase
numRestarts                         → alert on rate, not on any single one
```

---

## Restart strategies

When a job fails, the restart strategy decides whether and how quickly to try again.

### The default you get

- **Checkpointing enabled, nothing configured** → `exponential-delay` (Flink 1.19+; it was `fixed-delay` with `Integer.MAX_VALUE` attempts before that). Effectively "retry forever with backoff".
- **Checkpointing disabled** → `none`. Any failure terminates the job. Makes sense: without checkpoints there is nothing to restore.

### The modern configuration API

`env.setRestartStrategy(RestartStrategies.fixedDelayRestart(...))` is **deprecated**. The current way is through `Configuration` and `RestartStrategyOptions`.

```java
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

Configuration conf = new Configuration();
conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
         Duration.ofSeconds(10));

StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(conf);
```

Line notes:

- `Configuration` is a typed key-value bag. `conf.set(OPTION, value)` is generic: the `ConfigOption` object carries the value's type, so the compiler checks that you pass an `Integer` where an integer is expected. This is why `RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS` takes `3` and not `"3"`.
- The delay options take `java.time.Duration`. Older signatures took `Time` — deprecated, same as everywhere else.
- `getExecutionEnvironment(conf)` seeds the environment. The config must be passed **here**; setting it after the environment exists has no effect.

### 1. `fixed-delay`

Retry a fixed number of times, waiting a fixed interval between attempts.

```java
conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
```

```yaml
restart-strategy.type: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10s
```

```
fail → wait 10s → try → fail → wait 10s → try → fail → wait 10s → try → fail
→ attempts exhausted → job goes to FAILED and stays there
```

Simple and predictable. Its weakness: with a small `attempts`, a burst of unrelated transient failures over a long-running job eventually exhausts the budget and kills a job that was fine. The attempt counter is **not** reset by successful running time in this strategy.

The delay matters. Setting it to 0 with a persistent failure (a poison record, a bad config) means the job hot-loops through restart, restore, fail, thousands of times per minute, hammering your checkpoint storage.

### 2. `exponential-delay` — the good default

Back off progressively, and reset the backoff once the job has been healthy for a while.

```java
conf.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF,
         Duration.ofSeconds(1));
conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF,
         Duration.ofMinutes(5));
conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER, 2.0);
conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD,
         Duration.ofMinutes(10));
conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR, 0.1);
```

```yaml
restart-strategy.type: exponential-delay
restart-strategy.exponential-delay.initial-backoff: 1s
restart-strategy.exponential-delay.max-backoff: 5min
restart-strategy.exponential-delay.backoff-multiplier: 2.0
restart-strategy.exponential-delay.reset-backoff-threshold: 10min
restart-strategy.exponential-delay.jitter-factor: 0.1
restart-strategy.exponential-delay.attempts-before-reset-backoff: 2147483647
```

```
fail → 1s → fail → 2s → fail → 4s → fail → 8s → ... → capped at 5min
                                                        ↑ retries forever at 5min

If the job runs healthy for 10 minutes (reset-backoff-threshold),
the backoff resets to 1s.
```

Why this is the right default:

- A **transient** failure (a node blip, a brief network partition) is retried almost immediately — 1 second, not 30.
- A **persistent** failure backs off to 5-minute retries instead of hammering the cluster, giving you time to notice and intervene.
- The **reset threshold** means a healthy job doesn't accumulate penalty from failures that happened days ago.
- `jitter-factor: 0.1` randomises the delay by ±10%, which stops many jobs restarting from a shared outage in lockstep and stampeding your checkpoint storage all at once.

### 3. `failure-rate`

Fail the job only if failures exceed a rate — "more than N failures in an interval of T".

```java
conf.set(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");
conf.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 3);
conf.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL,
         Duration.ofMinutes(5));
conf.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY,
         Duration.ofSeconds(10));
```

```yaml
restart-strategy.type: failure-rate
restart-strategy.failure-rate.max-failures-per-interval: 3
restart-strategy.failure-rate.failure-rate-interval: 5min
restart-strategy.failure-rate.delay: 10s
```

```
"Restart forever, UNLESS more than 3 failures occur within any 5-minute window."

1 failure/hour for a week   → keeps restarting. Fine.
4 failures in 5 minutes     → job FAILED. Something is genuinely broken.
```

This encodes the distinction `fixed-delay` cannot: infrequent transient failures are normal operational noise; a burst is a real problem. Use it when you want a hard stop on a crash-looping job but don't want a long-running job killed by accumulated unrelated blips.

### 4. `none`

```java
conf.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
```

```yaml
restart-strategy.type: none
```

Any failure terminates the job. Use for: batch jobs where an external orchestrator (Airflow) owns retries; development, where you want the stack trace immediately instead of a restart loop hiding it.

### Choosing

| Strategy | Use when |
|---|---|
| `exponential-delay` | **Default for streaming.** Long-running, failures may be transient or persistent, you want to be paged rather than have the job die. |
| `failure-rate` | You want a hard stop on crash-looping, but tolerate rare unrelated failures indefinitely. |
| `fixed-delay` | Simple and predictable; you know the expected number of retries. Fine for short-lived or batch jobs. |
| `none` | Batch under an external scheduler, or development. |

**Precedence:** cluster `config.yaml` → `-D` at submit → code. Same as everywhere. And as with checkpointing, prefer to *not* set it in code so operations can change it without a rebuild.

---

## The restart loop you must be able to recognise

```
Checkpoint takes 55s on a 60s interval
    ↓
Job spends ~90% of its time checkpointing → throughput collapses
    ↓
Backpressure builds → barriers travel more slowly
    ↓
Checkpoint duration exceeds the timeout → checkpoint FAILS
    ↓
tolerableCheckpointFailureNumber exceeded → JOB FAILS
    ↓
Restart → restore state (minutes for large RocksDB state)
    ↓
Job resumes ~5 minutes behind → must consume FASTER than real time to catch up
    ↓
Catch-up load is higher than steady-state load → more backpressure
    ↓
                    ← back to the top, but worse
```

Every arrow is something you can break:

- `setMinPauseBetweenCheckpoints` breaks the first (guarantees work time).
- Incremental checkpoints break the third (shorter checkpoints).
- `setTolerableCheckpointFailureNumber(3)` breaks the fifth (don't die on one blip).
- Provisioning **headroom** — running at 60% of capacity, not 95% — breaks the last one. A job with no headroom can never catch up after any outage, which makes every restart permanent.

The exponential backoff strategy also helps: it stops the loop from spinning at full speed while you diagnose.

The full diagnostic tree for the checkpoint half of this loop is in [`../../01-checkpointing-slow.md`]; the backpressure half is in [`../../02-backpressure.md`].

---

## Metrics to watch

```
numRestarts                          restarts since job start
fullRestarts                         full (non-region) restarts
uptime                               ms since the last restart — a sawtooth = crash loop
downtime                             ms spent not RUNNING
lastCheckpointCompletedTimestamp     ← alert on its AGE. The most important one.
numberOfFailedCheckpoints
lastCheckpointDuration
lastCheckpointRestoreTimestamp
```

Two alerts every stateful Flink job should have:

1. `now() - lastCheckpointCompletedTimestamp > 3 × checkpoint interval` — you have silently lost your recovery point.
2. `rate(numRestarts) > 0` sustained — a crash loop, even if the job says RUNNING between restarts.

---

## Remember

- Recovery is **rewind and replay**: cancel the region, restore state from the last completed checkpoint, rewind sources to the checkpointed offsets, reprocess.
- Sequence: detection → cancel region → wait for slots → restore state → rewind sources → resume → **catch up**. The catch-up needs spare capacity, or you never close the gap.
- Detection can take up to `heartbeat.timeout` (50s default) when a node vanishes silently.
- Reprocessing means **duplicate side effects** for anything written outside Flink. Internal state stays correct.
- **Region failover** (`jobmanager.execution.failover-strategy: region`, the default) restarts only the affected region — but any `keyBy` couples everything into one region.
- **Checkpoint failure ≠ job failure.** A job with failing checkpoints looks healthy while its recovery point silently ages. Alert on the *age of the last successful checkpoint*.
- `env.setRestartStrategy(...)` is deprecated; use `Configuration` + `RestartStrategyOptions`, or `config.yaml`.
- Defaults: `exponential-delay` when checkpointing is on, `none` when it is off.
- **`exponential-delay` is the right default** — fast retry for blips, backoff for real breakage, reset when healthy, jitter to avoid stampedes.
- `failure-rate` when you want "restart forever unless it's crash-looping".
- Restart delay of 0 with a persistent failure = a hot loop hammering checkpoint storage.

**Interview one-liners**

- *"What happens when a TaskManager dies?"* → The JM detects it via a broken connection or heartbeat timeout, cancels the affected failover region, acquires new slots, restores each subtask's state from the last completed checkpoint, rewinds the sources to the checkpointed offsets, and resumes — then has to catch up on the accumulated lag.
- *"Why do you get duplicates even with exactly-once?"* → Records after the last checkpoint are provably reprocessed on recovery. Flink's state is corrected by the rewind; external writes are not, unless the sink is transactional or idempotent.
- *"What is region failover?"* → Flink restarts only the connected component containing the failure. Effective for embarrassingly parallel jobs; a single `keyBy` puts everything in one region, so any failure is a full restart.
- *"Checkpoint failure vs job failure?"* → A failed checkpoint doesn't stop processing; it just ages your recovery point silently. Only after `tolerableCheckpointFailureNumber` consecutive failures does it escalate to a job failure.
- *"Which restart strategy and why?"* → `exponential-delay`: near-immediate retry for transient failures, backoff up to a cap for persistent ones, backoff reset after a healthy period, and jitter so many jobs don't restart in lockstep.
- *"Why does a restart sometimes never recover?"* → No headroom. After a restart the job is behind and must consume faster than the incoming rate; a job sized at 95% of capacity can never close that gap.

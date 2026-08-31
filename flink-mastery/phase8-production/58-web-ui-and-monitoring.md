# 58. The Web UI and Monitoring

The Web UI at `http://<jobmanager>:8081` is not a dashboard. It is a **diagnostic instrument**, and almost every Flink incident is solved by reading four of its tabs in the right order.

> **Key idea**
> There is one diagnostic sequence. Learn it as a reflex:
> **Overview** (is it even running?) → **Job graph** (where does the flow stop?) → **Backpressure** (which operator is the bottleneck?) → **Checkpoints** (is state healthy?) → **TaskManagers/Exceptions** (is the JVM the problem?).

---

## Overview page

The landing page. Four numbers, top-left:

```
┌───────────────────────────────────────────────────────┐
│ Available Task Slots : 0        Total Task Slots : 16 │
│ Running Jobs         : 2        TaskManagers     : 4  │
└───────────────────────────────────────────────────────┘
```

| What you see | What it means |
|---|---|
| Available slots **0**, job stuck in `CREATED` | Not enough slots. `NoResourceAvailableException` is coming. Add TaskManagers or lower parallelism. |
| Total slots **less than expected** | A TaskManager died or never registered. Check the TaskManagers tab. |
| TaskManagers count **fluctuating** | Pods are crash-looping. Go look at TM logs before anything else. |
| Job in `RESTARTING` | Something is failing repeatedly. Go straight to the **Exceptions** tab. |

Job states you will see: `CREATED → RUNNING → FINISHED / CANCELED / FAILED`, plus `RESTARTING` and `FAILING`. A healthy streaming job sits in `RUNNING` forever.

---

## The Job graph view

Click a job. The main panel is the **JobGraph**: one box per vertex, arrows for data flow.

```
┌──────────────────────┐      ┌───────────────────┐      ┌─────────────────┐
│ Source: Kafka        │      │ KeyedProcess      │      │ Sink: Kafka     │
│ -> Map -> Filter     │─────►│ (FraudDetector)   │─────►│                 │
│                      │      │                   │      │                 │
│ Parallelism: 8       │      │ Parallelism: 8    │      │ Parallelism: 8  │
│ Records Sent 12.4M   │      │ Recv 12.4M        │      │ Recv 41.2K      │
│                      │      │ Sent 41.2K        │      │                 │
└──────────────────────┘      └───────────────────┘      └─────────────────┘
        HASH                          FORWARD
```

Things to read off it immediately:

**Box coloring.** Boxes are shaded by their **busy** ratio — from blue/grey (idle) through to red (busy or backpressured). Hovering shows the exact percentages. The colour is a hint; the Backpressure tab is the ground truth.

**Fewer boxes than operators you wrote.** That is **operator chaining** — adjacent operators with the same parallelism and no repartitioning are fused into one task. The box title shows the chain: `Source: Kafka -> Map -> Filter`. Not a bug.

**The edge label** tells you the shipping strategy: `FORWARD` (chained/pointwise, no network), `HASH` (a `keyBy`), `REBALANCE`, `BROADCAST`, `RESCALE`. Every non-FORWARD edge is a serialize-network-deserialize cost, per [`../../06-scale-arithmetic.md`].

**Records In / Records Out per edge.** This is the highest-value number on the page.

```
Source sent    12,400,000
Filter received 12,400,000     ← matches. Good.
Filter sent            0       ← 🔴 EVERYTHING is being filtered out.

Window received 12,400,000
Window sent             0      ← 🔴 usually NOT a filter bug:
                                  the watermark is not advancing, so no
                                  window has ever fired. Go to the
                                  Watermarks tab.
```

> **Key idea**
> **A `sent` count of zero on a windowed operator is a watermark problem until proven otherwise.** The most common causes are an idle Kafka partition holding the watermark back, and a `WatermarkStrategy` with no `withIdleness`.

**Caveat:** `numRecordsIn`/`numRecordsOut` are **not** counted across a chain — records passed inside a chained task are plain method calls, not tracked records. So a chained box often shows `Records Received: 0` for its first operator. Look at the chain's boundaries, not its interior.

### The Subtasks tab — skew detection in ten seconds

Click a vertex → **Subtasks**. Sort by Records Received.

```
subtask 0:  4,200,000    ← 🔴
subtask 1:     31,000
subtask 2:     29,500
subtask 3:     30,100
```

Max/median above roughly 5x is real skew. What to do about it is in [`../../03-state-and-skew.md`].

---

## The Backpressure tab

Per vertex, three percentages that sum to ~100%:

```
┌────────────────────────────────────────────────────────┐
│ Vertex             Busy    Backpressured    Idle       │
├────────────────────────────────────────────────────────┤
│ Source: Kafka       12%          88%          0%       │ ← victim
│ KeyedProcess         9%          91%          0%       │ ← victim
│ Sink: Postgres      99%           0%          1%       │ ← 🔴 THE BOTTLENECK
└────────────────────────────────────────────────────────┘
```

**The rule: the bottleneck is the first operator going downstream that is NOT backpressured.** Everything upstream of it is red and is a *symptom*. People instinctively blame the red boxes; the red boxes are the victims.

Three readings:

- **Busy ~100%, Backpressured ~0%** → this operator is the bottleneck, working flat out. CPU or logic bound.
- **Backpressured ~100%** → waiting on something downstream. Not your problem.
- **All three low but lag is growing** → the **source** is the limit: too few Kafka partitions, or a slow external system.

That third case is the one people miss, because nothing looks wrong.

The full method — the four causes, the code that creates each one, and the fixes — is [`../../02-backpressure.md`]. Read it; this section is only the "how to see it in the UI" half.

**One thing that masks everything:** a long GC pause looks *identical* to backpressure. Before restructuring your job, check `Status.JVM.GarbageCollector.*.Time` on the TaskManagers tab.

---

## The Checkpoints tab

Four sub-tabs. All four matter.

### Overview

```
Checkpoint Counts
  Triggered: 1,204   In Progress: 1   Completed: 1,198   Failed: 5   Restored: 2
```

- **Failed > 0 and increasing** → an incident. Occasional failures during a restart are normal; a sustained rate is not.
- **Restored** = how many times the job has recovered from a checkpoint. A rising number is a crash loop.
- **In Progress stuck at 1** for longer than the interval → the checkpoint is hanging. Usually backpressure.

### History

One row per recent checkpoint.

| Column | Meaning | A big number means |
|---|---|---|
| **ID** | checkpoint number | — |
| **Status** | IN_PROGRESS / COMPLETED / FAILED | — |
| **Trigger Time** | when the JobManager started it | — |
| **End to End Duration** | trigger → last subtask acked | useless alone — **always decompose it** |
| **Checkpointed Data Size** | bytes written by *this* checkpoint (the delta, if incremental) | growing steadily = state leak |
| **Full Checkpoint Data Size** | total logical state size | this is what drives restore time |
| **Processed / Persisted in-flight data** | unaligned checkpoints only | non-zero means unaligned kicked in |

Click a row to expand the **per-subtask** breakdown, which is where the real answer is:

| Column | Meaning | Diagnosis |
|---|---|---|
| **Sync Duration** | the blocking part of the snapshot | huge heap state or a very large number of timers → move to RocksDB |
| **Async Duration** | uploading to durable storage | state too large, slow/throttled object store, too many small files → enable incremental |
| **Alignment Duration** | gap between the first and last barrier arriving | channel **skew**, or backpressure on one path |
| **Start Delay** | JM trigger → first barrier reaching this subtask | **backpressure** — the barrier is queued behind data |

```
Checkpoint slow?
  ├─ Start Delay big?   → backpressure. Fix the pipeline, not the checkpoint config.
  ├─ Alignment big?     → skew, or backpressure on one path.
  ├─ Sync big?          → state backend choice / timer count.
  └─ Async big?         → state size, storage throughput. Go incremental.
```

The full causal tree with code fixes for each branch is [`../../01-checkpointing-slow.md`].

### Summary

Min / average / maximum for each of those columns across the retained history. Use it to spot a *trend*: an average async duration that has doubled over a week is the early warning for the incident you will have next week.

### Configuration

The job's effective checkpoint settings — interval, min pause, timeout, mode (EXACTLY_ONCE / AT_LEAST_ONCE), unaligned enabled, max concurrent, retention policy, and the persistence directory. **Check this first when a job is not checkpointing at all**; the usual answer is that `enableCheckpointing` was never called, and this tab says "Checkpointing is not enabled".

---

## The Watermarks tab

Per vertex → **Watermarks**. One row per subtask, showing that subtask's current watermark as a timestamp.

```
Subtask   Watermark
   1      2025-08-29 14:22:10
   2      2025-08-29 14:22:11
   3      2025-08-29 14:22:09
   4      -9223372036854775808     ← 🔴 Long.MIN_VALUE = NO watermark ever emitted
```

Three failure shapes:

1. **`Long.MIN_VALUE` (`-9223372036854775808`)** on a subtask → it has never emitted a watermark. Almost always an **idle source partition**: this subtask owns a Kafka partition with no traffic. Because a downstream operator's watermark is the **minimum** over its inputs, this one subtask freezes event time for the entire job and no window ever fires. Fix: `WatermarkStrategy.…​.withIdleness(Duration.ofMinutes(1))`.
2. **Watermarks far behind wall clock** and not advancing → the source is lagging, or your `forBoundedOutOfOrderness` bound is enormous.
3. **One subtask's watermark much lower than the others** → skew, or one slow partition. The job's effective event time is that minimum.

> **Key idea**
> A downstream operator's watermark is the **minimum across all its input channels**. One stalled subtask stalls the whole job. This is why the Watermarks tab is where you look when a windowed job produces nothing.

---

## TaskManagers

The tab for "is the JVM itself the problem?".

| Sub-tab | What it gives you |
|---|---|
| **Metrics** | heap used/max, direct memory, managed memory, GC count and time, CPU load |
| **Logs** | the TaskManager's `.log` file, in the browser |
| **Stdout** | the `.out` file — where `print()` goes |
| **Thread Dump** | a live stack dump of every thread |

What to look for:

- **Heap used near max, and GC Time climbing fast** → heap pressure. If GC time per interval approaches the interval itself, the JVM is spending all its time collecting and your job is effectively stopped.
- **Direct memory near its limit** → network buffers, or a native library. See the OOM triage in [ch. 59](59-tuning-parallelism-memory-checkpoints.md).
- **Thread Dump** is the tool for a *stuck* job — one that is neither busy nor backpressured nor progressing. Take two dumps 30 seconds apart. If the same thread is in the same stack frame, that is your hang. Look for your own code in the `Legacy Source Thread` / `Flink Task` threads, and for anything sitting in `SocketInputStream.read` (a blocking call in the hot path).

---

## Exceptions

**Root Exception** is the exception that caused the most recent failure; **Exception History** is the last N failures with timestamps and the failing task.

Two things people get wrong:

1. **The root exception is often not the root cause.** `FetchFailedException` or `Connection unexpectedly closed` means *some other TaskManager died first*. Find the earliest exception in the history, then read that TaskManager's log around that timestamp.
2. **`RESTARTING` with a repeating exception is a crash loop.** Note the interval — if it matches your restart backoff, the job is not making progress and each restart re-downloads state, which can hammer S3 and make things worse.

---

## The REST API

Everything the UI shows comes from a REST API on the same port. That makes it scriptable. All of these are GET unless noted.

```bash
JM=http://localhost:8081

# ---- Cluster ----
curl -s $JM/overview | jq
# {"taskmanagers":4,"slots-total":16,"slots-available":0,
#  "jobs-running":2,"jobs-finished":0,"jobs-cancelled":0,"jobs-failed":0,
#  "flink-version":"1.20.0", ...}

curl -s $JM/taskmanagers | jq '.taskmanagers[] | {id, slotsNumber, freeSlots}'

# ---- Jobs ----
curl -s $JM/jobs | jq
# {"jobs":[{"id":"4a3f00b1c9e2f...","status":"RUNNING"}]}

JID=$(curl -s $JM/jobs | jq -r '.jobs[0].id')

# Full job detail: vertices, per-vertex metrics, parallelism, duration
curl -s $JM/jobs/$JID | jq '{name, state, vertices: [.vertices[] | {name, status, parallelism, metrics}]}'

# Just the state - the cheapest health check you can write
curl -s $JM/jobs/$JID | jq -r '.state'
# RUNNING

# ---- Checkpoints ----
curl -s $JM/jobs/$JID/checkpoints | jq '.counts'
# {"restored":0,"total":1204,"in_progress":1,"completed":1198,"failed":5}

curl -s $JM/jobs/$JID/checkpoints | jq '.latest.completed |
    {id, end_to_end_duration, state_size, external_path}'

# Per-subtask detail for one checkpoint
curl -s $JM/jobs/$JID/checkpoints/details/1198 | jq

# ---- Arbitrary metrics (ch. 56) ----
# 1. list available metric names for a vertex
VID=$(curl -s $JM/jobs/$JID | jq -r '.vertices[0].id')
curl -s "$JM/jobs/$JID/vertices/$VID/metrics" | jq -r '.[].id' | head -30

# 2. fetch specific ones, comma-separated
curl -s "$JM/jobs/$JID/vertices/$VID/metrics?get=0.numRecordsInPerSecond,0.busyTimeMsPerSecond" | jq
# the "0." prefix is the subtask index

# ---- TRIGGERING A SAVEPOINT (POST — asynchronous) ----
REQ=$(curl -s -XPOST $JM/jobs/$JID/savepoints \
  -H 'Content-Type: application/json' \
  -d '{"target-directory":"s3://my-bucket/flink/savepoints","cancel-job":false}' \
  | jq -r '.["request-id"]')

# Poll until it completes - savepoints are NOT instant
curl -s $JM/jobs/$JID/savepoints/$REQ | jq
# {"status":{"id":"IN_PROGRESS"}}   ... then ...
# {"status":{"id":"COMPLETED"},
#  "operation":{"location":"s3://my-bucket/flink/savepoints/savepoint-4a3f00-1c9e2f"}}

# ---- Stop with savepoint (the graceful upgrade path) ----
curl -s -XPOST $JM/jobs/$JID/stop \
  -H 'Content-Type: application/json' \
  -d '{"targetDirectory":"s3://my-bucket/flink/savepoints","drain":false}'

# ---- Cancel (ungraceful) ----
curl -s -XPATCH "$JM/jobs/$JID?mode=cancel"
```

Two API notes: `POST /jobs/:id/savepoints` returns a **request-id**, not a path — savepointing is asynchronous and you must poll `GET /jobs/:id/savepoints/:request-id`. And `GET /jobs/:id/exceptions` gives you the exception history as JSON, which is what you want in an automated post-incident report.

A minimal health check you can wire into anything:

```bash
#!/usr/bin/env bash
# exits non-zero if the job is not RUNNING
set -euo pipefail
JM=${1:?usage: healthcheck.sh <jobmanager-url> <job-name>}
NAME=${2:?}
STATE=$(curl -sf "$JM/jobs/overview" \
        | jq -r --arg n "$NAME" '.jobs[] | select(.name==$n) | .state')
[[ "$STATE" == "RUNNING" ]] || { echo "job $NAME is $STATE"; exit 1; }
```

---

## What to alert on

Five alerts cover most real incidents. Everything else is a dashboard, not a page.

### 1. Consumer lag growing — the one alert you cannot skip

```yaml
# Prometheus alerting rule
- alert: FlinkConsumerLagGrowing
  expr: |
    max by (job_name) (
      flink_taskmanager_job_task_operator_records_lag_max
    ) > 1000000
    and
    deriv(
      max by (job_name) (
        flink_taskmanager_job_task_operator_records_lag_max
      )[15m:]
    ) > 0
  for: 15m
  labels: {severity: page}
  annotations:
    summary: "{{ $labels.job_name }} lag > 1M and rising for 15m"
```

Two conditions, deliberately. **Absolute lag alone is noisy** — a restart always spikes it, and it drains fine. **The slope is what matters:** rising lag means the drain rate is negative and the job will never catch up on its own. Use `max` across subtasks, never `avg` — one hot-key subtask averages away.

The threshold should be *your* threshold: pick the lag at which your business SLA breaks, using the drain-rate arithmetic in [`../../06-scale-arithmetic.md`].

### 2. Checkpoint failures

```yaml
- alert: FlinkCheckpointsFailing
  expr: increase(flink_jobmanager_job_numberOfFailedCheckpoints[15m]) > 2
  for: 5m
  labels: {severity: page}
```

Two failures in 15 minutes is not noise. And a corollary alert — **no checkpoint completed recently** — catches the hang that a failure counter misses:

```yaml
- alert: FlinkNoRecentCheckpoint
  expr: |
    time() - (flink_jobmanager_job_lastCheckpointExternalPath_timestamp / 1000) > 900
  for: 5m
# Simpler and usually sufficient:
# flink_jobmanager_job_lastCheckpointDuration > <3 × your checkpoint interval, in ms>
```

### 3. Restart count

```yaml
- alert: FlinkJobRestarting
  expr: increase(flink_jobmanager_job_numRestarts[10m]) > 3
  for: 1m
  labels: {severity: page}
```

More than 3 restarts in 10 minutes is a crash loop. Also alert on **uptime resetting**, and on the job not existing at all:

```yaml
- alert: FlinkJobDown
  expr: flink_jobmanager_numRunningJobs < 1
  for: 2m
  labels: {severity: page}
```

### 4. Sustained backpressure

```yaml
- alert: FlinkSustainedBackpressure
  expr: |
    avg by (job_name, task_name) (
      flink_taskmanager_job_task_backPressuredTimeMsPerSecond
    ) > 500
  for: 30m
  labels: {severity: ticket}
```

500 ms/s = backpressured half the time. **`for: 30m` is deliberate** — short backpressure bursts are normal and self-correcting. Ticket severity, not page: a job that is backpressured but keeping up is a capacity conversation, not a 3am problem. The lag alert pages if it becomes one.

### 5. Watermark lag

```yaml
- alert: FlinkWatermarkStalled
  expr: |
    max by (job_name) (
      flink_taskmanager_job_task_operator_currentEmitEventTimeLag
    ) > 600000
  for: 10m
  labels: {severity: page}
```

Event-time lag over 10 minutes means windows are firing 10 minutes late, or not at all. This catches the idle-partition failure that the throughput metrics happily report as healthy — records flowing, watermark frozen, zero output.

### Supporting alerts (ticket, not page)

```yaml
# State leak: checkpoint size doubling over a week with flat traffic
- alert: FlinkStateGrowing
  expr: |
    flink_jobmanager_job_lastCheckpointFullSize
      > 2 * (flink_jobmanager_job_lastCheckpointFullSize offset 7d)
  for: 1h

# JVM heap pressure
- alert: FlinkHighGcTime
  expr: |
    rate(flink_taskmanager_Status_JVM_GarbageCollector_G1_Old_Generation_Time[5m]) > 0.2
  for: 15m
  # 0.2 = 20% of wall-clock time spent in old-gen GC

# TaskManagers disappearing
- alert: FlinkTaskManagersMissing
  expr: flink_jobmanager_numRegisteredTaskManagers < 4
  for: 5m
```

### The dashboard, one row per question

```
Row 1  IS IT ALIVE?      numRestarts · uptime · numRunningJobs · registered TMs
Row 2  IS IT KEEPING UP? records-lag-max (max) · numRecordsInPerSecond
                         · currentEmitEventTimeLag
Row 3  WHERE IS IT SLOW? busyTime / backPressuredTime / idleTime, per operator
Row 4  IS STATE OK?      lastCheckpointDuration · lastCheckpointFullSize
                         · numberOfFailedCheckpoints
Row 5  IS THE JVM OK?    heap used/max · GC time · direct memory · CPU load
```

Rows 1 and 2 answer "should I care?". Rows 3–5 answer "why?".

---

## Remember

- Diagnostic order: **Overview → Job graph → Backpressure → Checkpoints → TaskManagers/Exceptions.**
- **`Records sent: 0` on a windowed operator** = watermark problem, not a filter bug.
- Chained operators show `Records Received: 0` internally — read the chain **boundaries**, not the interior.
- Backpressure: **the bottleneck is the first non-backpressured operator going downstream.** Red boxes upstream are victims. Full method in [`../../02-backpressure.md`].
- **Never quote End-to-End checkpoint Duration alone.** Decompose into **Start Delay / Alignment / Sync / Async** — each has a different fix ([`../../01-checkpointing-slow.md`]).
- Watermark showing **`-9223372036854775808`** = that subtask never emitted one. Usually an idle Kafka partition; fix with `withIdleness`.
- A watermark is the **minimum** across inputs. One stalled subtask stalls the whole job.
- **GC pauses look exactly like backpressure.** Rule out GC on the TaskManagers tab before restructuring anything.
- The **Checkpoints → Configuration** tab is the first place to look when nothing is checkpointing.
- The REST API mirrors the UI: `/overview`, `/jobs`, `/jobs/:id`, `/jobs/:id/checkpoints`, `POST /jobs/:id/savepoints` (returns a **request-id** to poll).
- Alert on five things: **lag rising**, **checkpoint failures**, **restart count**, **sustained backpressure**, **watermark lag**. Lag is the one you cannot skip.

**Interview one-liners**

- *"A job is lagging — walk me through the UI."* → Overview for slots and job state, then the graph for records in/out per edge, then the Backpressure tab to find the first non-backpressured operator, then Checkpoints to see whether state is the cause or the victim, then TaskManagers for GC.
- *"The job runs but produces no output."* → Watermarks tab. A subtask at `Long.MIN_VALUE` means an idle partition is holding event time back, so no window ever fires. Fix with `withIdleness`.
- *"What would you alert on for a Flink job?"* → Kafka `records-lag-max` rising over a sustained window, checkpoint failures, restart count, backpressure sustained for 30+ minutes, and event-time lag. Rising lag is the one that always matters.
- *"Why alert on the lag slope, not the absolute value?"* → Absolute lag spikes on every restart and drains fine; a positive slope means the drain rate is negative and the job cannot recover without intervention.
- *"How would you automate a savepoint?"* → `POST /jobs/:id/savepoints` with a target directory, then poll `GET /jobs/:id/savepoints/:request-id` until COMPLETED and read the location out of the response.

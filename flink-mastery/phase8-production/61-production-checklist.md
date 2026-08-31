# 61. Production Checklist

A go-live checklist. Dense on purpose — scan it before every deploy, and work through it fully before the first one.

> **Key idea**
> Every item below exists because it has silently destroyed someone's data or throughput. The dangerous ones are the items whose failure mode is **not an error** — a missing `uid()`, a missing TTL, a buffering sink that does not flush. Those jobs look perfectly healthy and are wrong.

---

## Correctness

- [ ] **`uid()` on every stateful operator.** Not `name()` — `name()` is display only. Without a stable uid, Flink derives ids from graph structure, so **adding one `filter()` upstream orphans all downstream state** and the job restores "fine" with empty state. Silent data loss. ([`../../03-state-and-skew.md`])
- [ ] **`uid()` values are unique** across the job and will not change. Treat them like database column names.
- [ ] **Event time vs processing time decided explicitly**, and the timestamp assigner reads the right field.
- [ ] **Watermark strategy set on the source**, not bolted on later, so per-partition watermarks work correctly.
- [ ] **`withIdleness(...)` set** if any Kafka partition can go quiet. Without it, one idle partition holds the watermark at `Long.MIN_VALUE` and **no window ever fires**. ([ch. 58](58-web-ui-and-monitoring.md))
- [ ] **Out-of-orderness bound justified.** It is usually the largest term in your latency budget ([`../../06-scale-arithmetic.md`]). Measure real lateness; do not guess 5 seconds.
- [ ] **Allowed lateness decided**, and late records routed to a **side output** rather than silently dropped. Somebody must be able to answer "how many did we drop yesterday?"
- [ ] **Exactly-once vs at-least-once decided**, written down, and the **sink matches**. `EXACTLY_ONCE` checkpointing with a non-transactional sink is at-least-once, whatever the config says.
- [ ] **If exactly-once with 2PC:** the checkpoint interval is accepted as a **hard floor on end-to-end latency**, and the Kafka `transaction.timeout.ms` exceeds your maximum expected downtime (or transactions expire and you lose data).
- [ ] **`disableGenericTypes()` run at least once in a test** to prove nothing silently falls back to Kryo (~10× slower). ([`../../02-backpressure.md`])
- [ ] **POJOs are real POJOs**: public class, public no-arg constructor, public fields or getter+setter.
- [ ] **Null handling**: null join keys are the most common source of real-world skew. Filter or bucket them deliberately.

## State

- [ ] **TTL on every state with an unbounded keyspace.** A `ValueState` keyed by `sessionId` or `orderId` grows forever. This is the #1 cause of "checkpoints got slow after three weeks".
      ```java
      StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(7))
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
              .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
              .cleanupInRocksdbCompactFilter(1000)   // ← without this, RocksDB
              .build();                              //   only clears on ACCESS
      descriptor.enableTimeToLive(ttl);
      ```
      **The caveat:** without an explicit cleanup strategy, expired state is only removed when it is read. A key never touched again is never cleaned up. ([`../../01-checkpointing-slow.md`])
- [ ] **Or explicit timer-based cleanup** — a timer per key that clears state. Then check you are **not leaking timers**: every registered timer must be deleted or fire.
- [ ] **`maxParallelism` set deliberately** (1024 / 4096), at first run, and never changed. Default 128 permanently caps you at 128 subtasks.
- [ ] **Parallelism divides `maxParallelism`**, or you accept permanent key-group imbalance.
- [ ] **State backend chosen for the state size**: `HashMapStateBackend` under a few GB per subtask; **RocksDB** above that. Large heap = long GC pauses that look exactly like backpressure.
- [ ] **RocksDB: `state.backend.incremental: true`.**
- [ ] **RocksDB: `state.backend.rocksdb.localdir` points at real local NVMe**, not the `/tmp` default (often tmpfs, i.e. RAM, or a slow root volume).
- [ ] **Local disk sized at 2–3× state** for compaction headroom.
- [ ] **Windows use `reduce`/`aggregate`, not `process`/`apply`**, unless you genuinely need every record. `process` buffers the entire window in state. ([`../../03-state-and-skew.md`])
- [ ] **State size growth measured over a week** in staging, not assumed.

## Reliability

- [ ] **Checkpointing enabled.** Off by default. Check the Web UI → Checkpoints → Configuration tab.
- [ ] **Checkpoint interval and `minPauseBetweenCheckpoints` both set.** Min-pause ≈ half the interval prevents the death spiral where the job spends all its time checkpointing. ([ch. 59](59-tuning-parallelism-memory-checkpoints.md))
- [ ] **`setCheckpointTimeout`** set so a hung checkpoint fails instead of hanging forever.
- [ ] **`setTolerableCheckpointFailureNumber(n)`** so one transient S3 blip does not fail the job.
- [ ] **`RETAIN_ON_CANCELLATION`.** Otherwise cancelling the job **deletes your only recovery point**.
      ```java
      env.getCheckpointConfig().setExternalizedCheckpointRetention(
              ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
      ```
- [ ] **`state.checkpoints.dir` and `state.savepoints.dir` both configured** and pointing at **durable, shared** storage (S3/HDFS/GCS). Local disk here is guaranteed data loss.
- [ ] **Savepoint dir set** — required for the Kubernetes Operator's `upgradeMode: savepoint` to work at all.
- [ ] **Restart strategy set to exponential backoff with jitter.** A zero-delay fixed restart hammers S3 and never recovers.
- [ ] **High availability configured** (`high-availability.type: kubernetes` or `zookeeper`) so a JobManager restart resumes the job rather than losing it.
- [ ] **Buffering sinks flush in `snapshotState()`.** A sink that buffers and does not flush has **silently broken exactly-once** — the checkpoint claims a record was processed while it sits in a heap buffer that vanishes on restart. ([`../../02-backpressure.md`])
- [ ] **Restore from savepoint tested**, with a savepoint of realistic size, and the restore time measured. This is where you learn it takes 40 minutes.
- [ ] **Rollback plan written down**: previous jar version + savepoint path.

## Performance

- [ ] **Parallelism derived, not guessed**: `throughput ÷ per-subtask rate`, with **~2× headroom** so you can drain a backlog. A job at exactly 100% of the incoming rate can never recover from an outage. ([`../../06-scale-arithmetic.md`])
- [ ] **Source parallelism ≤ Kafka partition count**, and ideally divides it evenly.
- [ ] **Total slots ≥ max operator parallelism**, or the job sits in `CREATED`.
- [ ] **No blocking I/O in the hot path.** A 20 ms synchronous call caps a subtask at ~50 records/sec — four orders of magnitude below a plain map. Use **AsyncIO**, a local cache, broadcast state, or pre-join upstream.
- [ ] **`taskmanager.memory.process.size` equals the container memory limit.** Larger and Kubernetes `OOMKilled`s the pod with no Java exception.
- [ ] **`managed.fraction` matched to the state backend**: 0.4–0.7 with RocksDB, ~0.05 with HashMap (where it is pure waste).
- [ ] **Heap kept to 4–8 GB per TaskManager.** A full GC on a 32 GB heap is 1–10 seconds and is indistinguishable from backpressure in the UI.
- [ ] **No unnecessary shuffles.** Every `keyBy`/`rebalance`, and **every parallelism change between adjacent operators**, breaks the chain and costs serialize + network + deserialize.
- [ ] **Skew checked** in staging: Subtasks tab, max/median records received under ~5×.
- [ ] **Load-tested at 2× expected peak**, not at expected peak.

## Observability

- [ ] **Metrics reporter configured** (Prometheus on `9249-9259`), and you have **verified the endpoint returns data** with `curl`. ([ch. 56](56-logging-and-metrics.md))
- [ ] **Alert: consumer lag rising** — `records-lag-max`, using `max` across subtasks and requiring a **positive slope**, not just a threshold. The one alert you cannot skip.
- [ ] **Alert: checkpoint failures** (`numberOfFailedCheckpoints` increasing) **and no recent successful checkpoint**.
- [ ] **Alert: restart count** (`numRestarts` > 3 in 10 minutes) and **job not running**.
- [ ] **Alert: sustained backpressure** (`backPressuredTimeMsPerSecond` > 500 for 30+ min). Ticket, not page.
- [ ] **Alert: watermark / event-time lag** (`currentEmitEventTimeLag`). Catches the idle-partition failure that throughput metrics report as healthy.
- [ ] **Dashboard with the five rows**: alive / keeping up / where slow / state ok / JVM ok.
- [ ] **`lastCheckpointFullSize` trended over weeks** — the state-leak early warning.
- [ ] **Log level is INFO, not DEBUG.** `rootLogger.level = DEBUG` in production is a self-inflicted outage.
- [ ] **No per-record logging.** Grep your diff for `LOG.info` inside `processElement`, `map`, `flatMap`, `invoke`.
- [ ] **Logs shipped off the pod** to a real aggregator. File appenders inside a container die with the container — taking the logs of the crash you care about with them.
- [ ] **Custom metrics registered in `open()`**, in `transient` fields, never per record and **never per key** (that takes down Prometheus).
- [ ] **Latency tracking OFF** (`setLatencyTrackingInterval(0)`, the default).

## Deployment

- [ ] **Application mode**, not session mode, for every production streaming job. Blast radius of a failure = one job.
- [ ] **Fat jar verified to contain the connectors**: `jar tf target/*.jar | grep connector/kafka`. ([ch. 54](54-project-structure-and-maven.md))
- [ ] **Fat jar verified NOT to contain Flink core** (`provided` scope worked).
- [ ] **`ServicesResourceTransformer` in the shade config**, and `META-INF/services/...` verified with `unzip -p`. Omitting it makes connectors "disappear" from a jar that visibly contains them.
- [ ] **No secrets in the jar, in git, or in `--args`.** A jar is a zip anyone can open; `--args` show up in the Web UI Configuration tab and the process listing. Use env vars from Kubernetes secrets, or IAM roles. ([ch. 55](55-configuration-and-parameters.md))
- [ ] **Secret-looking keys stripped before `setGlobalJobParameters`.**
- [ ] **Config externalised** — no hardcoded brokers, topics, thresholds, or paths.
- [ ] **Config validated on the client** so a typo fails the submission in milliseconds, not the results three hours later.
- [ ] **Kubernetes Operator: `upgradeMode: savepoint`** (not `stateless`, which throws state away).
- [ ] **Kubernetes: service account has RBAC to manage pods/configmaps**, or the JobManager cannot launch TaskManagers.
- [ ] **Job name includes the environment** so twelve jobs in one UI are distinguishable.
- [ ] **Upgrade procedure is `stop --savepointPath` → deploy → `run -s`**, never `cancel` + restart. ([ch. 57](57-deployment-modes.md))
- [ ] **`--allowNonRestoredState` is NOT in your deploy script.** It silently discards state that does not map to an operator.
- [ ] **Post-deploy verification is part of the runbook**: restarts stay at 0, the first post-restore checkpoint completes, lag is falling, state size matches.

## Testing

- [ ] **`main()` only wires; pipeline logic is in a static `buildPipeline(...)`** taking a source and a sink. ([ch. 60](60-testing-flink-jobs.md))
- [ ] **Unit tests for every stateful function** using operator test harnesses, with `processWatermark` driving event time.
- [ ] **A state-restore test**: `snapshot()` → new harness → `initializeState()` → assert.
- [ ] **A timer-leak assertion** (`numEventTimeTimers()` returns to zero).
- [ ] **An integration test** through the real graph on a MiniCluster — this is what proves the uids exist and the wiring is right.
- [ ] **A recovery test** with an induced failure, asserting no duplicates (proves your sink is idempotent or transactional).
- [ ] **At least one Testcontainers E2E test** covering serialization and the real connectors.
- [ ] **`log4j2-test.properties` in `src/test/resources`** with `rootLogger.level = OFF`, so assertion failures are not buried under MiniCluster logs.
- [ ] **CI runs `mvn clean verify`**, tests not skipped.

---

## Top 10 mistakes beginners make

**1. No `uid()` on stateful operators.**
*Symptom:* you add a `filter()`, redeploy from a savepoint, and the job starts cleanly with **empty state**. No error. Fraud counters at zero, sessions gone.
*Why:* Flink derives operator ids from graph structure. Change the structure, every downstream id changes, state maps to nothing.
*Fix:* `.uid("stable-name")` on every stateful operator, from day one. Never change one.

**2. Unbounded state with no TTL.**
*Symptom:* the job is perfect for three weeks, then checkpoints slow down, then a TaskManager OOMs.
*Why:* `ValueState` keyed by something unbounded — session id, order id, request id — has no expiry by default.
*Fix:* `StateTtlConfig` with **`cleanupInRocksdbCompactFilter`**, or explicit timer cleanup. Chart `lastCheckpointFullSize` to catch it early.

**3. `print()` / `System.out.println` for debugging on a cluster.**
*Symptom:* "my job produces no output."
*Why:* it goes to the **TaskManager's** `.out` file on a machine you are not looking at, not your terminal.
*Fix:* `private static final Logger LOG` and read TaskManager logs — or metrics, which is what you actually wanted.

**4. Logging per record.**
*Symptom:* an operator pinned at 100% busy doing trivial work; throughput an order of magnitude below expectation.
*Why:* the log appender synchronises and writes to disk. At 100k records/sec you built a log-shipping service.
*Fix:* log in `open()`, log exceptions, sample 1-in-N, or use a `Counter`.

**5. Forgetting `env.execute()`.**
*Symptom:* the program exits immediately. No output, **no error**.
*Why:* everything before `execute()` only builds a graph.
*Fix:* call it exactly once, at the end.

**6. Blocking I/O inside `map`/`process`.**
*Symptom:* lag grows; adding parallelism barely helps.
*Why:* a 20 ms synchronous call caps a subtask at ~50 records/sec. Reaching 1M/sec would need 20,000 subtasks.
*Fix:* AsyncIO, a local cache, broadcast state, or enrich upstream. **You cannot solve this with parallelism.**

**7. Assuming more parallelism fixes skew.**
*Symptom:* you double parallelism, cost doubles, the lagging subtask is still lagging.
*Why:* a hot key hashes to one key group, which lives on exactly one subtask, forever.
*Fix:* two-phase aggregation (salting), local pre-aggregation, or route the hot key to its own pipeline. ([`../../03-state-and-skew.md`])

**8. Thinking `EXACTLY_ONCE` checkpointing means exactly-once output.**
*Symptom:* duplicates downstream after every restart, despite the config.
*Why:* checkpointing mode governs **state**. Output is only exactly-once if the **sink** is transactional or idempotent.
*Fix:* transactional sink (Kafka 2PC), or an idempotent one (upsert on a primary key). And accept the latency floor at your checkpoint interval.

**9. A buffering sink that does not flush in `snapshotState()`.**
*Symptom:* records silently missing after a restart, in a job configured for exactly-once.
*Why:* the checkpoint records "processed up to offset X" while X still sits in a heap buffer that vanishes on restart.
*Fix:* flush **before** the checkpoint completes, or checkpoint the buffer into `ListState`. ([`../../02-backpressure.md`])

**10. `--allowNonRestoredState` to get past a restore error.**
*Symptom:* the restore that was failing now succeeds — and the job runs with empty state.
*Why:* the flag means "discard state you cannot map". The real cause was usually a changed `uid()`.
*Fix:* find out **why** the state does not map. Only use the flag when you deliberately removed a named operator and can say which.

**Honourable mentions:** non-static `Logger` (`NotSerializableException`); `process()` on an hour-long window buffering 3.6 billion records; forgetting `withIdleness` so an idle partition freezes event time; leaving `state.backend.rocksdb.localdir` on `/tmp`; `maxParallelism` left at 128 and discovered eighteen months later.

---

## Where to go next

**Operational deep-dives** — the parent folder. These are the interview-and-incident notes; this phase is the build-and-deploy view.

| File | When to open it |
|---|---|
| [`../../01-checkpointing-slow.md`] | Checkpoints slow or failing. The full causal tree: backpressure → alignment → state size → unbounded state → config self-sabotage, with the fix for each. |
| [`../../02-backpressure.md`] | Lag growing, and you need to find the bottleneck. The "first non-backpressured operator" rule, plus the four causes and their code. |
| [`../../03-state-and-skew.md`] | One subtask slower than the rest; slow recovery; rescaling. Key groups, skew fixes beyond salting, region failover, savepoints vs checkpoints. |
| [`../../04-exactly-once.md`] | Choosing delivery guarantees and matching the sink. |
| [`../../05-watermarks-and-time.md`] | Windows not firing, late data, event-time semantics. |
| [`../../06-scale-arithmetic.md`] | Any "can you handle N events/sec" question. The five numbers to compute, the throughput table, and the numbers worth memorising. |

**Within this course**

- Mechanism behind the checkpoint tuning here: `phase5-reliability/32-checkpoints-how-they-work.md`.
- Execution model behind the parallelism arithmetic: `phase1-foundations/03-first-job-and-execution-model.md`.

**Reading order for an interview:** [`../../06-scale-arithmetic.md`] → [`../../02-backpressure.md`] → [`../../01-checkpointing-slow.md`] → [`../../03-state-and-skew.md`] → this chapter's "top 10 mistakes".

---

## Remember

- The dangerous failures are **silent**: missing `uid()`, missing TTL, a non-flushing sink, `--allowNonRestoredState`. None of them throws.
- **`uid()` on every stateful operator, from day one.** It is the cheapest item on this list and the most expensive to retrofit.
- **TTL or timers on every unbounded keyspace**, with an explicit RocksDB cleanup strategy.
- **`RETAIN_ON_CANCELLATION`** or cancelling deletes your recovery point.
- **Exactly-once is a property of the sink**, not of the checkpointing mode.
- **Size for 2× peak.** Headroom, not throughput, determines how fast you recover.
- **Test the restore before you need it**, with realistic state.
- **Verify the jar and verify the deploy.** `jar tf` before; restarts / first checkpoint / lag / state size after.

**Interview one-liners**

- *"What's the first thing you'd check on a Flink job going to production?"* → That every stateful operator has an explicit `uid()`, and that every unbounded-keyspace state has a TTL or timer cleanup. Both fail silently, which makes them the expensive ones.
- *"How do you upgrade a Flink job safely?"* → `stop --savepointPath`, deploy, `run -s <savepoint>`, then verify restarts are zero, the first post-restore checkpoint completes, lag is falling, and state size matches the pre-upgrade size.
- *"Your job has been perfect for three weeks and now checkpoints are slow."* → Unbounded state with no TTL. Confirm with `lastCheckpointFullSize` trending up while input rate is flat.
- *"You configured EXACTLY_ONCE but see duplicates."* → Checkpointing mode governs state consistency. The sink must be transactional or idempotent for output to be exactly-once.
- *"What single metric would you alert on?"* → `records-lag-max`, on a rising slope rather than an absolute threshold, taken as the max across subtasks.

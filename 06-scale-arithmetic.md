# Scale Arithmetic — Making "1 Million Events/Sec" Concrete

> The question is never really *"can you handle 1M events/sec?"*
> It's *"can you reason quantitatively instead of hand-waving?"*

Candidates who say "we'd scale horizontally" lose. Candidates who do division on the spot
win. This note is the arithmetic.

---

## The five numbers to compute immediately

Given any throughput target, derive these out loud:

```
1. BYTES/SEC       = events/sec × avg event size
2. PARALLELISM     = events/sec ÷ per-subtask throughput
3. STATE SIZE      = key cardinality × state per key
4. CHECKPOINT RATE = state size ÷ checkpoint interval
5. NETWORK         = bytes/sec × number of shuffles
```

Worked example — 1M events/sec, 1 KB events:

```
1. BYTES/SEC   = 1M × 1KB           = 1 GB/sec       = 8 Gbit/sec
                                       ⚠️ saturates a 10GbE NIC. Network is a real constraint.

2. PARALLELISM = 1M ÷ 20k per subtask = 50 subtasks   (if pure CPU work, no I/O)
                 1M ÷ 50  per subtask = 20,000        (if a 20ms blocking call — impossible)
                                       ⚠️ the I/O version is the whole ballgame

3. STATE       = 100M users × 1KB      = 100 GB
                                       ⚠️ must be RocksDB; will not fit on heap

4. CHECKPOINT  = 100GB ÷ 60s           = 1.7 GB/sec sustained to S3
                                       ⚠️ not achievable → incremental is mandatory

5. NETWORK     = 1 GB/sec × 2 shuffles = 2 GB/sec cluster-wide
```

Every ⚠️ is a design constraint you just discovered in about 30 seconds of arithmetic. This
is exactly the reasoning the interviewer is testing.

---

## Rules of thumb (approximate, but defensible)

| Operation | Per-subtask throughput |
|---|---|
| Simple map/filter, POJO serde | 100k–1M events/sec |
| Kryo serialization | 10k–100k events/sec |
| Keyed state access, RocksDB, local NVMe | 10k–100k events/sec |
| Keyed state access, RocksDB, network disk | 1k–10k events/sec |
| Blocking I/O, 20ms | **50 events/sec** |
| AsyncIO, 20ms, capacity 1000 | ~50k events/sec |
| Window aggregate (incremental) | 100k–500k events/sec |
| Window `process` (buffers all records) | depends on window size — often the bottleneck |

The 20,000x gap between "blocking I/O" and "simple map" is the most important row in this
table. Say it explicitly: *"a synchronous call in the hot path costs you four orders of
magnitude."*

---

## Latency budget thinking

```
End-to-end latency =
    producer batching (linger.ms)          ~5-100ms
  + Kafka write + replication              ~5-20ms
  + Flink source poll interval             ~1-50ms
  + network shuffle per keyBy              ~1-10ms each
  + watermark out-of-orderness bound       ← usually DOMINATES (seconds!)
  + window size                            ← if windowed, you wait for the window
  + checkpoint interval                    ← if EXACTLY_ONCE 2PC sink
  + sink batching                          ~10-100ms
```

The insight: **your out-of-orderness bound and window size dwarf everything else.** People
try to shave milliseconds off serialization while a 5-second watermark bound sits in the
budget. If someone asks "how do we reduce latency," look at those two first.

And the trade from [[04-exactly-once]]: transactional exactly-once puts a **hard floor** at
your checkpoint interval. You cannot have 100ms end-to-end latency and 60s-checkpoint 2PC.
Those requirements are mutually exclusive — say so rather than trying to satisfy both.

---

## Sizing a cluster

```
TaskManagers = total parallelism ÷ slots per TM

Per TaskManager, budget:
  - 1 CPU core per slot (roughly; more for compute-heavy)
  - 4-8 GB heap per TM   (NOT more — big heaps = long GC pauses)
  - managed memory for RocksDB: 30-50% of total TM memory
  - local NVMe sized at 2-3x your per-TM state (compaction needs headroom)
```

```yaml
# flink-conf.yaml — a defensible starting point at this scale
taskmanager.numberOfTaskSlots: 4
taskmanager.memory.process.size: 16g
taskmanager.memory.managed.fraction: 0.4    # RocksDB block cache + write buffers
state.backend.rocksdb.localdir: /mnt/nvme/rocksdb    # ⚠️ NOT the default /tmp
state.backend.incremental: true
```

`state.backend.rocksdb.localdir` defaulting to `/tmp` is a classic production landmine —
`/tmp` is often a small tmpfs (in RAM!) or a slow root volume. Pointing it at real local NVMe
is frequently the single biggest performance fix on a struggling job.

---

## Kafka-side arithmetic

```
partitions ≥ Flink parallelism        ← else some subtasks are idle forever
                                        (and stall watermarks — see [[05-watermarks-and-time]])

per-partition throughput ≈ 10-50 MB/sec
→ 1 GB/sec needs ≥ 20-100 partitions minimum, before considering parallelism
```

```
consumer lag (records) ÷ processing rate = time to catch up

Example: 50M records behind, processing 1.2M/sec while ingesting 1M/sec
  → drain rate = 200k/sec
  → 50M ÷ 200k = 250 seconds to recover

If drain rate ≤ 0, you never catch up. That's the number that matters during an incident.
```

That last calculation is a great one to volunteer: **headroom, not throughput, determines
recovery.** A job running at exactly 100% of incoming rate can never recover from a backlog.
You need to provision for the drain rate, not the steady-state rate. Rule of thumb: size for
~2x peak so you can recover from an outage in reasonable time.

---

## Cost of a shuffle

```java
// each keyBy at 1M events/sec, 1KB events:
//   serialize 1M objects/sec        ~0.5-2 cores per subtask
//   1 GB/sec across the network
//   deserialize on the other side   ~0.5-2 cores per subtask
```

```java
// ❌ three shuffles
stream.keyBy(A).process(f1)
      .keyBy(B).process(f2)
      .keyBy(A).process(f3);     // back to A — the middle shuffle cost you twice

// ✅ group by key to shuffle once
stream.keyBy(A).process(combinedF1F3)
      .keyBy(B).process(f2);
```

Reordering operators to minimize shuffles is a legitimate, concrete optimization to offer.

---

## The framework for answering any scale question

1. **Restate the numbers** — "1M/sec, and what's the average event size and key cardinality?"
   (Asking for event size and cardinality is itself a strong signal — you can't size
   anything without them.)
2. **Compute bytes/sec and state size** — surfaces the hard physical constraints
3. **Find the per-record cost** — is there I/O? serialization? state access?
4. **Divide** — required parallelism = throughput ÷ per-subtask rate
5. **Sanity-check against physics** — NIC bandwidth, S3 throughput, disk IOPS
6. **Name the binding constraint** — there's always exactly one that matters most
7. **State the trade you're making** — latency vs correctness vs cost

Step 6 is the one that impresses. Every system has one binding constraint; identifying it
("at this scale your problem isn't CPU, it's that 100GB of state can't be fully checkpointed
in 60 seconds") is what senior engineers do.

---

## Numbers worth memorizing

```
1 GbE NIC              ~125 MB/sec
10 GbE NIC             ~1.25 GB/sec
Local NVMe SSD         ~2-7 GB/sec, ~500k IOPS
EBS gp3                ~250 MB/sec, 16k IOPS      ← RocksDB struggles here
S3 PUT                 ~50-100 MB/sec per connection, parallelize for more
Memory bandwidth       ~10-50 GB/sec
JVM full GC (32GB heap) 1-10 SECONDS               ← looks exactly like backpressure
Kafka partition        ~10-50 MB/sec
Cross-AZ network       adds ~1-2ms and real $$$
```

See also: [[01-checkpointing-slow]], [[02-backpressure]], [[04-exactly-once]]

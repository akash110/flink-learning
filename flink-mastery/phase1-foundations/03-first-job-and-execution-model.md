# 3. Your First Job, and How Flink Actually Executes It

## The minimal job

```java
package com.akash.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FirstJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.fromElements("alpha", "beta", "gamma");

        DataStream<String> upper = source.map(s -> s.toUpperCase());

        upper.print();

        env.execute("first-job");
    }
}
```

Line by line:

**`public static void main(String[] args) throws Exception`**
The JVM entry point. `static` because it runs with no object. `throws Exception` because `env.execute()` declares it — do not delete it.

**`StreamExecutionEnvironment.getExecutionEnvironment()`**
Your handle on the engine. The `static` factory method is context-aware: run it in IntelliJ and it returns a **local** environment backed by a MiniCluster; run it via `./bin/flink run` and it returns a **remote** environment wired to the cluster you submitted to. Same code, both places. That is why you never hardcode a cluster address.

**`env.fromElements("alpha", "beta", "gamma")`**
A source that emits three records and finishes. Returns `DataStream<String>` — the generic parameter is the record type. `fromElements` is for learning and tests only; real sources are Kafka, files, sockets.

**`source.map(s -> s.toUpperCase())`**
A transformation. `map` takes a `MapFunction<String, String>`; the lambda supplies it (ch. 2). **This does not run anything yet.**

**`upper.print()`**
A sink that writes each record to stdout of the task that produced it, prefixed by subtask id.

**`env.execute("first-job")`**
**This is the only line that runs anything.** It packages everything above into a JobGraph, ships it to a JobManager, and blocks until the job finishes or fails. The string is the job name shown in the Web UI.

Output:

```
3> ALPHA
4> BETA
1> GAMMA
```

The `3>`, `4>`, `1>` are **subtask indexes** of the print sink. With parallelism 4 (default = your CPU count locally), records are distributed round-robin, so ordering across subtasks is not guaranteed.

---

## The critical idea: building the graph vs executing it

Everything before `env.execute()` is **construction**. `map`, `filter`, `keyBy` are not processing records — they are appending nodes to a graph.

```
main() runs on the CLIENT
────────────────────────────────────────────────
env.fromElements(...)   →  add Source node to graph
   .map(...)            →  add Map node, wire it to Source
   .print()             →  add Sink node, wire it to Map
────────────────────────────────────────────────
env.execute()           →  serialize graph, ship to JobManager,
                           JobManager schedules it,
                           TaskManagers run it,
                           client blocks until done
```

Two consequences you will hit immediately.

### Consequence 1: `main()` runs once, on the client

```java
public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    System.out.println("A: before");          // prints ONCE, on the client, immediately

    DataStream<String> s = env.fromElements("x", "y", "z");

    DataStream<String> m = s.map(v -> {
        System.out.println("B: inside " + v); // prints on a TaskManager, once per record
        return v.toUpperCase();
    });

    System.out.println("C: after building");  // prints ONCE, on the client, BEFORE any B

    m.print();
    env.execute("ordering-demo");

    System.out.println("D: after execute");   // prints ONCE, after the job finishes
}
```

Output (locally, where client and TaskManager share a JVM so you can see everything):

```
A: before
C: after building
B: inside x
B: inside y
B: inside z
1> X
2> Y
3> Z
D: after execute
```

`C` prints before any `B`. That surprises everyone once. `C` is client code; `B` is operator code that has not been shipped yet when `C` runs.

**On a real cluster, `B` does not appear in your terminal at all.** It goes to the TaskManager's stdout, visible in the Web UI (Task Managers → Stdout) or the TM log file. This is exactly why `System.out.println` is useless for debugging distributed jobs — use a logger and look at TaskManager logs.

> **Key idea:** `main()` is a *program that writes a program*. The code inside your functions runs somewhere else, later, many times. Code outside them runs here, now, once.

### Consequence 2: without `env.execute()`, nothing happens

```java
DataStream<String> s = env.fromElements("x");
s.print();
// no env.execute()  →  program exits, zero output, zero errors
```

This is the single most common "why does my Flink job do nothing" question. There is no exception, no warning. You built a graph and threw it away.

The exception to the rule: `print()` on a `DataStream` needs `execute()`, but a few methods **execute implicitly** — `DataStream.executeAndCollect()` and `Table.execute()`. Everything else needs the explicit call.

Also: call `env.execute()` **exactly once**. Calling it twice submits two separate jobs, the second containing whatever you added after the first call.

---

## From your code to running tasks

There are three graph representations. Interviewers ask about them.

```
  YOUR CODE (client)
        │  env.execute()
        ▼
  ┌───────────────┐
  │  StreamGraph  │  1:1 with your API calls. One node per operator.
  └───────┬───────┘
          │  operator chaining: fuse adjacent ops with no shuffle
          ▼
  ┌───────────────┐
  │   JobGraph    │  Chained operators merged into one vertex.
  └───────┬───────┘  This is what is sent over the wire to the JobManager.
          │  parallelization: expand each vertex into N subtasks
          ▼
  ┌───────────────┐
  │ ExecutionGraph│  The JobManager's runtime data structure.
  └───────┬───────┘  One ExecutionVertex per (operator, subtask index).
          │  deploy
          ▼
  ┌───────────────┐
  │ Physical tasks│  Threads running in TaskManager slots.
  └───────────────┘
```

**StreamGraph** — the literal translation of your `map`/`filter`/`keyBy` calls. Built on the client.

**JobGraph** — the StreamGraph after **operator chaining**. If `map` feeds `filter` at the same parallelism with no repartitioning between them, Flink fuses them into one task so the record is passed as a plain method call instead of being serialized and handed to another thread. This is a large performance win and it is why the Web UI often shows fewer boxes than you wrote operators.

**ExecutionGraph** — the JobManager expands each JobGraph vertex into `parallelism` **subtasks** and tracks the state of each (`CREATED → SCHEDULED → DEPLOYING → RUNNING → FINISHED/FAILED`). This is the thing that gets restarted when something dies.

---

## JobManager, TaskManager, slots

```
                    ┌────────────────────────────────┐
    submit          │         JobManager             │
  client ─────────► │  • receives the JobGraph       │
                    │  • builds the ExecutionGraph   │
                    │  • schedules subtasks to slots │
                    │  • coordinates checkpoints     │
                    │  • handles failures/restarts   │
                    └───────────┬────────────────────┘
                                │ deploy tasks / heartbeats
                ┌───────────────┴───────────────┐
                ▼                               ▼
      ┌──────────────────┐            ┌──────────────────┐
      │  TaskManager 1   │            │  TaskManager 2   │
      │  ┌────┐  ┌────┐  │            │  ┌────┐  ┌────┐  │
      │  │slot│  │slot│  │            │  │slot│  │slot│  │
      │  └────┘  └────┘  │            │  └────┘  └────┘  │
      │  managed memory  │            │  managed memory  │
      │  network buffers │◄──────────►│  network buffers │
      └──────────────────┘  shuffle   └──────────────────┘
```

**JobManager** — one per job (in HA setups, one leader plus standbys). It is the coordinator: scheduling, checkpoint triggering, failure recovery. It does **not** process records.

**TaskManager** — a JVM worker process. It runs your operator code, holds your state, and moves records between machines.

**Task slot** — a unit of resource *isolation within a TaskManager*. A TM with `taskmanager.numberOfTaskSlots: 4` can run 4 parallel subtasks. Slots divide the TM's **managed memory** evenly; they do **not** isolate CPU.

Two rules that follow:

1. **Total slots must be ≥ your job's max parallelism**, otherwise the job never gets scheduled and sits in `CREATED` with `NoResourceAvailableException`.
2. **Subtasks of *different* operators from the same job share a slot by default** (slot sharing). So a job that is `source → map → sink` at parallelism 4 needs only 4 slots, not 12. This keeps a full pipeline slice on one machine, cutting network hops.

```
Parallelism 2, three chained-or-not operators, slot sharing on:

  Slot 0: [source-0] → [map-0] → [sink-0]
  Slot 1: [source-1] → [map-1] → [sink-1]
```

Setting parallelism:

```java
env.setParallelism(4);                     // whole job
stream.map(...).setParallelism(2);         // just this operator
sink.setParallelism(1);                    // e.g. force a single output file
```

Precedence: **operator-level > job-level (`env.setParallelism`) > `flink-conf.yaml` `parallelism.default` > CLI `-p`.** (More precisely, the CLI `-p` sets the job default, which an explicit `env.setParallelism` in your code overrides — so if `-p` seems ignored, look for a hardcoded call.)

---

## `print()` — where the output actually goes

```java
stream.print();              // "3> RECORD"
stream.print("tag");         // "tag:3> RECORD"
stream.printToErr();         // same, but stderr
```

The number is the **1-based subtask index** of the print sink. At parallelism 1 the prefix disappears entirely.

Where it lands:

| Where you run | Where `print()` output appears |
|---|---|
| IntelliJ (MiniCluster) | your IDE console — client and TM are the same JVM |
| `./bin/flink run` on a cluster | the **TaskManager's** `.out` file, not your terminal |
| Web UI | Task Managers → pick one → **Stdout** tab |

This trips people up constantly: you submit to a cluster, see no output, and assume the job is broken. It is writing to a machine you are not looking at.

For anything beyond a toy, use a logger instead:

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public static class Logged implements MapFunction<String, String> {
    // static + final: one logger per class, and it is not serialized with the function
    private static final Logger LOG = LoggerFactory.getLogger(Logged.class);

    @Override
    public String map(String v) {
        LOG.info("processing {}", v);   // {} placeholder — cheaper than string concat
        return v.toUpperCase();
    }
}
```

`Logger` is not serializable, which is why it must be `static` — static fields are not part of an object's serialized form.

---

## Bounded vs unbounded, and what "finished" means

`env.fromElements(...)` is a **bounded** source: it emits and terminates, so `env.execute()` returns and your program exits. A Kafka source is **unbounded**: `env.execute()` blocks forever, and the job only ends when you cancel it.

```java
// unbounded: run this and it never returns until you hit stop
DataStream<String> lines = env.socketTextStream("localhost", 9999);
```

Test it with `nc -lk 9999` in another terminal and type lines.

Everything in this course uses `fromElements` so you get deterministic, terminating runs while learning. The API is identical either way — that is Flink's "batch is a bounded stream" claim made concrete.

---

## A job with something to look at

```java
package com.akash.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SecondJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);                       // 2 subtasks per operator

        env.fromElements("flink", "is", "a", "stream", "processor")
           .filter(w -> w.length() > 2)              // drop "is", "a"
           .map(String::toUpperCase)                 // method reference
           .print();

        env.execute("second-job");
    }
}
```

Output:

```
1> FLINK
2> STREAM
1> PROCESSOR
```

Note that `filter` and `map` and `print` are all parallelism 2, all chained together, so the Web UI shows this as **one box**: `Source: Collection Source -> Filter -> Map -> Sink: Print to Std. Out`. That single box is one JobGraph vertex containing three operators.

---

## Remember

- `getExecutionEnvironment()` is context-aware: local MiniCluster in the IDE, remote on a cluster. Never hardcode.
- **Everything before `env.execute()` builds a graph. `execute()` runs it.** No `execute()` → no output, no error.
- `main()` runs **once on the client**. Function bodies run **on TaskManagers, once per record**. `println` in `main` and `println` in a `map` are on different machines at different times.
- StreamGraph → (chaining) → JobGraph → (parallelize) → ExecutionGraph → physical tasks.
- **JobManager coordinates; TaskManagers compute.** Slots are memory/concurrency units inside a TaskManager, not CPU isolation.
- **Slot sharing** means one pipeline slice fits in one slot — a job at parallelism N needs N slots, not N × operators.
- `print()` writes to the TaskManager's stdout, which is **not your terminal** on a real cluster. Use SLF4J and a `static final Logger`.

**Interview one-liners**

- *"What does `env.execute()` do?"* → Compiles the accumulated transformations into a JobGraph, submits it to the JobManager, and blocks. Nothing executes before it.
- *"StreamGraph vs JobGraph vs ExecutionGraph?"* → Logical 1:1 with your code → chained and shipped to the JobManager → parallelized into per-subtask vertices the JobManager schedules and restarts.
- *"What is a task slot?"* → A share of a TaskManager's managed memory and one concurrent subtask. Slot sharing lets one slot hold one subtask of each operator in a job, so required slots = max operator parallelism.
- *"Why do I see fewer operators in the UI than I wrote?"* → Operator chaining fused adjacent operators with no repartitioning into a single task.

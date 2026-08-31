# 57. Deployment Modes

You have a fat jar (ch. 54) and externalised config (ch. 55). Now: how does it actually get onto a cluster, and what are the consequences of each choice?

> **Key idea**
> The deployment modes differ in exactly one question: **where does `main()` run, and does the cluster outlive the job?**
> Everything else — isolation, blast radius, dependency conflicts — follows from that one answer.

---

## The three modes

```
SESSION MODE                    APPLICATION MODE
────────────                    ────────────────
 long-lived cluster              one cluster PER JOB
 many jobs share it              cluster dies with the job

 ┌─ CLIENT (your laptop) ─┐      ┌─ CLIENT ─┐
 │  main() runs HERE      │      │ just     │
 │  builds JobGraph       │      │ uploads  │
 │  uploads jar + graph   │      │ the jar  │
 └──────────┬─────────────┘      └────┬─────┘
            ▼                          ▼
 ┌────────────────────────┐      ┌────────────────────────┐
 │      JobManager        │      │      JobManager        │
 │  jobA  jobB  jobC      │      │  main() runs HERE      │
 │  (shared TMs, shared   │      │  → builds JobGraph     │
 │   classloader risks)   │      │  → runs exactly 1 job  │
 └────────────────────────┘      └────────────────────────┘
```

**Per-Job mode** was a third option: one cluster per job, but `main()` still on the client. It was **deprecated in Flink 1.15 and removed in Flink 2.0**. Application mode replaced it and is strictly better. You will still see `-t yarn-per-job` in old blog posts — do not use it.

---

## Comparison table

| | **Session** | **Per-Job** (deprecated/removed) | **Application** |
|---|---|---|---|
| Where `main()` runs | **client** | **client** | **JobManager** |
| Cluster lifetime | outlives jobs | one per job | one per job |
| Jars in cluster | many jobs' jars in one JM classloader | isolated | isolated |
| Resource isolation | **none** — jobs share TaskManagers | full | full |
| One job OOMs a TM | **kills every job on that TM** | only itself | only itself |
| Client must stay up | while submitting | while submitting | **no** — fire and forget |
| Client network load | **downloads deps, builds graph, uploads** | same | just uploads the jar |
| Startup latency per job | low (cluster is warm) | high | high |
| Multiple `execute()` in main | runs sequentially, client-driven | same | runs on the JM |
| Good for | dev, notebooks, SQL client, many tiny jobs | nothing — use Application | **production streaming** |

> **Recommendation**
> **Application mode for every production streaming job.** Session mode for local development, the SQL client, and ad-hoc exploration.

The reason is the "one job OOMs a TaskManager" row. In session mode, TaskManagers are shared. A single job with a memory leak takes down the slots that other, healthy jobs are running in. In Application mode the blast radius of any failure is exactly one job.

The second reason is the client. In session mode `main()` runs on your laptop or your CI runner: it downloads dependencies, builds the JobGraph, and uploads it. If your CI job's 5-minute timeout fires mid-upload, the submission is in an undefined state. In Application mode the client's only job is to hand the jar over.

---

## Standalone cluster

The simplest thing that works, and what you should run locally.

```bash
# Download and unpack; $FLINK_HOME is the resulting directory.
cd $FLINK_HOME

./bin/start-cluster.sh     # starts 1 JobManager + 1 TaskManager on this machine
                           # Web UI at http://localhost:8081

./bin/stop-cluster.sh      # stops them

# Add another TaskManager to the running cluster (more slots)
./bin/taskmanager.sh start
./bin/taskmanager.sh stop
```

For a real multi-machine standalone cluster you edit two files:

```bash
# conf/workers   — one hostname per line; a TaskManager starts on each
worker-01
worker-02
worker-03

# conf/masters   — JobManager host(s); more than one requires ZooKeeper HA
master-01:8081
```

`start-cluster.sh` then SSHes to each host and starts the processes. This works, needs passwordless SSH, and has no self-healing: if a TaskManager process dies, nothing restarts it. That is exactly the gap Kubernetes fills.

---

## Kubernetes

Two distinct approaches. Know the difference; use the second.

### Native Kubernetes integration

Flink talks to the Kubernetes API itself. The Flink ResourceManager creates TaskManager pods on demand and deletes them when they are idle.

```bash
./bin/flink run-application \
    --target kubernetes-application \
    -Dkubernetes.cluster-id=fraud-detection \
    -Dkubernetes.container.image.ref=my-registry/flink-fraud:1.0.0 \
    -Dkubernetes.namespace=streaming \
    -Dkubernetes.service-account=flink \
    -Dtaskmanager.numberOfTaskSlots=4 \
    -Dkubernetes.jobmanager.cpu=1 \
    -Dkubernetes.taskmanager.cpu=2 \
    -Dtaskmanager.memory.process.size=8192m \
    -Dparallelism.default=8 \
    local:///opt/flink/usrlib/fraud-detection-1.0.0.jar \
    --env prod
```

- **`local://`** is required in Application mode on Kubernetes. It means "this path inside the container image" — the jar must be **baked into the image**, because there is no client to upload it. Building the image is your CI's job:

  ```dockerfile
  FROM flink:1.20.0-scala_2.12-java11
  COPY target/fraud-detection-1.0.0.jar /opt/flink/usrlib/fraud-detection-1.0.0.jar
  ```
- **`kubernetes.cluster-id`** names the Kubernetes resources and is how `flink list`/`cancel` find the cluster later.
- **`kubernetes.service-account`** must have RBAC permission to create pods, or the JobManager cannot launch TaskManagers. This is the #1 first-time failure.

```bash
# Kill it (deletes the whole cluster, since it's application mode)
./bin/flink cancel -t kubernetes-application \
    -Dkubernetes.cluster-id=fraud-detection <jobId>
```

What native mode does **not** give you: declarative deployment, git-ops, automatic savepoint-on-upgrade, or restart on JobManager pod loss. You are running an imperative CLI command against a cluster.

### The Flink Kubernetes Operator — what most companies use

The Operator installs a **controller** into your cluster plus a **Custom Resource Definition** called `FlinkDeployment`. You then `kubectl apply` a YAML describing the desired state, and the controller makes reality match — including taking a savepoint before an upgrade and restoring from it afterwards.

```bash
# Install once, cluster-wide
helm repo add flink-operator-repo \
  https://downloads.apache.org/flink/flink-kubernetes-operator-1.10.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator

kubectl get pods -n default | grep flink-kubernetes-operator
```

### A full `FlinkDeployment`, field by field

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: fraud-detection          # becomes the Flink cluster-id and the pod name prefix
  namespace: streaming
spec:

  # ---- Which Flink to run ----
  image: my-registry/flink-fraud:1.0.0   # your image WITH the fat jar baked in
  flinkVersion: v1_20                    # enum: v1_18, v1_19, v1_20, ...
                                         # tells the operator which API shapes to use

  # ---- Cluster configuration ----
  # These are exactly the keys from conf/config.yaml (ch. 55). The operator
  # renders them into the container's config file.
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"

    # Checkpointing
    execution.checkpointing.interval: "60s"
    execution.checkpointing.min-pause: "30s"
    execution.checkpointing.timeout: "10min"
    execution.checkpointing.mode: "EXACTLY_ONCE"
    execution.checkpointing.max-concurrent-checkpoints: "1"
    execution.checkpointing.externalized-checkpoint-retention: "RETAIN_ON_CANCELLATION"

    # Storage. Both MUST be durable, shared, and reachable from every pod.
    # Local disk here is a guaranteed data-loss bug.
    state.checkpoints.dir: s3://my-bucket/flink/checkpoints/fraud-detection
    state.savepoints.dir:  s3://my-bucket/flink/savepoints/fraud-detection
    # ^ the operator needs state.savepoints.dir to be able to take savepoints
    #   for upgrades. Without it, upgradeMode: savepoint cannot work.

    state.backend.type: "rocksdb"
    state.backend.incremental: "true"

    # High availability: the JobManager stores its metadata in Kubernetes
    # ConfigMaps, so a JobManager pod restart resumes instead of losing the job.
    high-availability.type: kubernetes
    high-availability.storageDir: s3://my-bucket/flink/ha/fraud-detection

    # Restart strategy
    restart-strategy.type: "exponential-delay"
    restart-strategy.exponential-delay.initial-backoff: "10s"
    restart-strategy.exponential-delay.max-backoff: "5min"

    # Metrics (ch. 56)
    metrics.reporters: "prom"
    metrics.reporter.prom.factory.class: >-
      org.apache.flink.metrics.prometheus.PrometheusReporterFactory
    metrics.reporter.prom.port: "9249"

  # ---- Pod resources ----
  serviceAccount: flink          # needs RBAC to manage pods/configmaps

  jobManager:
    resource:
      memory: "2048m"            # container memory limit
      cpu: 1
    replicas: 1                  # >1 only with high-availability.type set

  taskManager:
    resource:
      memory: "8192m"            # must match taskmanager.memory.process.size logic (ch. 59)
      cpu: 2

  # ---- Extra pod spec: secrets, volumes, node selectors ----
  podTemplate:
    spec:
      containers:
        - name: flink-main-container    # this exact name targets the Flink container
          env:
            - name: KAFKA_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: kafka-credentials
                  key: password
          volumeMounts:
            - name: app-config
              mountPath: /etc/flink-app
      volumes:
        - name: app-config
          configMap:
            name: fraud-detection-config

  # ---- The job itself. Omit this whole block for a SESSION cluster. ----
  job:
    jarURI: local:///opt/flink/usrlib/fraud-detection-1.0.0.jar
    entryClass: com.akash.flink.jobs.FraudDetectionJob   # optional if Main-Class is set
    args: ["--env", "prod", "--config-file", "/etc/flink-app/application.properties"]
    parallelism: 8
    state: running               # 'running' or 'suspended' - flip to suspend the job

    # THE MOST IMPORTANT FIELD IN THIS FILE.
    upgradeMode: savepoint
```

### `upgradeMode` — the field that decides whether upgrades lose your state

| Value | What the operator does on a spec change | State preserved? | Requires |
|---|---|---|---|
| `stateless` | cancel, redeploy from scratch | **no** | nothing |
| `savepoint` | **stop-with-savepoint**, redeploy, restore from that savepoint | **yes, cleanly** | `state.savepoints.dir` set, job healthy enough to drain |
| `last-state` | kill the pod, redeploy from the **last completed checkpoint** | yes, but from a checkpoint | HA enabled; checkpoint retention on |

Use **`savepoint`** for planned upgrades: it drains in-flight data and produces a portable, canonical snapshot.

Use **`last-state`** as the fallback when the job is wedged and cannot take a savepoint — the operator will not wait for a drain, so recovery works even from a crash-looping job. Many teams set `savepoint` and rely on the operator's `kubernetes.operator.job.upgrade.last-state-fallback.enabled` (default true) to fall back automatically.

`stateless` is correct only for genuinely stateless jobs. Setting it on a stateful job is a silent data-loss configuration.

Two more useful fields:

```yaml
  job:
    initialSavepointPath: s3://my-bucket/flink/savepoints/savepoint-abc123-def456
    # First deploy only: bootstrap from an existing savepoint (e.g. migrating
    # a job from a standalone cluster).

    allowNonRestoredState: false
    # Keep FALSE. True means "discard state that doesn't map to an operator",
    # which silently empties your state if a uid() changed. See ../../03-state-and-skew.md
```

### Operating it

```bash
kubectl apply -f fraud-detection.yaml     # deploy or upgrade
kubectl get flinkdeployment -n streaming  # status: STABLE / DEPLOYING / ...
kubectl describe flinkdeployment fraud-detection -n streaming   # events + errors
kubectl delete flinkdeployment fraud-detection -n streaming     # tear down

# Logs
kubectl logs -f deploy/fraud-detection -n streaming             # JobManager
kubectl logs -f -l component=taskmanager -n streaming           # TaskManagers

# Web UI without an ingress
kubectl port-forward svc/fraud-detection-rest 8081:8081 -n streaming
```

To **upgrade**: edit the `image:` tag in the YAML and `kubectl apply`. The operator does stop-with-savepoint → new pods → restore. That is the entire procedure, which is why the Operator won.

There is also a `FlinkSessionJob` CRD for submitting individual jobs into a `FlinkDeployment` that has no `job:` block (a session cluster) — useful when you want many small jobs to share resources.

---

## YARN, briefly

Still common in Hadoop shops. Same modes, different flags.

```bash
# Application mode (what you want)
./bin/flink run-application -t yarn-application \
    -Djobmanager.memory.process.size=2048m \
    -Dtaskmanager.memory.process.size=8192m \
    -Dtaskmanager.numberOfTaskSlots=4 \
    -Dyarn.application.name="fraud-detection" \
    -Dyarn.application.queue=streaming \
    ./fraud-detection-1.0.0.jar --env prod

# Session cluster: start it, then submit into it
./bin/yarn-session.sh -d -nm flink-session      # -d = detached
./bin/flink run -t yarn-session \
    -Dyarn.application.id=application_1699_0042 \
    ./job.jar
```

YARN handles container allocation and restart-on-failure, so it plays the role Kubernetes plays above. The `-t yarn-per-job` target is the removed Per-Job mode.

---

## The CLI: `flink run`, `list`, `cancel`, `stop`

```bash
# ---------- SUBMIT (session mode) ----------
./bin/flink run \
    -m localhost:8081 \                     # JobManager address (or -t <target>)
    -c com.akash.flink.jobs.FraudDetectionJob \   # entry class; omit if Main-Class in manifest
    -p 8 \                                  # default parallelism
    -d \                                    # detached: return immediately, don't tail
    -s s3://bucket/savepoints/savepoint-abc123 \  # restore from this savepoint
    -n \                                    # --allowNonRestoredState. DANGEROUS. See below.
    ./target/fraud-detection-1.0.0.jar \
    --env prod --threshold 500              # everything after the jar goes to args[]

# ---------- SUBMIT (application mode) ----------
./bin/flink run-application -t kubernetes-application \
    -Dkubernetes.cluster-id=fraud-detection \
    -Dkubernetes.container.image.ref=my-registry/flink-fraud:1.0.0 \
    -Dexecution.savepoint.path=s3://bucket/savepoints/savepoint-abc123 \
    local:///opt/flink/usrlib/fraud-detection-1.0.0.jar --env prod
```

Flags worth memorising:

| Flag | Meaning |
|---|---|
| `-c` / `--class` | entry class. Needed when the manifest has no `Main-Class`, or the jar has several jobs. |
| `-p` / `--parallelism` | job default parallelism. **An explicit `env.setParallelism()` in code overrides it.** |
| `-d` / `--detached` | submit and exit. Always use for streaming jobs. |
| `-s` / `--fromSavepoint` | restore from this savepoint (or retained checkpoint) path. |
| `-n` / `--allowNonRestoredState` | permit state in the savepoint that maps to no operator. |
| `-m` / `--jobmanager` | `host:port` of the JobManager REST endpoint. |
| `-t` / `--target` | deployment target: `remote`, `local`, `kubernetes-application`, `yarn-application`, `yarn-session`. |
| `-D<key>=<value>` | any `config.yaml` key, for this submission only. |

> **`--allowNonRestoredState` is the most dangerous flag on this page.** It tells Flink "silently drop savepoint state you cannot map to an operator". Correct when you deliberately deleted an operator. Catastrophic when the real cause is that a `uid()` changed and your job restarts with empty state and no error. The details are in [`../../03-state-and-skew.md`].

```bash
# ---------- INSPECT ----------
./bin/flink list                     # running + scheduled
./bin/flink list -a                  # include finished/cancelled
./bin/flink list -r                  # running only
# Output:  30.08.2025 10:14:22 : 4a3f...c91 : fraud-detection (RUNNING)
#                                  └ the jobId you need for everything below

# ---------- STOP ----------
# cancel: immediate, ungraceful. In-flight records are dropped.
# Only creates a savepoint if RETAIN_ON_CANCELLATION kept the last checkpoint.
./bin/flink cancel <jobId>

# stop: GRACEFUL. This is what you want for upgrades.
#   1. injects a MAX_WATERMARK so windows fire and timers complete
#   2. takes a savepoint
#   3. shuts down cleanly, committing any 2PC sink transactions
./bin/flink stop --savepointPath s3://bucket/savepoints <jobId>

# Same, but do NOT drain (skip the MAX_WATERMARK step). Faster; leaves
# open windows unfired, which is usually what you want for a pure upgrade.
./bin/flink stop --no-drain --savepointPath s3://bucket/savepoints <jobId>

# ---------- SAVEPOINT WITHOUT STOPPING ----------
./bin/flink savepoint <jobId> s3://bucket/savepoints
./bin/flink savepoint -d s3://bucket/savepoints/savepoint-abc123   # dispose one
```

`cancel` vs `stop` in one line: **`cancel` is `kill -9`, `stop` is a clean shutdown with a snapshot.** Use `stop` unless the job is already broken.

---

## Job upgrade procedure — the checklist

This is the thing you will do most often and the thing most likely to lose data if you improvise.

**Before you touch anything**

- [ ] 1. Every stateful operator in **both** the old and new job has an explicit **`uid()`**. Verify by reading the code, not by hoping. Without stable uids, adding one `filter()` reorders every auto-generated uid and orphans all your state.
- [ ] 2. The new jar was built with `mvn clean package` and you verified the connectors are inside it (`jar tf`, ch. 54).
- [ ] 3. `state.savepoints.dir` is configured and the path is writable from the cluster.
- [ ] 4. You have **tested the restore in staging** with a savepoint of comparable size. Restore time scales with state size and this is where you discover it takes 40 minutes.
- [ ] 5. You know the rollback plan: the old jar version and the savepoint path, written down.

**The upgrade**

- [ ] 6. **Note the current savepoint/checkpoint path and the Kafka offsets.** You need these to roll back.
- [ ] 7. **Stop with a savepoint:**
      ```bash
      ./bin/flink stop --no-drain \
          --savepointPath s3://bucket/savepoints/fraud-detection <jobId>
      ```
      It prints the created path. **Copy it.** If it fails, the job is still running — investigate rather than escalating to `cancel`.
      *(With the Kubernetes Operator, steps 7–9 are `kubectl apply` with `upgradeMode: savepoint`.)*
- [ ] 8. **Deploy the new jar** — push the new image, or copy the jar to the cluster.
- [ ] 9. **Start from the savepoint:**
      ```bash
      ./bin/flink run -d \
          -s s3://bucket/savepoints/fraud-detection/savepoint-4a3f00-1c9e2f \
          -p 8 ./fraud-detection-1.1.0.jar --env prod
      ```
      **Do not add `--allowNonRestoredState`** unless you deliberately removed an operator and have said out loud which one.

**Verify — do not walk away after step 9**

- [ ] 10. Job reaches `RUNNING`, and **`numRestarts` stays at 0**. A crash loop appears here.
- [ ] 11. **First checkpoint after restore completes.** Until it does you have no new recovery point.
- [ ] 12. **`records-lag-max` is falling**, not flat and not rising. It spikes during the restart; it must come back down.
- [ ] 13. **State size is comparable to before.** `lastCheckpointFullSize` near zero after a restore means the state did not restore — a mismatched uid. Stop and roll back.
- [ ] 14. Output is sane: alert counts, downstream row counts, spot-check a few records.
- [ ] 15. Watermarks are advancing (Watermarks tab, ch. 58).

**Rollback**, if 10–15 fail: `flink cancel` the new job, and `flink run -s <the savepoint from step 7>` with the **old** jar. This works because savepoints are forward-compatible with the job that produced them — which is exactly why step 6 matters.

---

## Remember

- **Session** = long-lived shared cluster, `main()` on the client, no isolation. **Application** = one cluster per job, `main()` on the JobManager, full isolation. **Per-Job** is deprecated/removed.
- **Application mode for production; session mode for dev and the SQL client.**
- In Application mode on Kubernetes the jar must be **baked into the image** and referenced as `local://` — there is no client to upload it.
- **Flink Kubernetes Operator** (`FlinkDeployment` CRD) is the modern standard: declarative, git-ops friendly, and it automates stop-with-savepoint upgrades.
- **`upgradeMode: savepoint`** preserves state cleanly and needs `state.savepoints.dir`. `last-state` restores from the last checkpoint (needs HA). `stateless` throws state away.
- `high-availability.type: kubernetes` is what lets a JobManager pod restart without losing the job.
- **`stop` drains and snapshots; `cancel` kills.** Use `stop --savepointPath` for upgrades, `--no-drain` when you do not want windows to fire early.
- **`--allowNonRestoredState` silently discards unmapped state.** Never a default.
- Upgrade = uid() check → stop-with-savepoint → deploy → restore → **verify restarts, first checkpoint, lag falling, and state size**.

**Interview one-liners**

- *"Session vs Application mode?"* → Session runs many jobs on one shared long-lived cluster with `main()` on the client and no isolation; Application gives each job its own cluster and runs `main()` on the JobManager, so a failure's blast radius is one job.
- *"Why was Per-Job mode removed?"* → It gave per-job clusters but still ran `main()` on the client, so it kept the client-side bottleneck without any benefit Application mode does not also provide.
- *"How do you upgrade a stateful Flink job with no data loss?"* → `flink stop --savepointPath` to drain and snapshot, deploy the new jar, restart with `-s <savepoint>`, then verify the first post-restore checkpoint and that state size matches. Every stateful operator must have a stable `uid()`.
- *"`cancel` vs `stop`?"* → `cancel` terminates immediately and drops in-flight data; `stop` takes a savepoint, lets two-phase-commit sinks commit, and shuts down cleanly.
- *"What does the Flink Kubernetes Operator give you over native Kubernetes mode?"* → Declarative `FlinkDeployment` resources, automatic savepoint-on-upgrade via `upgradeMode`, reconciliation and self-healing — instead of imperative CLI calls.
- *"When is `--allowNonRestoredState` correct?"* → Only when you intentionally removed a stateful operator. Otherwise it masks a changed `uid()` and restarts the job with empty state and no error.

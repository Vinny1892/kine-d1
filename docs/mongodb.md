# MongoDB backend for kine

Runs a Kubernetes cluster (k3s) with **MongoDB** as the datastore instead of
etcd.

> **Status:** v0.2 — validated against a real k3s cluster (node `Ready`,
> deployment, scale, rolling update, `kubectl exec`/`logs`), with parity,
> conformance, load and chaos coverage. Not for critical production; read
> [When not to use it](#when-not-to-use-it).

**New here?** Start with the [k3s tutorial](k3s-tutorial.md). This page is the
reference; [implementation notes](implementation-notes.md) covers behaviour
that is not obvious from the code.

---

## How it works

kine translates the etcd v3 API that `kube-apiserver` speaks into the
underlying datastore. The SQL drivers (SQLite, PostgreSQL, MySQL) share the
`server.Dialect` interface, which returns `*sql.Rows`. MongoDB does not speak
SQL, so this driver implements `server.Backend` directly — the same path the
`nats` and `t4` drivers take.

Two decisions shape everything else, and both have an ADR:

| Decision | Why | ADR |
|---|---|---|
| The etcd revision **is** MongoDB's `clusterTime`, not a counter | A counter hits `WriteConflict` under concurrency (8.2 writes/s) and the change stream delivers out of its order | [ADR-0001](adr/0001-revision-from-clustertime.md) |
| The watch listens to `insert` only, deriving the revision from the event's `clusterTime` | Waiting for the update that writes `rev` made events disappear and stalled the apiserver | [ADR-0002](adr/0002-watch-listens-to-inserts.md) |

The watch uses **change streams**, not polling. SQL drivers run one query per
second forever; here there is **a single stream per process**, shared by every
watcher through `pkg/broadcaster`.

---

## Requirements

- **MongoDB as a replica set.** Change streams and transactions require it.
  Atlas M0 (free) qualifies — it is a 3-node replica set.
- A user with `readWrite` on the chosen database.

A standalone `mongod` will not work.

---

## Usage

```bash
kine --endpoint "mongodb+srv://user:password@cluster.example.mongodb.net/?retryWrites=true&w=majority"
```

And in k3s, pointing at a running kine:

```bash
k3s server --datastore-endpoint="http://127.0.0.1:2379"
```

### DSN parameters

The connection string is MongoDB's own. Extra parameters use the `kine_`
prefix and are stripped before the URI reaches the official driver, which
rejects unknown keys.

| Parameter | Default | Purpose |
|---|---|---|
| `kine_database` | `kine` | Database holding the collections. Also accepted in the URI path |
| `kine_collection` | `kine` | Revision log collection |
| `kine_epoch_base` | `1767225600` | Instant revisions are counted from — **see below** |
| `kine_connect_timeout` | `30s` | Initial connection timeout |
| `kine_server_selection_timeout` | `30s` | How long to wait for an eligible server |

**About `kine_epoch_base`:** the revision is
`(clusterTime.T - epochBase) << 20 | clusterTime.I`. Without the subtraction
the shift overflows `int64` around 2038. The value is written to metadata on
first run and **cannot change afterwards** — changing it would rewrite the
meaning of every revision already handed to the apiserver. The driver refuses
to start if the DSN asks for a value different from the stored one.

`writeConcern` and `readConcern` are pinned to `majority`: kine must read the
revision it just wrote. Measured — 26.6 ms versus 27.2 ms for a default write,
an indistinguishable difference.

---

## Collections

| Collection | Contents |
|---|---|
| `kine` | the revision log (one entry per mutation) |
| `kine_meta` | cluster epoch base and compacted revision |

Indexes on `kine`: `(name, rev)`, `(rev)`, `(name, prev_revision)` **unique**,
`(prev_revision)`. The unique index is what produces etcd's `ErrKeyExists` when
two clients create the same key.

---

## What was measured

Against an Atlas M0 in São Paulo, and a real k3s v1.36.4 on EC2:

| | |
|---|---|
| Write latency (`insert`, w=majority) | **26.6 ms** p50 |
| Read latency (`find` by index) | **21.9 ms** p50 |
| Node `Ready` | **~4 s** |
| apiserver `readyz` | **~30 s** |
| Operations during a full bootstrap | 4,866 WATCH · 82 LIST · 4 DELETE · **0 errors** |
| A whole k3s cluster | **~1 MB** (842 documents) |
| Projected onto M0's 512 MB | **~429,000 documents** |
| Oplog window on M0 | **~4.4 h** |
| Real write ceiling (M0) | **~92 ops/s** — beyond that, latency explodes |
| Practical cluster mutation ceiling | **~30/s** (each mutation costs ~2 ops) |
| Latency the apiserver sees, one write at a time | **~87 ms** p50 |
| Latency the apiserver sees, 12 concurrent writes | **~884 ms** p50 |

Details and reproduction steps in [`spikes/results/`](../spikes/results/).

---

## Capacity and what to monitor

M0's ceiling is **~92 write operations/s**. Each kine mutation costs ~2
operations (the insert, and the update that writes the revision), so:

| | Cluster mutations/s |
|---|---|
| Absolute ceiling | ~45 |
| **Where latency is still healthy** | **~30** |
| Medium cluster, idle | 3.3 |
| Medium cluster, in operation | 10 |

Three to nine times headroom for a medium cluster — enough, not comfortable.

**Exceeding the ceiling does not fail; it gets slow.** Measured in
[MSPIKE-6](../spikes/results/mspike-6.md): zero errors at 6× the ceiling, but
write p99 went from 810 ms to **63 s**, and the change stream fell **44 s**
behind. For Kubernetes that is worse than an error: leader election has a 10 s
`RenewDeadline`, so scheduler and controller-manager lose leadership with
nothing in the logs to explain it.

So what to monitor is **not error rate** — there will not be any:

| Signal | Alert when |
|---|---|
| **Write latency p99** | above ~1 s, the cluster is heading toward losing leadership |
| **Change stream lag** | above a few seconds, informers are going stale |
| **Write concurrency** | this is what hurts, not volume: one write at a time costs ~87 ms; twelve at once cost ~884 ms |
| Storage used | M0 does not grow; when full, the cluster stops |

Metrics exposed by the driver, all prefixed `kine_mongo_`:

| Metric | |
|---|---|
| `ops_total{op,result}` | count per operation |
| `op_duration_seconds{op}` | histogram, buckets from 1 ms to ~16 s |
| **`change_stream_lag_seconds`** | age of the last event — **the one that catches the real failure mode** |
| `change_stream_reconnects_total{motivo}` | `queda`, `historico_perdido`, `falha_ao_abrir` |
| `storage_bytes{componente}` | `dados`, `storage`, `indices` |
| `current_revision`, `compacted_revision` | log position |

---

## When not to use it

Be honest about what this is.

**Do not use it if:**

- **It is critical production.** This driver is new, has no field usage, and
  while it passes an etcd conformance suite written against the patterns the
  apiserver emits, it has not run for weeks under unpredictable load.
- **The cluster has heavy churn** — CI creating thousands of Jobs, operators
  with aggressive reconcile loops. The ceiling is ~30 mutations/s before
  latency hurts leader election
  ([MSPIKE-6](../spikes/results/mspike-6.md)), and the symptom of exceeding it
  is the cluster stalling with no error message.
- **Latency matters.** Every apiserver operation pays a round trip to MongoDB.
  With the database far away that becomes tens or hundreds of milliseconds,
  and leader election deadlines are 5-10 s.
- **You need automatic backups on M0.** The free tier has none — only manual
  `mongodump`, with a runbook in [backup-restore.md](backup-restore.md).
- **You cannot use a replica set.** Standalone will not work.

**It makes sense for:** homelab, edge, dev/staging, small clusters where "not
administering a database" is worth more than milliseconds, and where Atlas is
already part of the stack.

### Known risks

| Risk | Status |
|---|---|
| Watch falls outside the oplog window (~4.4 h on M0) and gets invalidated | Handled: the driver detects `ChangeStreamHistoryLost` and backfills by reading the collection ([MW-3](../spikes/results/mt3.md)) |
| M0 does not grow storage — when full, the cluster stops | Large headroom (~429,000 documents), and `kine_mongo_storage_bytes` exposes it |
| Operation ceiling on M0 | **Measured** ([MSPIKE-6](../spikes/results/mspike-6.md)): no errors, just latency. p99 reaches **63 s** at 6× the ceiling — and leader election has a 10 s deadline |
| Change stream falls behind under load | **Measured**: 44 s lag at 6× the ceiling, without losing events. A lagging watch is a cluster blind to its own changes |
| Revisions are not dense — they jump | By design ([ADR-0001](adr/0001-revision-from-clustertime.md)). The apiserver treats `resourceVersion` as opaque; validated against real k3s |
| Orphan with `rev = 0` if the process dies between insert and update | Inert, does not corrupt state ([ADR-0002](adr/0002-watch-listens-to-inserts.md)) |

---

## Development

```bash
go build ./...
go test ./...                                    # unit tests, no credentials

export KINE_MONGO_TEST_URI="mongodb+srv://..."   # integration, real MongoDB
go test -tags=integration ./pkg/drivers/mongo/ ./test/mongo/
```

Integration tests create their own collection per run and drop it afterwards.
`test/mongo/` exercises the backend through the **real etcd v3 protocol** —
including `Txn` with `Compare(ModRevision)`, which is how the apiserver
implements optimistic concurrency.

One trap worth knowing if you write tests against kine: `Txn` is not generic.
kine recognizes a closed set of shapes by exact form
(`pkg/server/limited.go:29`) — a conditional delete, for instance, requires an
`Else` branch containing a `Range` (`pkg/server/delete.go:19`).

### Scripts

| Script | Purpose |
|---|---|
| `hack/ec2-mt3.sh criar` / `destruir` | Provisions an EC2 instance, runs the full k3s test, tears it down |
| `hack/k3s-mongo.sh` | Brings up kine + k3s locally (needs real Linux — see below) |
| `hack/k3s-diag.sh` | Starts only k3s against a running kine, leaving everything up for inspection |
| `hack/k3s-limpar.sh` | Tears down everything k3s, including orphaned instances |
| `hack/k3s-estado.sh` | Collects k3s state, including what requires root |
| `spikes/*.py` | The measurements, reproducible |

**Do not try this on WSL2.** `modprobe iptable_nat` hangs in `D` state
(uninterruptible) inside the WSL kernel, immune to `kill -9`. Since module
loading is serialized in the kernel, every subsequent `modprobe` queues behind
it and k3s waits forever — without logging an error. Use `hack/ec2-mt3.sh` or
a VM.

Backup and restore: [backup-restore.md](backup-restore.md).

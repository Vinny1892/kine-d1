# kine-mongo — MongoDB backend for kine

> **Status:** v0.2 shipped. All three milestones closed (37 issues).
> **Repository:** fork of `k3s-io/kine`
> **History:** this project evaluated Cloudflare D1 first and
> [discarded it on cost](docs/d1-rejected/README.md).

This file is the project's planning record. For using the backend, start with
the **[k3s tutorial](docs/k3s-tutorial.md)**; the
**[reference](docs/mongodb.md)** and
**[implementation notes](docs/implementation-notes.md)** cover the rest.

---

## Goal

Run a Kubernetes cluster (k3s) with MongoDB as the datastore instead of etcd.

**There was no precedent.** kine supports SQLite, PostgreSQL, MySQL, NATS and
t4 — no document store. That was both the opportunity and the risk.

## Why MongoDB, after D1

The D1 evaluation concluded that **everything worked except the price**: US$ 22
per sustained write/s, with no ceiling. The control plane alone consumed 87% of
the free allowance, and an operator with `RequeueAfter: 10s` over 500 objects
cost US$ 1,037/month.

Atlas changes the nature of the risk:

| | Cloudflare D1 | Atlas M0 |
|---|---|---|
| Billing model | **per operation** | **per instance** (free) |
| Excess load becomes | **an invoice** | **latency** |
| Spending ceiling | none | **US$ 0, by construction** |
| Latency (from Brazil) | 232 ms (no SA region) | ~10-30 ms (**São Paulo region**) |
| Storage | 10 GB | **512 MB** |

> **The core trade:** out goes a system that degrades into money, in comes one
> that degrades into latency. Measured later: at 6× the operation ceiling,
> latency degrades badly enough to break leader election. Visible failure beats
> a silent invoice — but it is not painless.

## The architectural problem: kine is SQL

kine's natural extension point is `server.Dialect`
(`pkg/server/types.go:43`), and it returns **`*sql.Rows`**.

With D1 that was a gift: a `database/sql` driver inherited ~90% of the code
through `pkg/drivers/generic`. **With MongoDB that saving disappears
entirely** — the driver has to implement `server.Backend` directly, the way
`nats` and `t4` do, reimplementing revision semantics, watch, compaction and
gap-fill.

What MongoDB gives back:

| Feature | Replaces | Gain |
|---|---|---|
| **Change streams** | the 1 s polling loop (`sqllog/sql.go:486`) | real-time watch, no polling |
| **TTL indexes** | `pkg/ttl` — *rejected, see below* | — |
| **ACID transactions** | D1's deferred CAS transaction | real interactive transactions |
| **`clusterTime`** | `AUTOINCREMENT` | revision sequence |

Two of those did not survive contact with reality. TTL indexes delete documents
without producing the etcd tombstone, and can resurrect deleted data —
[implementation notes](docs/implementation-notes.md). Transactions turned out
to be unusable on the write path: 6 concurrent ones produced 2 commits and 4
`WriteConflict`s.

---

## What the measurements changed

The whole point of the spike phase. Each of these overturned something that had
been asserted with confidence:

| Belief | Reality |
|---|---|
| D1 cost model: `rows_written ≈ 2 × mutations` | **≈ 9 ×** — every INSERT also writes the 6 indexes. Off by ~70× |
| Cloudflare docs: 2 MB per row | **2 MiB per value, 4 MiB per row** — two separate limits |
| A CAS via `UPDATE ... WHERE` would guard compaction | It returns **success** when it matches nothing. Would have deleted data on a false premise, silently |
| A revision counter is the idiomatic approach | 8.2 writes/s, and the change stream delivers **out of its order** |
| Storage would be the M0 bottleneck | A whole k3s cluster is **~1 MB**. Off by 10× |
| `LIST` is slow because of the query plan | It is **payload over the network**. A materialised collection made it *worse* |
| Exceeding the operation ceiling would surface as errors | **Zero errors** at 6× the ceiling — it queues instead, p99 reaching 63 s |

Two production bugs were caught the same way: `Version` never being populated,
and the watch waiting on the wrong event — which stalled a real apiserver at
`autoregister-completion`.

---

## Design decisions

Both have an ADR:

- **[ADR-0001](docs/adr/0001-revision-from-clustertime.md)** — the etcd revision
  is MongoDB's `clusterTime`, not a counter. A counter invents an ordering that
  competes with the database's; `clusterTime` *is* the database's ordering.
- **[ADR-0002](docs/adr/0002-watch-listens-to-inserts.md)** — the watch listens
  to `insert` only, deriving the revision from the event's `clusterTime`.
  Serializing writes to fix the ordering bug created a queue that broke leader
  election; this fixes it without serializing anything.

---

## Where it stands

| Milestone | |
|---|---|
| v0.1 — runs on k3s | 12/12 ✅ |
| v0.2 — hardening | 12/12 ✅ |
| v1.0 — production | 13/13 ✅ |

39 integration tests, including the real etcd v3 protocol, two kine instances
over one MongoDB, repeated compaction and real oplog invalidation. A full k3s
cluster validated on EC2: node `Ready`, deployment, scale, rolling update,
`kubectl exec` and `logs`.

## What remains genuinely open

Not tracked as issues, because they are not tasks — they are limits of what has
been established:

- **No field usage.** 39 tests and one real cluster is not weeks under
  unpredictable load.
- **The load test was 300 objects.** A cluster with hundreds of nodes and
  third-party operators is a different regime.
- **The oplog window (~4.4 h) is not observable at runtime** on M0 —
  `collStats` on the oplog is blocked. The driver handles invalidation when it
  happens rather than predicting it.

[When not to use it](docs/mongodb.md#when-not-to-use-it) is the honest version
of this list.

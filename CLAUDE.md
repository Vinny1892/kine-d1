# Project guide

This is a fork of [k3s-io/kine](https://github.com/k3s-io/kine) that adds a
**MongoDB backend**, so a k3s cluster can use MongoDB as its datastore instead
of etcd. Shipped as `v0.2.0-mongo`; all three milestones are closed.

The driver lives in `pkg/drivers/mongo/`. Integration tests that need the real
etcd protocol live in `test/mongo/`.

---

## Read this before changing the backend

Two decisions shape everything, and both have measured alternatives that were
tried and rejected. Changing either without reading the ADR will reintroduce a
bug that took a real cluster down:

- **[ADR-0001](docs/adr/0001-revision-from-clustertime.md)** — the etcd revision
  is MongoDB's `clusterTime`, not a counter. A counter delivers 8.2 writes/s
  under concurrency and the change stream arrives out of its order.
- **[ADR-0002](docs/adr/0002-watch-listens-to-inserts.md)** — the watch matches
  `insert` only, deriving the revision from the event's `clusterTime`. Waiting
  for the update that writes `rev` made events disappear and stalled the
  apiserver at `autoregister-completion`. Serializing writes to fix that
  created a queue that broke leader election.

**[docs/implementation-notes.md](docs/implementation-notes.md)** covers the
smaller non-obvious behaviour: why writes take two operations, why leases do not
use a TTL index, the direction of the delete in compaction, why `keysOnly` is
not cosmetic, and why metrics target latency rather than errors.

---

## Conventions

- **Rationale goes in `docs/`, never in code comments.** Code carries only
  one-line doc comments on exported symbols, as Go convention requires. If you
  find yourself explaining *why* in a comment, it belongs in
  `implementation-notes.md` or an ADR.
- **Everything is in English** — code, comments, messages, tests, docs. The
  only exception is `docs/d1-rejected/`, which is archived and not maintained.
- **Measure before asserting.** The spike method is why this project works:
  every number in the docs came from running against a real Atlas cluster, and
  it caught a cost model off by 70×, a silently wrong transaction guard, and
  Cloudflare's own documentation being wrong about size limits.
- Commit messages explain *why*, in prose. See the history.

---

## Running things

Credentials live in `~/.config/kine-mongo/env`, **outside the repository**.
Never print or commit `MONGO_URI`.

```bash
MONGO_URI='mongodb+srv://user:password@cluster.example.mongodb.net/?retryWrites=true&w=majority'
```

Single quotes matter: the URI contains `&`, which the shell reads as a
background operator when sourcing.

```bash
go build ./...
go test ./...                                    # unit, no credentials needed

set -a; source ~/.config/kine-mongo/env; set +a
export KINE_MONGO_TEST_URI="$MONGO_URI"
go test -tags=integration ./pkg/drivers/mongo/ ./test/mongo/

KINE_MONGO_CARGA=1 go test -tags=integration ./test/mongo/ -run 'TestLoad|TestChaos'
```

39 integration tests. They create their own collection per run and drop it
afterwards. The spikes need a venv:

```bash
~/.config/kine-mongo/venv/bin/python spikes/mspike-1-setup.py
```

`mongodump`/`mongorestore` are in `~/.local/bin` (needed by `TestBackupRestore`,
which skips without them).

---

## Traps that already cost time

**Do not try k3s on WSL2.** `modprobe iptable_nat` hangs in `D` state inside
the WSL kernel — uninterruptible, immune to `kill -9`. Module loading is
serialized in the kernel, so every subsequent `modprobe` queues behind it and
k3s waits on the child forever **without logging an error**. Three attempts
were lost to this before the cause was found. Use `hack/ec2-mt3.sh criar`,
which provisions an EC2 instance, runs the full test and tears it down.

**A leftover `k3s server` holds the lock and port 6443**, and `k3s-killall.sh`
does not catch instances started by hand. The failure looks identical to the
WSL one. Use `hack/k3s-limpar.sh`, which escalates `TERM → TERM → KILL` and
verifies nothing survived.

**kine's `Txn` is a closed set of shapes**, recognised by exact form
(`pkg/server/limited.go:29`). A conditional delete requires an `Else` branch
containing a `Range` (`pkg/server/delete.go:19`). Valid etcd requests that do
not match a known shape get `unsupported operations in txn request`.

**Never bulk-replace loose words across code.** Doing that here turned
`github.com` into `github.with` and broke Portuguese words mid-sentence. Use
complete phrases and word-delimited identifiers.

**`endpoint.Config` with `NotifyInterval` unset** makes kine panic on the first
watch, in `rand.Int63n(0)` (`pkg/server/watch.go:35`). The app always passes a
default, so it only bites code that builds the Config by hand — like tests.

---

## Operational shape of the backend

Numbers that constrain design choices, all measured (details in
[spikes/results/](spikes/results/)):

| | |
|---|---|
| Write latency (driver, isolated) | 26.6 ms p50 |
| Write latency (apiserver sees, 12 concurrent) | **884 ms** p50 |
| Real write ceiling on M0 | ~92 ops/s ≈ **30 cluster mutations/s** |
| Oplog window on M0 | ~4.4 h |
| A whole k3s cluster | ~1 MB, 842 documents |

**Exceeding the operation ceiling produces no errors** — Atlas queues instead.
Zero errors at 6× the ceiling, but write p99 reaches 63 s and the change stream
falls 44 s behind. Since leader election has a 10 s `RenewDeadline`, the cluster
loses its leader with nothing in the logs. That is why the metrics
(`kine_mongo_*`) target **write latency p99 and change stream lag**, not error
rate.

What hurts is **concurrency**, not volume.

---

## Scripts

| | |
|---|---|
| `hack/ec2-mt3.sh criar` / `destruir` | provision EC2, run the full k3s test, tear down |
| `hack/k3s-mongo.sh` | kine + k3s locally (real Linux only) |
| `hack/k3s-diag.sh` | k3s only, against a running kine, leaves everything up |
| `hack/k3s-limpar.sh` | tear down all k3s, including orphans |
| `hack/k3s-estado.sh` | collect k3s state, including what needs root |

---

## Test artifacts outside the repo

What the test setup needs, and what it may have left behind:

- `~/.config/kine-mongo/env` — credentials, `chmod 600`
- `~/.config/kine-mongo/venv/` — Python venv with `pymongo`, for the spikes
- `~/.local/bin/mongodump`, `mongorestore` — needed by `TestBackupRestore`,
  which skips without them

Containers from earlier test runs may still be up:

```bash
docker ps -a --filter name=kine-
```

`kine-test` + `kine-k3s-test` on the `kine-k3s-test-net` network are a SQLite
smoke test, useful for separating driver problems from k3s→kine problems.

AWS: `hack/ec2-mt3.sh destruir` removes instance, security group and key pair.
Verify with `aws ec2 describe-instances --filters
"Name=tag:Projeto,Values=kine-mongo"` before assuming it is clean.

---

## Where things stand

All 37 issues closed across three milestones. What is genuinely open is not
tracked as tasks, because they are limits of what has been established:

- **No field usage.** 39 tests and one real cluster is not weeks under
  unpredictable load.
- **The load test was 300 objects.** Hundreds of nodes with third-party
  operators is a different regime.
- **The oplog window is not observable at runtime** on M0 (`collStats` on the
  oplog is blocked), so the driver handles invalidation rather than predicting
  it.

[When not to use it](docs/mongodb.md#when-not-to-use-it) is the honest version
of that list.

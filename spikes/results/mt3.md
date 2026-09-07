# MT-3 — Real k3s on the MongoDB backend

**Status:** ✅ done · **Date:** 2026-09-07 · **Environment:** EC2 t3.small, Ubuntu 24.04, kernel 7.0.0-1012-aws

## The ordering bug, and why the first fix was expensive

Each record is written in two operations: `InsertOne`, which supplies the
`clusterTime` used as the revision, and `UpdateByID`, which stores it in the
`rev` field.

The original `watchLoop` discarded the insert event (`if r.Rev == 0 { continue }`)
and waited for the **update** event. Under concurrency the inserts go out in
order A/B but the updates may go out B/A — the watch advanced its cutoff with
the larger revision and discarded the smaller one as already delivered. Events
vanished, and the apiserver stalled at `autoregister-completion`.

The first fix serialized the pipeline with two mutexes (`insertMu`,
`revisionMu`). It preserved ordering but created a queue: under k3s bootstrap
load the wait exceeded leader election's 5-10 s deadlines, and scheduler and
controller-manager lost their leases.

## The fix adopted: listen to inserts only

The watch does not need the `rev` field. The insert event already carries the
revision — it is its `clusterTime`, the same value the write stores — and the
oplog delivers events in that order by construction ([MSPIKE-4](mspike-4.md):
zero out-of-order events in 40).

```go
// before: {"operationType": {"$in": ["insert","update","replace"]}} + fullDocument.rev
// after:  {"operationType": "insert"}          + EncodeRevision(ev.clusterTime)
```

This works because kine is an append-only log: every mutation — create, update,
delete — inserts a new document. The only update that exists is the one writing
`rev`, and it is no mutation at all.

Both mutexes went away. `UpdateByID` leaves the ordering critical path and now
serves only historical queries, which read after the fact.

**Side benefit:** ordering now comes from the oplog, which is global to the
MongoDB cluster. The limitation recorded earlier — "the mutexes coordinate only
within one kine instance" — ceases to exist.

## Why it had to leave WSL

Three attempts on WSL2 hung at the same point, and the backend was not the
cause:

```
81339  D  19:03  modprobe -- iptable_nat     <- stuck in the kernel
88567  S   6:21  modprobe -- iptable_nat
89687  S   2:16  modprobe -- iptable_nat
```

The first `modprobe` sat in state **`D`** — uninterruptible sleep, stuck inside
the WSL kernel loading `iptable_nat`. A process in `D` cannot be killed even
with `kill -9`, and module loading is serialized in the kernel, so every
subsequent `modprobe` queued behind it while k3s waited on the child in
`do_wait`. containerd never started, and the log stopped at "Module
br_netfilter was already loaded" with no error.

Throughout all of that, kine received **two** calls (`LIST /bootstrap`),
answered `count=0` correctly in milliseconds, and sat idle. MongoDB was never
exercised.

On EC2 the same module loads instantly.

## Result

kine in a container (`kine-mongo:fix-order`) against Atlas M0, k3s
`rancher/k3s:latest` on the same Docker network — and then a native k3s
v1.36.4 on EC2.

| Acceptance criterion | Result |
|---|---|
| Node `Ready` | ✅ **in ~4 s** |
| `coredns` and `local-path-provisioner` | ✅ Running |
| Deployment with real pods (nginx) | ✅ 4/4 Running |
| Scale 2 → 4 | ✅ |
| Rolling update (alpine → 1.27-alpine) | ✅ |
| `kubectl exec` | ✅ `nginx/1.27.5` |
| `kubectl logs` | ✅ |
| Leader election stable | ✅ holders unchanged |

Leases acquired: `apiserver`, `k3s`, `k3s-cloud-controller-manager`,
`kube-controller-manager`, `kube-scheduler`.

## The backend under a real cluster

```
operations served:  4,866 WATCH · 82 LIST · 4 DELETE
kine errors:        0
machine memory:     847 MB of 1,906 MB (kine + k3s + containerd + pods)
```

**Zero errors.** The overwhelmingly watch-heavy profile confirms the design:
with change streams, the cost of watching is the single shared stream, not one
query per second per watcher.

## Size of a k3s cluster in MongoDB

```
842 documents · 417 distinct keys
dataSize 1,729,056 bytes · storage + indexes 1,052,672 bytes
```

| Prefix | Keys |
|---|---|
| `/registry/events` | 108 |
| `/registry/clusterroles` | 74 |
| `/registry/clusterrolebindings` | 57 |
| `/registry/serviceaccounts` | 43 |
| `/registry/apiregistration.k8s.io` | 23 |

**A whole k3s cluster is ~1 MB.** Projected onto M0's 512 MB: **~429,000
documents**, roughly 500 clusters that size. The storage ceiling, which looked
like the most likely bottleneck, is far more generous than the MSPIKE-7
estimate suggested — that one used 6 KB objects, and real k3s objects are much
smaller.

## Reproduce

`hack/ec2-mt3.sh` provisions, runs and tears down. See the script for the
region used.

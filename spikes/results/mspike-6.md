# MSPIKE-6 — What happens past M0's 100 ops/s ceiling

**Status:** ✅ done · **Date:** 2026-09-07

This was the project's last big unmeasured claim. The pivot from Cloudflare D1
to MongoDB rested on one premise: **excess load must become latency, not an
invoice.** On D1 an operator in a hot loop generated US$ 887/month with no
brake at all.

## The premise holds — zero errors

Up to **6× the documented ceiling**, no operation failed:

| Target rate | Actual ops/s | Errors | p50 | p99 |
|---|---|---|---|---|
| 50/s (write) | 47 | **0** | 34 ms | 810 ms |
| 100/s (at the ceiling) | 60 | **0** | 39 ms | 2,928 ms |
| 250/s (2.5×) | 70 | **0** | 995 ms | 22,388 ms |
| 600/s (6×) | 92 | **0** | 1,032 ms | **63,836 ms** |
| 100/s (read) | 100 | **0** | 26 ms | 110 ms |
| 600/s (read) | 160 | **0** | 1,009 ms | 20,048 ms |

No error codes, no throttling exceptions. Atlas **queues** rather than
rejecting. And recovery is clean: five seconds after the 6× spike, p50 was back
to 30 ms.

**The real write ceiling is ~92 ops/s** — consistent with the documented 100.
Reads scale better, reaching 160 ops/s.

## But queueing has a cost no error would have revealed

Two things the absence of errors hides:

### 1. Latency explodes well before the ceiling

p99 goes from 810 ms (half the ceiling) to **63 seconds** (6× the ceiling).
**At the documented ceiling itself** p99 is already 2.9 s.

For Kubernetes that is fatal in a way an error would not be: leader election
has a 10 s `RenewDeadline`. A write taking 63 s means **scheduler and
controller-manager losing leadership** — exactly the failure mode that took
down the first MT-3 attempts. The cluster receives no error; it simply stops
working.

### 2. The change stream falls behind

```
events received: 10,000 (all of them)
stream errors: none
last event received 43.9 s ago
```

The stream neither broke nor lost an event — but it fell **44 seconds behind**.
To the apiserver, a watch 44 s behind is a cluster that cannot see its own
changes for 44 s: reconciliation stops, informers go stale, and leader election
has already lost the race.

With the 4.4 h oplog window ([MSPIKE-8](mspike-8.md)), 44 s is nowhere near
invalidation. But it shows the mechanism: under sustained load above the
ceiling, the lag grows, and a long enough lag lands in the invalidation
scenario.

## Against D1, which is what motivated all this

| | Cloudflare D1 | Atlas M0 |
|---|---|---|
| Excess load becomes | **an invoice** (US$ 22/month per write/s) | **latency** |
| Ceiling | none — the bill grows | ~92 write ops/s |
| How you find out | on the invoice, at month end | in the cluster, immediately |
| Recovery | irrelevant (the money is gone) | immediate once load stops |

The premise is right: **a system that degrades visibly beats one that charges
silently.** But "better" is not "painless" — D1 broke the budget, M0 breaks the
cluster.

## Translated into cluster capacity

Each kine mutation costs ~2 MongoDB operations (the insert, and the update that
writes `rev`). So:

| | Cluster writes/s |
|---|---|
| Absolute ceiling (~92 ops/s) | ~45 |
| Where latency is still healthy (~60 ops/s) | **~30** |
| Medium cluster, idle | 3.3 |
| Medium cluster, in operation | 10 |

**Three to nine times headroom** for a medium cluster. Enough, but not
comfortable: an operator with an aggressive reconcile loop eats that headroom
fast — and the symptom will be leader election flapping, not an error message.

## Operational consequence

This promotes `MOPS-1` (metrics) from nice-to-have to **necessary**, but with a
corrected target: what matters is **not errors** — there will not be any. It is
**write latency p99** and **change stream lag**. If p99 crosses ~1 s, the
cluster is on its way to losing leadership.

## Reproduce

```bash
spikes/mspike-6-throttle.py [multiplier]
```

# MSPIKE-1 — M0 cluster: identity, limits and baseline

**Status:** ✅ done · **Date:** 2026-09-06

| | |
|---|---|
| MongoDB | **8.0.32** |
| Topology | replica set `atlas-5uuxgo-shard-0`, **3 nodes** |
| `maxBsonObjectSize` | **16,777,216** (16 MB) |
| `maxWriteBatchSize` | 100,000 |
| Region (inferred) | **São Paulo or very close** — ping p50 21.4 ms |

## Latency — against D1's 232 ms

| Operation | p50 | p95 | p99 |
|---|---|---|---|
| `ping` | 21.6 ms | 28.5 ms | 55.9 ms |
| `insert_one` (6 KB) | 27.2 ms | 37.4 ms | 264.6 ms |
| `find_one` by index | 21.9 ms | 43.2 ms | 376.2 ms |
| **`insert` w=majority** | **26.6 ms** | 32.4 ms | 34.6 ms |
| **`find` readConcern=majority** | **21.6 ms** | 23.6 ms | 24.2 ms |

**8.7× faster than D1** on insert (26.6 ms against 232 ms). The São Paulo
region does what no amount of optimisation could do for D1, which has no South
American presence.

## Two inherited risks that died here

**Consistency — solved for free.** `w=majority` costs 26.6 ms against 27.2 ms
for a default write: **an indistinguishable difference**. kine can use majority
on the write path at no cost.

**Value size — no longer a thing.** A document can be 16 MB; D1 capped at
2 MiB per value and 4 MiB per row, which forced a hex encoding scheme and left
only 28% headroom in the worst case. Here a 1.5 MB k8s object with its
`old_value` uses under 20% of the limit.

## Reproduce

```bash
spikes/mspike-1-setup.py
```

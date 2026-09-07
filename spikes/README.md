# Spikes

Measurements against a **real MongoDB Atlas**, to validate assumptions before
writing production code.

That discipline is what caught the D1 cost model being off by 70×, a silently
wrong transaction guard, and the ordering bug that stalled the apiserver.

## Running

Credentials live in `~/.config/kine-mongo/env`, outside the repository:

```bash
MONGO_URI='mongodb+srv://user:password@cluster.example.mongodb.net/?retryWrites=true&w=majority'
```

The single quotes matter: the URI contains `&`, which the shell would read as a
background operator when the file is sourced.

```bash
python3 -m venv ~/.config/kine-mongo/venv
~/.config/kine-mongo/venv/bin/pip install pymongo
~/.config/kine-mongo/venv/bin/python spikes/mspike-1-setup.py
```

## Status

| Spike | Question | Result |
|---|---|---|
| MSPIKE-1 | M0 cluster: identity, limits, baseline | ✅ [done](results/mspike-1.md) |
| MSPIKE-2 | Do change streams work on M0? | ✅ [done](results/mspike-2-3.md) |
| MSPIKE-3 | Do multi-document transactions work on M0? | ✅ [done](results/mspike-2-3.md) |
| MSPIKE-4 | Revision generation: counter vs clusterTime | ✅ [done](results/mspike-4.md) |
| MSPIKE-5 | Latency of the real kine queries | ✅ [done](results/mspike-5-9.md) |
| MSPIKE-6 | What happens past the 100 ops/s ceiling? | ✅ [done](results/mspike-6.md) |
| MSPIKE-7 | On-disk size of a cluster | ✅ [done](results/mspike-5-9.md) |
| MSPIKE-8 | Oplog window and change stream invalidation | ✅ [done](results/mspike-8.md) |
| MSPIKE-9 | Document model and indexes | ✅ [done](results/mspike-5-9.md) |
| MT-1/2/4/5 | Parity, conformance, load, chaos | ✅ [done](results/mt-1-2-4-5.md) |
| MT-3 | Real k3s with a `Ready` node | ✅ [done](results/mt3.md) |

The discarded Cloudflare D1 work is archived in
[docs/d1-rejected/](../docs/d1-rejected/README.md).

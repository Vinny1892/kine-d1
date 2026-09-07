"""Shared client for the kine-mongo spikes. Never prints credentials."""
import os, sys, time

ENV = os.environ.get("KINE_MONGO_ENV", os.path.expanduser("~/.config/kine-mongo/env"))
DB  = os.environ.get("KINE_MONGO_DB", "kine_spike")

def load():
    cfg = {}
    for line in open(ENV):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            # the file uses single quotes because the URI contains "&", which
            # the shell would read as an operator when sourcing
            cfg[k.strip()] = v.strip().strip("'\"")
    if not cfg.get("MONGO_URI"):
        sys.exit(f"MONGO_URI is not set in {ENV}")
    return cfg

def client(**kw):
    from pymongo import MongoClient
    cfg = load()
    kw.setdefault("serverSelectionTimeoutMS", 15000)
    return MongoClient(cfg["MONGO_URI"], **kw)

def db(**kw):
    return client(**kw)[DB]

def timeit(fn, n=20, aquecer=True):
    """Runs fn() n times and returns sorted latencies in ms."""
    if aquecer:
        try: fn()
        except Exception: pass
    out = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        out.append((time.perf_counter() - t) * 1000)
    return sorted(out)

def pct(v, q):
    return v[min(int(len(v) * q), len(v) - 1)] if v else 0

def summary(rotulo, v):
    import statistics
    print(f"  {rotulo:<34} p50={statistics.median(v):>7.1f}ms  "
          f"p95={pct(v,.95):>7.1f}ms  p99={pct(v,.99):>7.1f}ms")

def section(t):
    print(f"\n{'='*74}\n{t}\n{'='*74}")

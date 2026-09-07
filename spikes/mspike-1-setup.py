#!/usr/bin/env python3
"""MSPIKE-1 — validates the M0 cluster and measures the baseline.

Usage:  spikes/mspike-1-setup.py
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import section, timeit, summary

def main():
    print("\nMSPIKE-1 — cluster M0: identidade, limites e linha de base")
    c = mongo.client()
    d = c[mongo.DB]

    section("1. Cluster identity")
    hi = c.admin.command("hello")
    bi = c.admin.command("buildInfo")
    print(f"  MongoDB ................. {bi['version']}")
    print(f"  replica set ............. {hi.get('setName')}  ({len(hi.get('hosts',[]))} nodes)")
    print(f"  primary ................. {hi.get('primary','?').split('.')[0]}")
    print(f"  maxBsonObjectSize ....... {hi['maxBsonObjectSize']:,} bytes  "
          f"(o D1 limitava a 2 MiB por valor)")
    print(f"  maxWriteBatchSize ....... {hi['maxWriteBatchSize']:,}")
    print(f"  logicalSessionTimeout ... {hi.get('logicalSessionTimeoutMinutes')} min")

    section("2. Network latency — the number D1 lost (232 ms)")
    summary("ping (admin)", timeit(lambda: c.admin.command("ping"), 25))
    col = d["lat"]
    col.drop()
    doc = {"name": "/registry/pods/default/nginx", "value": os.urandom(6*1024)}
    summary("insert_one (6 KB)", timeit(lambda: col.insert_one(dict(doc)), 25))
    col.create_index("name")
    summary("find_one by index", timeit(lambda: col.find_one({"name": doc["name"]}), 25))
    from pymongo import WriteConcern
    from pymongo.read_concern import ReadConcern
    colm = d.get_collection("lat", write_concern=WriteConcern(w="majority"),
                            read_concern=ReadConcern("majority"))
    summary("insert w=majority", timeit(lambda: colm.insert_one(dict(doc)), 25))
    summary("find readConcern=majority", timeit(lambda: colm.find_one({"name": doc["name"]}), 25))

    section("3. Where is the cluster? (inferred from latency)")
    import statistics
    p = statistics.median(timeit(lambda: c.admin.command("ping"), 15))
    region = ("São Paulo or very close" if p < 40 else
              "probably outside Brazil - North America" if p < 200 else "far")
    print(f"  ping p50 = {p:.1f}ms  ->  {region}")
    print(f"  for comparison, D1 measured 232ms (Brazil -> ENAM)")

    section("4. Storage — M0's real ceiling (512 MB)")
    st = d.command("dbStats")
    print(f"  dataSize={st['dataSize']:,}  storageSize={st['storageSize']:,}  "
          f"indexSize={st.get('indexSize',0):,}")
    print(f"  collections={st['collections']}  objects={st['objects']:,}")

    col.drop()
    print("\ncleaned up")

if __name__ == "__main__":
    main()

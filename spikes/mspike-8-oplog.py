#!/usr/bin/env python3
"""MSPIKE-8 — oplog window and real change stream invalidation.

MW-3 implemented the backfill, but the test exercised the mechanism, not the
trigger. Here we provoke real invalidation and measure the window.

Usage:  spikes/mspike-8-oplog.py
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import section
from pymongo.errors import OperationFailure
from bson.timestamp import Timestamp
from bson.binary import Binary

def main():
    c = mongo.client(); d = c[mongo.DB]
    print("\nMSPIKE-8 — oplog window and change stream recovery")

    section("1. Is the oplog window visible on M0?")
    for tentativa, fn in [
        ("db.getReplicationInfo (admin)", lambda: c.admin.command("replSetGetStatus")),
        ("local.oplog.rs (leitura)",      lambda: c["local"]["oplog.rs"].find_one()),
        ("collStats do oplog",            lambda: c["local"].command("collStats", "oplog.rs")),
    ]:
        try:
            r = fn()
            chaves = sorted(r.keys())[:6] if isinstance(r, dict) else type(r).__name__
            print(f"  ✓ {tentativa:<34} {chaves}")
        except Exception as e:
            print(f"  ✗ {tentativa:<34} {type(e).__name__}: {str(e)[:70]}")

    section("2. Change stream com timestamp anterior ao oplog — invalida?")
    col = d["oplog_test"]; col.drop(); d.create_collection("oplog_test")
    col.insert_one({"marco": 1})

    # A sufficiently old timestamp falls outside any conceivable oplog window.
    antigos = [
        ("1 hour ago",   int(time.time()) - 3600),
        ("1 day ago",    int(time.time()) - 86400),
        ("30 days ago",  int(time.time()) - 30*86400),
    ]
    codigos = {}
    for rotulo, seg in antigos:
        try:
            cs = col.watch(start_at_operation_time=Timestamp(seg, 1))
            # opening may not fail; the error surfaces on the first read
            cs.try_next()
            cs.close()
            print(f"  {rotulo:<16} aceito sem erro")
        except OperationFailure as e:
            cod = e.code
            codigos[rotulo] = cod
            nome = {286: "ChangeStreamHistoryLost", 280: "ChangeStreamFatalError"}.get(cod, "?")
            print(f"  {rotulo:<16} ✓ refused - code {cod} ({nome})")
        except Exception as e:
            print(f"  {rotulo:<16} {type(e).__name__}: {str(e)[:60]}")

    section("3. Would our historyLost() recognise these codes?")
    print("  Codes the driver handles (watch.go):")
    print("    286 ChangeStreamHistoryLost")
    print("    280 ChangeStreamFatalError")
    vistos = set(codigos.values())
    if vistos:
        cobertos = vistos & {286, 280}
        print(f"\n  Codes observed: {sorted(vistos)}")
        if cobertos == vistos:
            print("  ✓ all covered - the backfill fires")
        else:
            print(f"  ⚠ NÃO cobertos: {sorted(vistos - {286,280})} — precisam entrar no historicoPerdido()")
    else:
        print("\n  ⚠ no invalidation was provoked: the oplog reaches 30 days back,")
        print("    ou o M0 aceita qualquer startAtOperationTime. Ver item 4.")

    section("4. Estimando a janela por escrita — quanto oplog um cluster gasta?")
    # Write a known volume and measure how far the oplog advanced.
    col2 = d["oplog_vol"]; col2.drop()
    cs = col2.watch() if "oplog_vol" in d.list_collection_names() else None
    d.create_collection("oplog_vol")
    t0 = time.time()
    lote = [{"i": i, "v": Binary(os.urandom(4096))} for i in range(500)]
    col2.insert_many(lote)
    dt = time.time() - t0
    print(f"  500 documents de 4 KB (≈2 MB) em {dt:.1f}s")
    try:
        st = c["local"].command("collStats", "oplog.rs")
        print(f"  oplog: maxSize={st.get('maxSize',0):,} bytes  usado={st.get('size',0):,}")
        if st.get("maxSize"):
            print(f"  => ≈{st['maxSize']/(2*1024*1024):.0f} × esse volume antes de rodar a janela")
    except Exception as e:
        print(f"  (oplog collStats unavailable on M0: {type(e).__name__})")
        print("  => the window is not observable from here; the driver must handle invalidation")
        print("     when it happens, without predicting it - which is what MW-3 does.")

    col.drop(); col2.drop()
    print("\ncleaned up")

if __name__ == "__main__":
    main()

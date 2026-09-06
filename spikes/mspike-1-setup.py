#!/usr/bin/env python3
"""MSPIKE-1 — valida o cluster M0 e mede a linha de base.

Uso:  spikes/mspike-1-setup.py
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import titulo, cronometrar, resumo

def main():
    print("\nMSPIKE-1 — cluster M0: identidade, limites e linha de base")
    c = mongo.client()
    d = c[mongo.DB]

    titulo("1. Identidade do cluster")
    hi = c.admin.command("hello")
    bi = c.admin.command("buildInfo")
    print(f"  MongoDB ................. {bi['version']}")
    print(f"  replica set ............. {hi.get('setName')}  ({len(hi.get('hosts',[]))} nós)")
    print(f"  primary ................. {hi.get('primary','?').split('.')[0]}")
    print(f"  maxBsonObjectSize ....... {hi['maxBsonObjectSize']:,} bytes  "
          f"(o D1 limitava a 2 MiB por valor)")
    print(f"  maxWriteBatchSize ....... {hi['maxWriteBatchSize']:,}")
    print(f"  logicalSessionTimeout ... {hi.get('logicalSessionTimeoutMinutes')} min")

    titulo("2. Latência da rede — o número que o D1 perdeu (232 ms)")
    resumo("ping (admin)", cronometrar(lambda: c.admin.command("ping"), 25))
    col = d["lat"]
    col.drop()
    doc = {"name": "/registry/pods/default/nginx", "value": os.urandom(6*1024)}
    resumo("insert_one (6 KB)", cronometrar(lambda: col.insert_one(dict(doc)), 25))
    col.create_index("name")
    resumo("find_one por índice", cronometrar(lambda: col.find_one({"name": doc["name"]}), 25))
    from pymongo import WriteConcern
    from pymongo.read_concern import ReadConcern
    colm = d.get_collection("lat", write_concern=WriteConcern(w="majority"),
                            read_concern=ReadConcern("majority"))
    resumo("insert w=majority", cronometrar(lambda: colm.insert_one(dict(doc)), 25))
    resumo("find readConcern=majority", cronometrar(lambda: colm.find_one({"name": doc["name"]}), 25))

    titulo("3. Onde fica o cluster? (inferido pela latência)")
    import statistics
    p = statistics.median(cronometrar(lambda: c.admin.command("ping"), 15))
    regiao = ("São Paulo ou muito próximo" if p < 40 else
              "provavelmente fora do Brasil — norte-americano" if p < 200 else "longe")
    print(f"  ping p50 = {p:.1f}ms  ->  {regiao}")
    print(f"  para comparação, o D1 media 232ms (Brasil -> ENAM)")

    titulo("4. Storage — o teto real do M0 (512 MB)")
    st = d.command("dbStats")
    print(f"  dataSize={st['dataSize']:,}  storageSize={st['storageSize']:,}  "
          f"indexSize={st.get('indexSize',0):,}")
    print(f"  coleções={st['collections']}  objetos={st['objects']:,}")

    col.drop()
    print("\nlimpo")

if __name__ == "__main__":
    main()

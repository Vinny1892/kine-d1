#!/usr/bin/env python3
"""MSPIKE-8 — janela do oplog e invalidação real de Change Stream.

O MW-3 implementou a recuperação, mas o teste exercitava o mecanismo, não o
gatilho. Aqui provocamos a invalidação de verdade e medimos a janela.

Uso:  spikes/mspike-8-oplog.py
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mongo
from mongo import titulo
from pymongo.errors import OperationFailure
from bson.timestamp import Timestamp
from bson.binary import Binary

def main():
    c = mongo.client(); d = c[mongo.DB]
    print("\nMSPIKE-8 — janela do oplog e recuperação de Change Stream")

    titulo("1. A janela do oplog é visível no M0?")
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

    titulo("2. Change stream com timestamp anterior ao oplog — invalida?")
    col = d["oplog_test"]; col.drop(); d.create_collection("oplog_test")
    col.insert_one({"marco": 1})

    # Um timestamp bem antigo cai fora de qualquer janela de oplog concebível.
    antigos = [
        ("1 hora atrás",   int(time.time()) - 3600),
        ("1 dia atrás",    int(time.time()) - 86400),
        ("30 dias atrás",  int(time.time()) - 30*86400),
    ]
    codigos = {}
    for rotulo, seg in antigos:
        try:
            cs = col.watch(start_at_operation_time=Timestamp(seg, 1))
            # abrir pode não falhar; o erro vem na primeira leitura
            cs.try_next()
            cs.close()
            print(f"  {rotulo:<16} aceito sem erro")
        except OperationFailure as e:
            cod = e.code
            codigos[rotulo] = cod
            nome = {286: "ChangeStreamHistoryLost", 280: "ChangeStreamFatalError"}.get(cod, "?")
            print(f"  {rotulo:<16} ✓ recusado — código {cod} ({nome})")
        except Exception as e:
            print(f"  {rotulo:<16} {type(e).__name__}: {str(e)[:60]}")

    titulo("3. O nosso historicoPerdido() reconheceria esses códigos?")
    print("  Códigos que o driver trata (watch.go):")
    print("    286 ChangeStreamHistoryLost")
    print("    280 ChangeStreamFatalError")
    vistos = set(codigos.values())
    if vistos:
        cobertos = vistos & {286, 280}
        print(f"\n  Códigos observados: {sorted(vistos)}")
        if cobertos == vistos:
            print("  ✓ todos cobertos — a recuperação dispara")
        else:
            print(f"  ⚠ NÃO cobertos: {sorted(vistos - {286,280})} — precisam entrar no historicoPerdido()")
    else:
        print("\n  ⚠ nenhuma invalidação foi provocada: o oplog cobre até 30 dias atrás,")
        print("    ou o M0 aceita qualquer startAtOperationTime. Ver item 4.")

    titulo("4. Estimando a janela por escrita — quanto oplog um cluster gasta?")
    # Escreve um volume conhecido e mede quanto o oplog avançou no tempo.
    col2 = d["oplog_vol"]; col2.drop()
    cs = col2.watch() if "oplog_vol" in d.list_collection_names() else None
    d.create_collection("oplog_vol")
    t0 = time.time()
    lote = [{"i": i, "v": Binary(os.urandom(4096))} for i in range(500)]
    col2.insert_many(lote)
    dt = time.time() - t0
    print(f"  500 documentos de 4 KB (≈2 MB) em {dt:.1f}s")
    try:
        st = c["local"].command("collStats", "oplog.rs")
        print(f"  oplog: maxSize={st.get('maxSize',0):,} bytes  usado={st.get('size',0):,}")
        if st.get("maxSize"):
            print(f"  => ≈{st['maxSize']/(2*1024*1024):.0f} × esse volume antes de rodar a janela")
    except Exception as e:
        print(f"  (collStats do oplog indisponível no M0: {type(e).__name__})")
        print("  => a janela não é observável daqui; o driver precisa tratar a invalidação")
        print("     quando ela ocorrer, sem depender de prevê-la — que é o que o MW-3 faz.")

    col.drop(); col2.drop()
    print("\nlimpo")

if __name__ == "__main__":
    main()
